use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{OrderStatus, PositionSide},
    identifiers::{AccountId, InstrumentId},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{Money, Price, Quantity},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::execution::events::TbankFillCommission;

#[derive(Debug, Clone, Default)]
pub(super) struct TbankFillProjection {
    pub(super) orders: HashMap<String, TbankOrderFillProjection>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct TbankOrderFillProjection {
    cumulative_filled_quantity: Decimal,
    emitted_fill_quantity: Decimal,
    emitted_fill_notional: Decimal,
    emitted_commission: Option<Money>,
    unmatched_emitted_quantity: Decimal,
    seen_trade_ids: HashSet<String>,
    unmatched_synthetic_fills: VecDeque<TbankSyntheticFill>,
    synthetic_matches_by_trade_id: HashMap<String, Vec<TbankSyntheticFillMatch>>,
    commission_by_trade_id: HashMap<String, TbankTrackedFillCommission>,
    unknown_commission_quantity: Decimal,
}

/// Returns whether snapshot provenance can safely advance or repeat the status of a live fill.
/// Snapshot reads run independently from the live ledger, so an older `Unknown` value must not
/// overwrite a commission already resolved by a stream or reconciliation query.
pub(super) fn snapshot_fill_commission_is_newer(
    projection: &TbankFillProjection,
    order_id: &str,
    trade_id: &str,
    incoming: TbankFillCommission,
) -> bool {
    let Some(current) = projection
        .orders
        .get(order_id)
        .and_then(|order| order.commission_by_trade_id.get(trade_id))
    else {
        return true;
    };

    incoming == current.commission
        || commission_status_precedence(incoming) > commission_status_precedence(current.commission)
}

/// Accepts snapshot provenance without changing execution identity or fill quantities.
///
/// Snapshot reports can refine a commission already recorded by a live fill. Keep that refinement
/// in the shared ledger so later cumulative order-state reports cannot replace it with a weaker
/// attribution. Only a matching existing fill quantity is updated. A snapshot fill absent from the
/// ledger remains publishable, but must not create deduplication or execution-count state here.
pub(super) fn apply_snapshot_fill_provenance(
    projection: &mut TbankFillProjection,
    order_id: &str,
    trade_id: &str,
    fill_quantity: Decimal,
    incoming: TbankFillCommission,
) -> bool {
    let Some(current) = projection
        .orders
        .get(order_id)
        .and_then(|order| order.commission_by_trade_id.get(trade_id))
        .copied()
    else {
        return true;
    };
    if current.quantity != fill_quantity
        || !snapshot_fill_commission_is_newer(projection, order_id, trade_id, incoming)
    {
        return false;
    }

    let order = projection
        .orders
        .get_mut(order_id)
        .expect("tracked T-Bank fill order remains in projection");
    let tracked = order
        .commission_by_trade_id
        .get_mut(trade_id)
        .expect("tracked T-Bank fill remains in projection");
    if let Some(amount) = incoming.amount() {
        tracked.commission = commission_with_status(incoming, amount);
        tracked.resolved_quantity = tracked.quantity;
        recompute_commission_tracking(order);
    }

    true
}

#[derive(Debug, Clone, Copy)]
struct TbankTrackedFillCommission {
    quantity: Decimal,
    /// Quantity covered by `commission` when it is known; always zero for `Unknown`.
    /// A partial operations match resolves only the matched share, so the remainder keeps
    /// unknown provenance instead of letting the partial fee stand in for the whole fill.
    resolved_quantity: Decimal,
    commission: TbankFillCommission,
}

#[derive(Debug, Clone)]
struct TbankSyntheticFill {
    trade_id: String,
    quantity: Decimal,
}

#[derive(Debug, Clone)]
struct TbankSyntheticFillMatch {
    trade_id: String,
    quantity: Decimal,
}

pub(super) fn merge_fill_projection_alias(
    projection: &mut TbankFillProjection,
    alias_order_id: &str,
    canonical_order_id: &str,
) {
    if alias_order_id.is_empty() || alias_order_id == canonical_order_id {
        return;
    }
    let Some(alias) = projection.orders.get(alias_order_id).cloned() else {
        return;
    };
    // Build the merged row separately because resizing a partially retained synthetic fee can
    // fail on invalid money arithmetic. Do not leave the shared projection half-merged.
    let mut canonical = projection
        .orders
        .get(canonical_order_id)
        .cloned()
        .unwrap_or_default();
    canonical.cumulative_filled_quantity = canonical
        .cumulative_filled_quantity
        .max(alias.cumulative_filled_quantity);
    canonical.emitted_fill_quantity = canonical
        .emitted_fill_quantity
        .max(alias.emitted_fill_quantity);
    canonical.emitted_fill_notional = canonical
        .emitted_fill_notional
        .max(alias.emitted_fill_notional);
    let target_unmatched_quantity = canonical
        .unmatched_emitted_quantity
        .max(alias.unmatched_emitted_quantity)
        .max(Decimal::ZERO);
    let alias_unmatched_fills = alias.unmatched_synthetic_fills.clone();
    let alias_commission_by_trade_id = alias.commission_by_trade_id.clone();
    for (trade_id, alias_commission) in alias.commission_by_trade_id {
        match canonical.commission_by_trade_id.get_mut(trade_id.as_str()) {
            Some(canonical_commission)
                if tracked_commission_precedence(alias_commission)
                    > tracked_commission_precedence(*canonical_commission) =>
            {
                *canonical_commission = alias_commission;
            }
            Some(_) => {}
            None => {
                canonical
                    .commission_by_trade_id
                    .insert(trade_id, alias_commission);
            }
        }
    }
    canonical.seen_trade_ids.extend(alias.seen_trade_ids);
    merge_unmatched_synthetic_fills(
        &mut canonical.unmatched_synthetic_fills,
        alias.unmatched_synthetic_fills,
        target_unmatched_quantity,
        &mut canonical.commission_by_trade_id,
    )
    .expect("T-Bank alias commission projection merge must remain valid");
    // The scalar is a ledger of exactly the queue entries which have not yet been matched by a
    // real trade. Recompute it after merging instead of retaining a max which can disagree with
    // the queue when both aliases observed the same cumulative execution.
    canonical.unmatched_emitted_quantity = canonical
        .unmatched_synthetic_fills
        .iter()
        .map(|fill| fill.quantity)
        .sum();
    let retained_synthetic_trade_ids = canonical
        .unmatched_synthetic_fills
        .iter()
        .map(|fill| fill.trade_id.as_str())
        .collect::<HashSet<_>>();
    let mut matched_canonical_trade_ids = HashSet::new();
    for alias_fill in alias_unmatched_fills {
        if retained_synthetic_trade_ids.contains(alias_fill.trade_id.as_str()) {
            continue;
        }
        // The alias row was discarded as an already represented cumulative execution. Match each
        // discarded row to a distinct canonical row in queue order, so equal-sized fills retain
        // their commission provenance one-to-one.
        let Some(canonical_trade_id) = canonical
            .unmatched_synthetic_fills
            .iter()
            .find(|fill| {
                fill.quantity == alias_fill.quantity
                    && !matched_canonical_trade_ids.contains(fill.trade_id.as_str())
            })
            .map(|fill| fill.trade_id.clone())
        else {
            canonical
                .commission_by_trade_id
                .remove(alias_fill.trade_id.as_str());
            continue;
        };
        matched_canonical_trade_ids.insert(canonical_trade_id.clone());
        // Preserve a fee learned through the alias by upgrading its corresponding canonical row.
        if let Some(TbankTrackedFillCommission {
            commission: alias_commission,
            resolved_quantity: alias_resolved_quantity,
            ..
        }) = alias_commission_by_trade_id.get(alias_fill.trade_id.as_str())
            && let Some(alias_amount) = alias_commission.amount()
            && let Some(canonical_commission) = canonical
                .commission_by_trade_id
                .get_mut(canonical_trade_id.as_str())
            && tracked_commission_precedence(TbankTrackedFillCommission {
                quantity: alias_fill.quantity,
                resolved_quantity: *alias_resolved_quantity,
                commission: *alias_commission,
            }) > tracked_commission_precedence(*canonical_commission)
        {
            canonical_commission.commission =
                commission_with_status(*alias_commission, alias_amount);
            canonical_commission.resolved_quantity =
                (*alias_resolved_quantity).min(canonical_commission.quantity);
        }
        canonical
            .commission_by_trade_id
            .remove(alias_fill.trade_id.as_str());
    }
    merge_synthetic_fill_matches(
        &mut canonical.synthetic_matches_by_trade_id,
        alias.synthetic_matches_by_trade_id,
    );
    recompute_commission_tracking(&mut canonical);
    projection.orders.remove(alias_order_id);
    projection
        .orders
        .insert(canonical_order_id.to_string(), canonical);
}

fn merge_unmatched_synthetic_fills(
    canonical: &mut VecDeque<TbankSyntheticFill>,
    alias: VecDeque<TbankSyntheticFill>,
    target_quantity: Decimal,
    commission_by_trade_id: &mut HashMap<String, TbankTrackedFillCommission>,
) -> anyhow::Result<()> {
    for alias_fill in alias {
        if let Some(canonical_fill) = canonical
            .iter_mut()
            .find(|fill| fill.trade_id == alias_fill.trade_id)
        {
            canonical_fill.quantity = canonical_fill.quantity.max(alias_fill.quantity);
        } else {
            canonical.push_back(alias_fill);
        }
    }

    // Alias order IDs can produce different synthetic IDs for the same cumulative snapshot.
    // Keep the canonical queue first and admit alias entries only until the merged unmatched
    // quantity reaches the monotonic aggregate. This makes the queue and scalar one invariant;
    // a later real trade can then consume only the already-emitted cumulative quantity.
    let mut quantity = canonical.iter().map(|fill| fill.quantity).sum::<Decimal>();
    while quantity > target_quantity {
        let Some(last) = canonical.back_mut() else {
            break;
        };
        let excess = quantity - target_quantity;
        if last.quantity <= excess {
            quantity -= last.quantity;
            canonical.pop_back();
        } else {
            let retained_quantity = last.quantity - excess;
            last.quantity = retained_quantity;
            quantity = target_quantity;
        }
    }
    for fill in canonical.iter() {
        if let Some(tracked) = commission_by_trade_id.get_mut(fill.trade_id.as_str()) {
            synchronize_tracked_commission_quantity(tracked, fill.quantity)?;
        }
    }
    Ok(())
}

fn synchronize_tracked_commission_quantity(
    tracked: &mut TbankTrackedFillCommission,
    queue_quantity: Decimal,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        queue_quantity >= Decimal::ZERO,
        "invalid queue quantity while merging T-Bank alias commission: quantity={queue_quantity}",
    );
    if queue_quantity >= tracked.quantity {
        tracked.quantity = queue_quantity;
        return Ok(());
    }
    let old_resolved_quantity = tracked.resolved_quantity;
    let retained_resolved_quantity = if tracked.quantity > Decimal::ZERO {
        old_resolved_quantity
            .checked_mul(queue_quantity)
            .and_then(|quantity| quantity.checked_div(tracked.quantity))
            .ok_or_else(|| anyhow::anyhow!("T-Bank alias resolved quantity overflow"))?
    } else {
        Decimal::ZERO
    }
    .min(queue_quantity);
    if let Some(amount) = tracked.commission.amount() {
        anyhow::ensure!(
            old_resolved_quantity > Decimal::ZERO,
            "known T-Bank alias commission has no resolved quantity"
        );
        let retained_amount = allocate_money_by_weights(
            amount,
            &[
                retained_resolved_quantity,
                old_resolved_quantity - retained_resolved_quantity,
            ],
        )?
        .into_iter()
        .next()
        .expect("commission allocation includes the retained portion");
        tracked.commission = commission_with_status(tracked.commission, retained_amount);
    }
    tracked.quantity = queue_quantity;
    tracked.resolved_quantity = retained_resolved_quantity;
    Ok(())
}

fn merge_synthetic_fill_matches(
    canonical: &mut HashMap<String, Vec<TbankSyntheticFillMatch>>,
    alias: HashMap<String, Vec<TbankSyntheticFillMatch>>,
) {
    for (trade_id, alias_matches) in alias {
        let canonical_matches = canonical.entry(trade_id).or_default();
        let target_quantity = canonical_matches
            .iter()
            .map(|match_| match_.quantity)
            .sum::<Decimal>()
            .max(
                alias_matches
                    .iter()
                    .map(|match_| match_.quantity)
                    .sum::<Decimal>(),
            );
        for alias_match in alias_matches {
            if let Some(canonical_match) = canonical_matches
                .iter_mut()
                .find(|match_| match_.trade_id == alias_match.trade_id)
            {
                canonical_match.quantity = canonical_match.quantity.max(alias_match.quantity);
            } else {
                canonical_matches.push(alias_match);
            }
        }
        let mut quantity = canonical_matches
            .iter()
            .map(|match_| match_.quantity)
            .sum::<Decimal>();
        while quantity > target_quantity {
            let Some(last) = canonical_matches.last_mut() else {
                break;
            };
            let excess = quantity - target_quantity;
            if last.quantity <= excess {
                quantity -= last.quantity;
                canonical_matches.pop();
            } else {
                last.quantity -= excess;
                quantity = target_quantity;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct TbankProjectedOrderStatus {
    pub(super) status: OrderStatus,
    pub(super) ts_last: UnixNanos,
    pub(super) filled_quantity: Decimal,
}

#[derive(Debug, Clone)]
pub(super) struct TbankProjectedPosition {
    pub(super) account_id: AccountId,
    pub(super) instrument_id: InstrumentId,
    pub(super) source: TbankPositionProjectionSource,
    pub(super) is_flat: bool,
    pub(super) ts_last: UnixNanos,
    pub(super) securities_watermark: UnixNanos,
    pub(super) portfolio_watermark: UnixNanos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TbankPositionProjectionSource {
    SecuritiesSnapshot,
    PortfolioStream,
}

impl TbankProjectedPosition {
    fn source_watermark(&self, source: TbankPositionProjectionSource) -> UnixNanos {
        match source {
            TbankPositionProjectionSource::SecuritiesSnapshot => self.securities_watermark,
            TbankPositionProjectionSource::PortfolioStream => self.portfolio_watermark,
        }
    }

    fn advance_source_watermark(
        &mut self,
        source: TbankPositionProjectionSource,
        watermark: UnixNanos,
    ) {
        let current = match source {
            TbankPositionProjectionSource::SecuritiesSnapshot => &mut self.securities_watermark,
            TbankPositionProjectionSource::PortfolioStream => &mut self.portfolio_watermark,
        };
        if *current < watermark {
            *current = watermark;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::PositionSide,
        identifiers::InstrumentId,
        reports::PositionStatusReport,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use crate::execution::events::TbankFillCommission;

    fn current_unix_nanos() -> UnixNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after Unix epoch")
            .as_nanos();
        UnixNanos::from(u64::try_from(nanos).expect("current timestamp must fit u64"))
    }

    #[test]
    fn snapshot_commission_does_not_downgrade_live_provenance() {
        let mut order = super::TbankOrderFillProjection::default();
        for (trade_id, commission) in [
            ("unknown", TbankFillCommission::Unknown),
            (
                "allocated",
                TbankFillCommission::Allocated(Money::from("1 RUB")),
            ),
            (
                "reported",
                TbankFillCommission::Reported(Money::from("1 RUB")),
            ),
        ] {
            order.commission_by_trade_id.insert(
                trade_id.to_string(),
                super::TbankTrackedFillCommission {
                    quantity: Decimal::ONE,
                    resolved_quantity: Decimal::ONE,
                    commission,
                },
            );
        }
        let projection = super::TbankFillProjection {
            orders: HashMap::from([("venue-order".to_string(), order)]),
        };

        assert!(super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "missing",
            TbankFillCommission::Unknown,
        ));
        assert!(super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "unknown",
            TbankFillCommission::Unknown,
        ));
        assert!(super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "unknown",
            TbankFillCommission::Reported(Money::from("1 RUB")),
        ));
        assert!(super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "allocated",
            TbankFillCommission::Reported(Money::from("1 RUB")),
        ));
        assert!(!super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "reported",
            TbankFillCommission::Unknown,
        ));
        assert!(!super::snapshot_fill_commission_is_newer(
            &projection,
            "venue-order",
            "reported",
            TbankFillCommission::Reported(Money::from("2 RUB")),
        ));
    }

    #[test]
    fn snapshot_provenance_upgrade_changes_only_the_existing_commission_ledger() {
        let mut order = super::TbankOrderFillProjection::default();
        order.cumulative_filled_quantity = Decimal::from(10);
        order.emitted_fill_quantity = Decimal::from(10);
        order.emitted_fill_notional = Decimal::from(1_000);
        order.unmatched_emitted_quantity = Decimal::from(2);
        order.seen_trade_ids.insert("live-trade".to_string());
        order
            .unmatched_synthetic_fills
            .push_back(super::TbankSyntheticFill {
                trade_id: "pending-synthetic".to_string(),
                quantity: Decimal::from(2),
            });
        order.synthetic_matches_by_trade_id.insert(
            "matched-trade".to_string(),
            vec![super::TbankSyntheticFillMatch {
                trade_id: "synthetic-trade".to_string(),
                quantity: Decimal::from(2),
            }],
        );
        order.commission_by_trade_id.insert(
            "live-trade".to_string(),
            super::TbankTrackedFillCommission {
                quantity: Decimal::from(10),
                resolved_quantity: Decimal::ZERO,
                commission: TbankFillCommission::Unknown,
            },
        );
        order.unknown_commission_quantity = Decimal::from(10);

        let projection = Arc::new(Mutex::new(super::TbankFillProjection {
            orders: HashMap::from([("venue-order".to_string(), order)]),
        }));
        let execution_state = |order: &super::TbankOrderFillProjection| {
            (
                order.cumulative_filled_quantity,
                order.emitted_fill_quantity,
                order.emitted_fill_notional,
                order.unmatched_emitted_quantity,
                order.seen_trade_ids.clone(),
                order
                    .unmatched_synthetic_fills
                    .iter()
                    .map(|fill| (fill.trade_id.clone(), fill.quantity))
                    .collect::<Vec<_>>(),
                order
                    .synthetic_matches_by_trade_id
                    .iter()
                    .map(|(trade_id, matches)| {
                        (
                            trade_id.clone(),
                            matches
                                .iter()
                                .map(|match_| (match_.trade_id.clone(), match_.quantity))
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<HashMap<_, _>>(),
            )
        };
        let before = {
            let projection = projection.lock().unwrap();
            execution_state(&projection.orders["venue-order"])
        };

        assert!(!super::apply_snapshot_fill_provenance(
            &mut projection.lock().unwrap(),
            "venue-order",
            "live-trade",
            Decimal::from(9),
            TbankFillCommission::Reported(Money::from("1 RUB")),
        ));
        {
            let projection = projection.lock().unwrap();
            let order = &projection.orders["venue-order"];
            assert_eq!(execution_state(order), before);
            assert_eq!(
                order.commission_by_trade_id["live-trade"].commission,
                TbankFillCommission::Unknown
            );
        }

        assert!(super::apply_snapshot_fill_provenance(
            &mut projection.lock().unwrap(),
            "venue-order",
            "live-trade",
            Decimal::from(10),
            TbankFillCommission::Reported(Money::from("1 RUB")),
        ));

        {
            let projection = projection.lock().unwrap();
            let order = &projection.orders["venue-order"];
            let tracked = &order.commission_by_trade_id["live-trade"];
            assert_eq!(tracked.quantity, Decimal::from(10));
            assert_eq!(tracked.resolved_quantity, Decimal::from(10));
            assert_eq!(
                tracked.commission,
                TbankFillCommission::Reported(Money::from("1 RUB"))
            );
            assert_eq!(order.unknown_commission_quantity, Decimal::ZERO);
            assert_eq!(order.emitted_commission, Some(Money::from("1 RUB")));
            assert_eq!(execution_state(order), before);
        }

        assert!(
            super::project_cumulative_order_fill(
                &projection,
                "venue-order",
                "query-snapshot",
                Decimal::from(10),
                Decimal::from(1_000),
                Some(Money::from("1 RUB")),
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            projection.lock().unwrap().orders["venue-order"].commission_by_trade_id["live-trade"]
                .commission,
            TbankFillCommission::Reported(Money::from("1 RUB"))
        );
    }

    fn position_report(
        account_id: nautilus_model::identifiers::AccountId,
        instrument_id: InstrumentId,
        position_side: PositionSide,
        quantity: Quantity,
        ts_last: UnixNanos,
        venue_position_id: &str,
    ) -> PositionStatusReport {
        PositionStatusReport::new(
            account_id,
            instrument_id,
            position_side,
            quantity,
            ts_last,
            ts_last,
            Some(UUID4::new()),
            Some(venue_position_id.into()),
            None,
        )
    }

    #[test]
    fn incomplete_position_snapshot_preserves_missing_projected_position() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id: nautilus_model::identifiers::AccountId = "TBANK-001".into();
        let instrument_id: InstrumentId = "SBER_TQBR.MOEX".parse().unwrap();
        let active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(20),
            current_unix_nanos(),
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let mut partial_snapshot = Vec::new();

        super::apply_position_snapshot(
            &projection,
            account_id,
            &mut partial_snapshot,
            current_unix_nanos(),
            super::TbankPositionProjectionSource::SecuritiesSnapshot,
            false,
        );

        assert!(partial_snapshot.is_empty());
        assert_eq!(projection.lock().unwrap().len(), 1);
    }

    #[test]
    fn older_snapshot_does_not_flatten_newer_stream_position() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id = "TBANK-001".into();
        let instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
        let snapshot_boundary = UnixNanos::from(100_u64);
        let newer_stream_ts = UnixNanos::from(200_u64);
        let active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            newer_stream_ts,
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let mut empty_snapshot = Vec::new();

        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_snapshot,
            snapshot_boundary,
        );

        assert!(empty_snapshot.is_empty());
        assert!(!projection.lock().unwrap().values().next().unwrap().is_flat);
    }

    #[test]
    fn authoritative_empty_snapshots_create_and_advance_flat_tombstone() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id = "TBANK-001".into();
        let instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
        let first_snapshot_ts = UnixNanos::from(100_u64);
        let second_snapshot_ts = UnixNanos::from(300_u64);
        let delayed_active_ts = UnixNanos::from(200_u64);
        let active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            first_snapshot_ts,
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let mut empty_snapshot = Vec::new();
        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_snapshot,
            first_snapshot_ts,
        );
        assert_eq!(empty_snapshot.len(), 1);
        assert_eq!(empty_snapshot[0].instrument_id, instrument_id);
        assert_eq!(empty_snapshot[0].position_side, PositionSide::Flat);
        assert_eq!(empty_snapshot[0].quantity.as_decimal(), Decimal::ZERO);
        assert_eq!(empty_snapshot[0].venue_position_id, None);
        {
            let projection = projection.lock().unwrap();
            let tombstone = projection.values().next().unwrap();
            assert!(tombstone.is_flat);
            assert_eq!(
                tombstone.source,
                super::TbankPositionProjectionSource::SecuritiesSnapshot
            );
        }
        empty_snapshot.clear();
        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_snapshot,
            second_snapshot_ts,
        );
        assert!(empty_snapshot.is_empty());
        let delayed_active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            delayed_active_ts,
            "SBER-POSITION",
        );

        assert!(!super::record_position_projection(
            &projection,
            &delayed_active,
        ));
        let projection = projection.lock().unwrap();
        let tombstone = projection.values().next().unwrap();
        assert!(tombstone.is_flat);
        assert_eq!(tombstone.ts_last, second_snapshot_ts);
    }

    #[test]
    fn securities_snapshot_watermark_rejects_older_update_without_flattening_portfolio() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id = "TBANK-001".into();
        let instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
        let portfolio_ts = UnixNanos::from(200_u64);
        let securities_snapshot_ts = UnixNanos::from(300_u64);
        let delayed_securities_ts = UnixNanos::from(250_u64);
        let portfolio = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            portfolio_ts,
            "SBER-POSITION",
        );
        super::record_portfolio_position_projection(&projection, &portfolio);
        let mut empty_snapshot = Vec::new();
        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_snapshot,
            securities_snapshot_ts,
        );
        let delayed_securities = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            delayed_securities_ts,
            "SBER-POSITION",
        );

        assert!(empty_snapshot.is_empty());
        assert!(!super::record_position_projection(
            &projection,
            &delayed_securities,
        ));
        let projection = projection.lock().unwrap();
        let current = projection.values().next().unwrap();
        assert!(!current.is_flat);
        assert_eq!(
            current.source,
            super::TbankPositionProjectionSource::PortfolioStream
        );
        assert_eq!(current.securities_watermark, securities_snapshot_ts);
    }

    #[test]
    fn portfolio_update_supersedes_older_securities_snapshot_authority() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id: nautilus_model::identifiers::AccountId = "TBANK-001".into();
        let security = position_report(
            account_id,
            "SBER_TQBR.MOEX".parse().unwrap(),
            PositionSide::Long,
            Quantity::from(10),
            current_unix_nanos(),
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &security);
        super::record_portfolio_position_projection(&projection, &security);
        let mut empty_securities_snapshot = Vec::new();

        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_securities_snapshot,
            current_unix_nanos(),
        );

        assert!(empty_securities_snapshot.is_empty());
        assert_eq!(projection.lock().unwrap().len(), 1);
    }

    #[test]
    fn portfolio_flat_supersedes_older_securities_position() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id = "TBANK-001".into();
        let instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
        let active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            current_unix_nanos(),
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let flat = position_report(
            account_id,
            instrument_id,
            PositionSide::Flat,
            Quantity::from(0),
            current_unix_nanos(),
            "SBER-POSITION",
        );

        assert!(super::record_position_projection_from_source(
            &projection,
            &flat,
            super::TbankPositionProjectionSource::PortfolioStream,
        ));
        let projection_guard = projection.lock().unwrap();
        assert_eq!(projection_guard.len(), 1);
        let tombstone = projection_guard.values().next().unwrap();
        assert!(tombstone.is_flat);
        assert_eq!(
            tombstone.source,
            super::TbankPositionProjectionSource::PortfolioStream
        );
        drop(projection_guard);
        assert!(!super::record_position_projection(&projection, &active));
        let projection_guard = projection.lock().unwrap();
        assert!(projection_guard.values().next().unwrap().is_flat);
        drop(projection_guard);
        let reopened_ts = UnixNanos::from(flat.ts_last.as_u64().saturating_add(1));
        let reopened = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            reopened_ts,
            "SBER-POSITION",
        );
        assert!(super::record_position_projection(&projection, &reopened));
        assert!(!projection.lock().unwrap().values().next().unwrap().is_flat);
    }

    #[test]
    fn explicit_flat_snapshot_is_applied_once_before_watermark_advances() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id = "TBANK-001".into();
        let instrument_id = "SBER_TQBR.MOEX".parse().unwrap();
        let active_ts = UnixNanos::from(100_u64);
        let flat_ts = UnixNanos::from(150_u64);
        let snapshot_boundary = UnixNanos::from(200_u64);
        let active = position_report(
            account_id,
            instrument_id,
            PositionSide::Long,
            Quantity::from(10),
            active_ts,
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let mut reports = vec![position_report(
            account_id,
            instrument_id,
            PositionSide::Flat,
            Quantity::from(0),
            flat_ts,
            "SBER-POSITION",
        )];

        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut reports,
            snapshot_boundary,
        );

        assert_eq!(reports.len(), 1);
        let projection = projection.lock().unwrap();
        assert_eq!(projection.len(), 1);
        let tombstone = projection.values().next().unwrap();
        assert!(tombstone.is_flat);
        assert_eq!(
            tombstone.source,
            super::TbankPositionProjectionSource::SecuritiesSnapshot
        );
        assert_eq!(tombstone.securities_watermark, snapshot_boundary);
    }

    #[test]
    fn portfolio_security_is_not_closed_by_independent_initial_positions_snapshot() {
        let projection = Arc::new(Mutex::new(HashMap::new()));
        let account_id: nautilus_model::identifiers::AccountId = "TBANK-001".into();
        let security = position_report(
            account_id,
            "SBER_TQBR.MOEX".parse().unwrap(),
            PositionSide::Long,
            Quantity::from(10),
            current_unix_nanos(),
            "SBER-POSITION",
        );
        super::record_portfolio_position_projection(&projection, &security);
        let mut empty_securities_snapshot = Vec::new();

        super::reconcile_position_snapshot(
            &projection,
            account_id,
            &mut empty_securities_snapshot,
            current_unix_nanos(),
        );

        assert!(empty_securities_snapshot.is_empty());
        assert_eq!(projection.lock().unwrap().len(), 1);

        let mut empty_portfolio_snapshot = Vec::new();
        super::reconcile_portfolio_snapshot(
            &projection,
            account_id,
            &mut empty_portfolio_snapshot,
            current_unix_nanos(),
        );
        assert_eq!(empty_portfolio_snapshot.len(), 1);
        assert_eq!(
            empty_portfolio_snapshot[0].position_side,
            PositionSide::Flat
        );
        let projection = projection.lock().unwrap();
        assert_eq!(projection.len(), 1);
        let tombstone = projection.values().next().unwrap();
        assert!(tombstone.is_flat);
        assert_eq!(
            tombstone.source,
            super::TbankPositionProjectionSource::PortfolioStream
        );
    }

    #[test]
    fn cumulative_snapshot_below_emitted_quantity_is_ignored() {
        let mut order = super::TbankOrderFillProjection {
            emitted_fill_quantity: Decimal::from(10),
            unknown_commission_quantity: Decimal::from(10),
            ..super::TbankOrderFillProjection::default()
        };
        order.commission_by_trade_id.insert(
            "stream-trade-10".to_string(),
            super::TbankTrackedFillCommission {
                quantity: Decimal::from(10),
                resolved_quantity: Decimal::ZERO,
                commission: TbankFillCommission::Unknown,
            },
        );
        let projection = Arc::new(Mutex::new(super::TbankFillProjection {
            orders: HashMap::from([("broker-order-1".to_string(), order)]),
        }));

        assert!(
            super::project_cumulative_order_fill(
                &projection,
                "broker-order-1",
                "stale-order-state-5",
                Decimal::from(5),
                Decimal::from(1_375),
                Some(Money::from("0.5 RUB")),
            )
            .unwrap()
            .is_none()
        );

        let order = projection.lock().unwrap().orders["broker-order-1"].clone();
        assert_eq!(order.cumulative_filled_quantity, Decimal::ZERO);
        assert_eq!(order.unknown_commission_quantity, Decimal::from(10));
        assert_eq!(
            order.commission_by_trade_id["stream-trade-10"].commission,
            TbankFillCommission::Unknown
        );

        let correction = super::project_cumulative_order_fill(
            &projection,
            "broker-order-1",
            "fresh-order-state-10",
            Decimal::from(10),
            Decimal::from(2_750),
            Some(Money::from("1 RUB")),
        )
        .unwrap()
        .expect("fresh snapshot resolves the full emitted fill");
        assert_eq!(correction.quantity.as_decimal(), Decimal::from(10));
        assert_eq!(
            correction.commission,
            TbankFillCommission::Allocated(Money::from("1 RUB"))
        );
    }
}

pub(super) fn position_projection_accepts(
    previous: Option<&TbankProjectedPosition>,
    report: &PositionStatusReport,
    source: TbankPositionProjectionSource,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };
    if report.ts_last < previous.ts_last || report.ts_last < previous.source_watermark(source) {
        return false;
    }
    !(report.ts_last == previous.ts_last
        && source == TbankPositionProjectionSource::SecuritiesSnapshot
        && previous.source == TbankPositionProjectionSource::PortfolioStream)
}

pub(super) fn projected_position_from_report(
    previous: Option<&TbankProjectedPosition>,
    report: &PositionStatusReport,
    source: TbankPositionProjectionSource,
) -> TbankProjectedPosition {
    let mut position = TbankProjectedPosition {
        account_id: report.account_id,
        instrument_id: report.instrument_id,
        source,
        is_flat: report.position_side == PositionSide::Flat
            || report.quantity.as_decimal() == Decimal::ZERO,
        ts_last: report.ts_last,
        securities_watermark: previous
            .map(|position| position.securities_watermark)
            .unwrap_or_default(),
        portfolio_watermark: previous
            .map(|position| position.portfolio_watermark)
            .unwrap_or_default(),
    };
    position.advance_source_watermark(source, report.ts_last);
    position
}

pub(super) fn order_status_rank(status: OrderStatus) -> u8 {
    match status {
        OrderStatus::Accepted => 1,
        OrderStatus::Triggered => 2,
        OrderStatus::PartiallyFilled => 3,
        OrderStatus::Filled
        | OrderStatus::Canceled
        | OrderStatus::Rejected
        | OrderStatus::Expired => 4,
        _ => 0,
    }
}

pub(super) fn project_order_status_report(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedOrderStatus>>>,
    mut report: OrderStatusReport,
) -> Option<OrderStatusReport> {
    let key = report.venue_order_id.to_string();
    let mut next = TbankProjectedOrderStatus {
        status: report.order_status,
        ts_last: report.ts_last,
        filled_quantity: report.filled_qty.as_decimal(),
    };
    let mut projection = projection.lock().expect("order_status_projection lock");
    if let Some(previous) = projection.get(key.as_str()) {
        let previous_rank = order_status_rank(previous.status);
        let next_rank = order_status_rank(next.status);
        let lifecycle_progress =
            next_rank > previous_rank || next.filled_quantity > previous.filled_quantity;
        let previous_is_terminal = matches!(
            previous.status,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Rejected
                | OrderStatus::Expired
        );
        if next_rank < previous_rank
            || (!lifecycle_progress && next.ts_last < previous.ts_last)
            || (next.status == previous.status && next.filled_quantity <= previous.filled_quantity)
            || (previous_is_terminal
                && (next.filled_quantity <= previous.filled_quantity
                    || (next.status != previous.status && next.status != OrderStatus::Filled)))
        {
            return None;
        }
        next.ts_last = next.ts_last.max(previous.ts_last);
    }
    report.ts_last = next.ts_last;
    projection.insert(key, next);
    Some(report)
}

#[derive(Debug, Clone)]
pub(super) struct TbankProjectedFill {
    pub(super) quantity: Quantity,
    pub(super) price: Price,
    /// Venue commission, or `Unknown` when the source message carries no commission field.
    pub(super) commission: TbankFillCommission,
    /// A repeated cumulative snapshot may only correct provenance for an already emitted fill.
    /// Keep that outcome explicit so callers publish the custom event without emitting a second
    /// Nautilus fill.
    pub(super) provenance_only: bool,
    pub(super) trade_id: Option<String>,
}

#[cfg(test)]
pub(super) fn project_cumulative_order_fill(
    projection: &Arc<Mutex<TbankFillProjection>>,
    order_id: &str,
    trade_id: &str,
    cumulative_quantity: Decimal,
    cumulative_notional: Decimal,
    cumulative_commission: Option<Money>,
) -> anyhow::Result<Option<TbankProjectedFill>> {
    project_cumulative_order_fill_with_publication(
        projection,
        order_id,
        trade_id,
        cumulative_quantity,
        cumulative_notional,
        cumulative_commission,
        |_| Ok(()),
    )
}

/// Stages a cumulative fill update and commits it only after the required publication succeeds.
pub(super) fn project_cumulative_order_fill_with_publication(
    projection: &Arc<Mutex<TbankFillProjection>>,
    order_id: &str,
    trade_id: &str,
    cumulative_quantity: Decimal,
    cumulative_notional: Decimal,
    cumulative_commission: Option<Money>,
    publish: impl FnOnce(Option<&TbankProjectedFill>) -> anyhow::Result<()>,
) -> anyhow::Result<Option<TbankProjectedFill>> {
    if cumulative_quantity <= Decimal::ZERO {
        return Ok(None);
    }
    let mut projection = projection.lock().expect("fill_projection lock");
    let mut order = projection.orders.get(order_id).cloned().unwrap_or_default();
    let observed_filled_quantity = order
        .cumulative_filled_quantity
        .max(order.emitted_fill_quantity);
    if cumulative_quantity < observed_filled_quantity {
        // A lagging order-state response may carry a cumulative commission for only the older
        // quantity. Never let it correct provenance for a fill already projected at a newer
        // execution watermark, even if a producer has not advanced the snapshot watermark yet.
        return Ok(None);
    }
    order.cumulative_filled_quantity = cumulative_quantity;
    let quantity_decimal = cumulative_quantity - order.emitted_fill_quantity;
    let projected = if quantity_decimal <= Decimal::ZERO {
        let correction = match cumulative_commission {
            Some(commission) => update_cumulative_commission_tracking(&mut order, commission)?,
            None => None,
        };
        let projected = correction
            .map(
                |(trade_id, quantity, commission)| -> anyhow::Result<TbankProjectedFill> {
                    Ok(TbankProjectedFill {
                        quantity: Quantity::from_decimal(quantity)?,
                        price: Price::from_decimal_dp(
                            cumulative_notional / cumulative_quantity,
                            nautilus_model::types::fixed::FIXED_PRECISION,
                        )?,
                        commission,
                        provenance_only: true,
                        trade_id: Some(trade_id),
                    })
                },
            )
            .transpose()?;
        projected
    } else {
        let residual_notional = cumulative_notional - order.emitted_fill_notional;
        anyhow::ensure!(
            residual_notional >= Decimal::ZERO,
            "cumulative execution notional regressed for order {order_id}: cumulative={cumulative_notional}, emitted={}",
            order.emitted_fill_notional,
        );
        let quantity = Quantity::from_decimal(quantity_decimal)?;
        let price = Price::from_decimal_dp(
            residual_notional / quantity_decimal,
            nautilus_model::types::fixed::FIXED_PRECISION,
        )?;
        order.emitted_fill_quantity += quantity_decimal;
        order.emitted_fill_notional += residual_notional;
        order.unmatched_emitted_quantity += quantity_decimal;
        let commission = project_cumulative_commission(&mut order, cumulative_commission)?;
        order.commission_by_trade_id.insert(
            trade_id.to_string(),
            TbankTrackedFillCommission {
                quantity: quantity_decimal,
                resolved_quantity: if commission.amount().is_some() {
                    quantity_decimal
                } else {
                    Decimal::ZERO
                },
                commission,
            },
        );
        order
            .unmatched_synthetic_fills
            .push_back(TbankSyntheticFill {
                trade_id: trade_id.to_string(),
                quantity: quantity_decimal,
            });
        recompute_commission_tracking(&mut order);
        Some(TbankProjectedFill {
            quantity,
            price,
            commission,
            provenance_only: false,
            trade_id: None,
        })
    };
    publish(projected.as_ref())?;
    projection.orders.insert(order_id.to_string(), order);
    Ok(projected)
}

fn update_cumulative_commission_tracking(
    order: &mut TbankOrderFillProjection,
    cumulative_commission: Money,
) -> anyhow::Result<Option<(String, Decimal, TbankFillCommission)>> {
    // The order-state commission is cumulative, while both synthetic trade IDs and tracked
    // quantities describe individual deltas. It can correct a fill only when that fill is the
    // sole unresolved commission on the order. A partially operations-resolved entry still has
    // an unknown remainder, so it stays a candidate: subtracting its already resolved amount
    // yields the fee of the unresolved lots and resolves the row in full.
    let mut unresolved_fills = order.commission_by_trade_id.iter().filter(|(_, tracked)| {
        matches!(tracked.commission, TbankFillCommission::Unknown)
            || tracked.resolved_quantity < tracked.quantity
    });
    let Some((candidate_trade_id, candidate_tracked)) = unresolved_fills.next() else {
        return Ok(None);
    };
    if unresolved_fills.next().is_some() {
        return Ok(None);
    }
    let candidate_trade_id = candidate_trade_id.clone();
    let candidate_quantity = candidate_tracked.quantity;
    let candidate_reported = candidate_tracked.commission.amount();

    let mut known_commission = Decimal::ZERO;
    for tracked in order.commission_by_trade_id.values() {
        let Some(commission) = tracked.commission.amount() else {
            continue;
        };
        if commission.currency != cumulative_commission.currency {
            return Ok(None);
        }
        known_commission += commission.as_decimal();
    }
    let incremental_commission = cumulative_commission.as_decimal() - known_commission;
    if incremental_commission < Decimal::ZERO {
        return Ok(None);
    }
    if let Some(candidate_reported) = candidate_reported
        && candidate_reported.currency != cumulative_commission.currency
    {
        return Ok(None);
    }
    let amount = Money::from_decimal(
        incremental_commission + candidate_reported.map_or(Decimal::ZERO, |m| m.as_decimal()),
        cumulative_commission.currency,
    )?;
    let commission = TbankFillCommission::Allocated(amount);
    let Some(tracked) = order
        .commission_by_trade_id
        .get_mut(candidate_trade_id.as_str())
    else {
        return Ok(None);
    };
    tracked.commission = commission;
    tracked.resolved_quantity = tracked.quantity;
    recompute_commission_tracking(order);
    Ok(Some((candidate_trade_id, candidate_quantity, commission)))
}

fn project_cumulative_commission(
    order: &mut TbankOrderFillProjection,
    cumulative_commission: Option<Money>,
) -> anyhow::Result<TbankFillCommission> {
    let Some(cumulative_commission) = cumulative_commission else {
        return Ok(TbankFillCommission::Unknown);
    };
    // A cumulative order commission cannot be attributed to the new remainder while an earlier
    // fill still has unknown provenance. Keeping the new fill unknown is fail-closed; a later
    // operations-cursor observation can upgrade each trade without replaying execution fills.
    if order.unknown_commission_quantity > Decimal::ZERO {
        return Ok(TbankFillCommission::Unknown);
    }
    let emitted_commission = order
        .emitted_commission
        .filter(|emitted| emitted.currency == cumulative_commission.currency)
        .map(|emitted| emitted.as_decimal())
        .unwrap_or(Decimal::ZERO);
    let incremental_commission =
        (cumulative_commission.as_decimal() - emitted_commission).max(Decimal::ZERO);
    let amount = Money::from_decimal(incremental_commission, cumulative_commission.currency)?;
    // Executed commission is cumulative at the order level. Its per-fill delta is an
    // attribution computed by the adapter, even when only one fill is currently visible.
    Ok(TbankFillCommission::Allocated(amount))
}

#[cfg(test)]
pub(super) fn project_trade_fill_report(
    projection: &Arc<Mutex<TbankFillProjection>>,
    report: FillReport,
) -> anyhow::Result<Option<FillReport>> {
    let mut projection = projection.lock().expect("fill_projection lock");
    let commission = TbankFillCommission::Reported(report.commission);
    project_trade_fill_report_locked(&mut projection, report, commission)
}

pub(super) fn project_trade_fill_report_locked(
    projection: &mut TbankFillProjection,
    mut report: FillReport,
    commission: TbankFillCommission,
) -> anyhow::Result<Option<FillReport>> {
    let order_id = report.venue_order_id.to_string();
    let trade_id = report.trade_id.to_string();
    let source_quantity = report.last_qty.as_decimal();
    if source_quantity <= Decimal::ZERO {
        return Ok(None);
    }

    let mut order = projection
        .orders
        .get(&order_id)
        .cloned()
        .unwrap_or_default();
    if !order.seen_trade_ids.insert(trade_id.clone()) {
        return Ok(None);
    }
    let mut emit_quantity = source_quantity;
    if order.unmatched_emitted_quantity > Decimal::ZERO {
        let mut consumed = Decimal::ZERO;
        while emit_quantity > Decimal::ZERO {
            let Some(synthetic_fill) = order.unmatched_synthetic_fills.front_mut() else {
                break;
            };
            let matched_quantity = synthetic_fill.quantity.min(emit_quantity);
            if matched_quantity <= Decimal::ZERO {
                order.unmatched_synthetic_fills.pop_front();
                continue;
            }
            let synthetic_trade_id = synthetic_fill.trade_id.clone();
            synthetic_fill.quantity -= matched_quantity;
            if synthetic_fill.quantity <= Decimal::ZERO {
                order.unmatched_synthetic_fills.pop_front();
            }
            order
                .synthetic_matches_by_trade_id
                .entry(trade_id.clone())
                .or_default()
                .push(TbankSyntheticFillMatch {
                    trade_id: synthetic_trade_id,
                    quantity: matched_quantity,
                });
            consumed += matched_quantity;
            emit_quantity -= matched_quantity;
        }
        order.unmatched_emitted_quantity -= consumed;
    }
    if emit_quantity <= Decimal::ZERO {
        // The broker trade only identifies an execution already emitted from cumulative order
        // state. Keep its identity in `synthetic_matches_by_trade_id`, but do not add a second
        // commission-tracking row: that would count the same execution twice and would leave an
        // unknown duplicate behind when the broker trade has no fee field.
        projection.orders.insert(order_id, order);
        return Ok(None);
    }
    order.commission_by_trade_id.insert(
        trade_id.clone(),
        TbankTrackedFillCommission {
            quantity: emit_quantity,
            resolved_quantity: if commission.amount().is_some() {
                emit_quantity
            } else {
                Decimal::ZERO
            },
            commission,
        },
    );
    recompute_commission_tracking(&mut order);
    let emitted_notional = report.last_px.as_decimal() * emit_quantity;
    order.emitted_fill_quantity += emit_quantity;
    order.emitted_fill_notional += emitted_notional;
    if order.emitted_fill_quantity > order.cumulative_filled_quantity {
        order.cumulative_filled_quantity = order.emitted_fill_quantity;
    }

    if emit_quantity != source_quantity {
        report.last_qty = Quantity::from_decimal(emit_quantity)?;
        report.commission = scale_commission(report.commission, emit_quantity, source_quantity)?;
    }
    projection.orders.insert(order_id, order);
    Ok(Some(report))
}

/// Records a venue commission learned later for a fill which was already accepted from a source
/// without commission data. This deliberately returns only a provenance transition: the Nautilus
/// execution fill must not be emitted a second time.
pub(super) fn update_duplicate_fill_provenance(
    projection: &mut TbankFillProjection,
    report: &FillReport,
    commission: TbankFillCommission,
) -> anyhow::Result<(
    Vec<(String, TbankFillCommission)>,
    Option<TbankFillCommission>,
)> {
    let Some(reported) = commission.amount() else {
        return Ok((Vec::new(), None));
    };
    let order_id = report.venue_order_id.to_string();
    let trade_id = report.trade_id.to_string();
    let Some(order) = projection.orders.get_mut(order_id.as_str()) else {
        return Ok((Vec::new(), None));
    };

    let synthetic_matches = order
        .synthetic_matches_by_trade_id
        .remove(trade_id.as_str())
        .unwrap_or_default();
    if synthetic_matches.is_empty() {
        if let Some(tracked) = order.commission_by_trade_id.get_mut(trade_id.as_str())
            && (tracked.commission.status()
                == crate::execution::events::TbankFillCommissionStatus::Unknown
                || (commission.status()
                    == crate::execution::events::TbankFillCommissionStatus::Reported
                    && tracked.commission.status()
                        == crate::execution::events::TbankFillCommissionStatus::Allocated))
        {
            tracked.resolved_quantity = tracked.quantity;
            tracked.commission = commission_with_status(commission, reported);
            recompute_commission_tracking(order);
            return Ok((
                vec![(trade_id, commission_with_status(commission, reported))],
                None,
            ));
        }
        return Ok((Vec::new(), None));
    }

    let source_quantity = report.last_qty.as_decimal();
    anyhow::ensure!(
        source_quantity > Decimal::ZERO,
        "cannot allocate commission for non-positive T-Bank trade quantity"
    );
    let matched_quantity = synthetic_matches
        .iter()
        .map(|match_| match_.quantity)
        .sum::<Decimal>();
    anyhow::ensure!(
        matched_quantity <= source_quantity,
        "synthetic T-Bank fill match exceeds source trade quantity: matched={matched_quantity}, source={source_quantity}"
    );

    let residual_quantity = source_quantity - matched_quantity;
    let mut allocations = Vec::with_capacity(
        synthetic_matches.len() + usize::from(residual_quantity > Decimal::ZERO),
    );
    let mut fixed_commission_quantities = Vec::<(String, Decimal)>::new();
    for match_ in &synthetic_matches {
        // A tracked row is a fixed commission source only while fully resolved: every lot of the
        // synthetic fill is then covered by its recorded commission. A partially resolved row
        // still has lots with unknown provenance, so its matched share joins the allocation.
        let is_fully_resolved = order
            .commission_by_trade_id
            .get(match_.trade_id.as_str())
            .is_some_and(|tracked| {
                matches!(
                    tracked.commission,
                    TbankFillCommission::Reported(_) | TbankFillCommission::Allocated(_)
                ) && tracked.resolved_quantity == tracked.quantity
            });
        if is_fully_resolved {
            if let Some((_, quantity)) = fixed_commission_quantities
                .iter_mut()
                .find(|(trade_id, _)| trade_id == &match_.trade_id)
            {
                *quantity += match_.quantity;
            } else {
                fixed_commission_quantities.push((match_.trade_id.clone(), match_.quantity));
            }
        } else {
            allocations.push((match_.trade_id.as_str(), match_.quantity));
        }
    }
    if residual_quantity > Decimal::ZERO {
        allocations.push((trade_id.as_str(), residual_quantity));
    }

    // Keep a venue-reported status only when the operation fee maps to one fill. Already
    // resolved synthetic fills count too: their commission is reserved below, but still forms
    // part of this operation's multi-fill attribution.
    let mut attributed_trade_ids = HashSet::new();
    attributed_trade_ids.extend(
        allocations
            .iter()
            .map(|(target_trade_id, _)| *target_trade_id),
    );
    attributed_trade_ids.extend(
        fixed_commission_quantities
            .iter()
            .map(|(target_trade_id, _)| target_trade_id.as_str()),
    );
    let allocation_commission = match commission {
        TbankFillCommission::Reported(_) if attributed_trade_ids.len() > 1 => {
            TbankFillCommission::Allocated(reported)
        }
        other => other,
    };

    if allocations.is_empty() {
        // A per-trade Operations observation is more precise than the order-level amount used
        // to create a synthetic fill. Upgrade the provenance only when the source trade maps
        // one-to-one to the complete synthetic fill; otherwise its reported amount cannot be
        // represented as a per-fill amount without inventing an allocation.
        if matches!(commission, TbankFillCommission::Reported(_))
            && fixed_commission_quantities.len() == 1
            && fixed_commission_quantities[0].1 == source_quantity
        {
            let (synthetic_trade_id, matched_quantity) = &fixed_commission_quantities[0];
            if let Some(tracked) = order
                .commission_by_trade_id
                .get_mut(synthetic_trade_id.as_str())
                && tracked.quantity == *matched_quantity
                && tracked.resolved_quantity == tracked.quantity
            {
                let reported_commission = TbankFillCommission::Reported(reported);
                tracked.commission = reported_commission;
                recompute_commission_tracking(order);
                return Ok((
                    vec![(synthetic_trade_id.clone(), reported_commission)],
                    None,
                ));
            }
        }

        // Every other matched portion already has a commission from cumulative order state.
        // Keep that amount because the operation cannot be mapped to a single complete fill.
        recompute_commission_tracking(order);
        return Ok((Vec::new(), None));
    }

    // Order-state commissions already account for their matched portions. Reserve those amounts
    // before allocating this operation's total across the still-unresolved synthetic portions and
    // the publishable residual. For a partial match, reserve only the matched share of the known
    // synthetic fill commission.
    let mut reserved_commission_minor = 0_i128;
    for (synthetic_trade_id, matched_quantity) in fixed_commission_quantities {
        let Some(TbankTrackedFillCommission {
            quantity: tracked_quantity,
            commission:
                TbankFillCommission::Reported(known_commission)
                | TbankFillCommission::Allocated(known_commission),
            ..
        }) = order
            .commission_by_trade_id
            .get(synthetic_trade_id.as_str())
        else {
            continue;
        };
        if known_commission.currency != reported.currency {
            continue;
        }
        anyhow::ensure!(
            matched_quantity <= *tracked_quantity,
            "synthetic T-Bank fill matches exceed tracked quantity: trade_id={synthetic_trade_id}, matched={matched_quantity}, tracked={tracked_quantity}"
        );
        let unmatched_quantity = *tracked_quantity - matched_quantity;
        let matched_commission =
            allocate_money_by_weights(*known_commission, &[matched_quantity, unmatched_quantity])?
                .into_iter()
                .next()
                .expect("commission allocation has a matched portion");
        reserved_commission_minor = reserved_commission_minor
            .checked_add(money_to_minor_units(matched_commission)?)
            .ok_or_else(|| anyhow::anyhow!("T-Bank reserved commission overflow"))?;
    }

    let reported_commission_minor = money_to_minor_units(reported)?;
    let remaining_commission_minor = reported_commission_minor
        .checked_sub(reserved_commission_minor)
        .ok_or_else(|| anyhow::anyhow!("T-Bank remaining commission overflow"))?;
    anyhow::ensure!(
        remaining_commission_minor == 0
            || (reported_commission_minor != 0
                && remaining_commission_minor.signum() == reported_commission_minor.signum()),
        "T-Bank operation commission is smaller than the already reported synthetic commission: operation={reported}, reserved_minor={reserved_commission_minor}"
    );
    let remaining_commission = Money::from_decimal(
        Decimal::from_i128_with_scale(
            remaining_commission_minor,
            u32::from(reported.currency.precision),
        ),
        reported.currency,
    )?;
    let weights = allocations
        .iter()
        .map(|(_, quantity)| *quantity)
        .collect::<Vec<_>>();
    let commission_allocations = allocate_money_by_weights(remaining_commission, &weights)?;
    let mut correction_events = Vec::with_capacity(synthetic_matches.len());
    let mut residual_commission = None;
    for ((target_trade_id, matched_quantity), allocated_commission) in
        allocations.iter().zip(commission_allocations)
    {
        if *target_trade_id == trade_id.as_str() {
            if let Some(tracked) = order.commission_by_trade_id.get_mut(*target_trade_id) {
                tracked.quantity = residual_quantity;
                tracked.resolved_quantity = residual_quantity;
                let residual_provenance =
                    commission_with_status(allocation_commission, allocated_commission);
                tracked.commission = residual_provenance;
                residual_commission = Some(residual_provenance);
            }
            continue;
        }
        let Some(tracked) = order.commission_by_trade_id.get_mut(*target_trade_id) else {
            continue;
        };
        let operation_correction = match tracked.commission {
            TbankFillCommission::Unknown => {
                tracked.resolved_quantity = *matched_quantity;
                Some(commission_with_status(
                    allocation_commission,
                    allocated_commission,
                ))
            }
            // The operations cursor already resolved only a share of this synthetic fill. The
            // new allocation covers a previously unknown share of the same execution, so it
            // accumulates onto the entry instead of replacing it.
            TbankFillCommission::Reported(_) | TbankFillCommission::Allocated(_)
                if tracked.resolved_quantity < tracked.quantity =>
            {
                anyhow::ensure!(
                    tracked.resolved_quantity + *matched_quantity <= tracked.quantity,
                    "synthetic T-Bank fill commission resolution exceeds tracked quantity: trade_id={target_trade_id}, resolved={}, matched={matched_quantity}, tracked={}",
                    tracked.resolved_quantity,
                    tracked.quantity,
                );
                tracked.resolved_quantity += *matched_quantity;
                Some(add_commission(
                    tracked.commission,
                    commission_with_status(allocation_commission, allocated_commission),
                )?)
            }
            // Cumulative order state already supplied the commission. OperationsService is a
            // later identity/fee source for this same execution, not an additional commission.
            TbankFillCommission::Reported(_) | TbankFillCommission::Allocated(_) => None,
        };
        if let Some(operation_correction) = operation_correction {
            tracked.commission = operation_correction;
            // A provenance event is keyed by the synthetic fill's trade ID and has no partial
            // coverage field. Publish only once Operations has resolved every lot of that fill;
            // otherwise consumers would mistake the accumulated partial amount for the full fee.
            if tracked.resolved_quantity == tracked.quantity {
                correction_events.push(((*target_trade_id).to_string(), tracked.commission));
            }
        }
    }
    if residual_quantity <= Decimal::ZERO {
        order.commission_by_trade_id.remove(trade_id.as_str());
    }
    recompute_commission_tracking(order);
    Ok((correction_events, residual_commission))
}

/// Allocates a money amount proportionally in currency minor units using largest remainder.
///
/// Every returned amount is representable at the currency precision and the sum is exactly the
/// input amount. This is deliberately shared by operation-level and correction-level commission
/// allocation so neither path can create a negative residual through independent rounding.
pub(super) fn allocate_money_by_weights(
    total: Money,
    weights: &[Decimal],
) -> anyhow::Result<Vec<Money>> {
    if weights.is_empty() {
        return Ok(Vec::new());
    }
    for weight in weights {
        anyhow::ensure!(
            *weight >= Decimal::ZERO,
            "T-Bank commission allocation weight must be non-negative: {weight}"
        );
    }

    let total_weight = weights.iter().copied().sum::<Decimal>();
    let total_minor = money_to_minor_units(total)?;
    let sign = total_minor.signum();
    let total_minor = total_minor
        .checked_abs()
        .ok_or_else(|| anyhow::anyhow!("T-Bank commission minor units overflow"))?;
    let total_minor_decimal = Decimal::from_i128_with_scale(total_minor, 0);
    let mut allocations = vec![0_i128; weights.len()];
    let mut remainders = vec![Decimal::ZERO; weights.len()];

    if total_weight > Decimal::ZERO {
        for (index, weight) in weights.iter().copied().enumerate() {
            let ideal = total_minor_decimal * weight / total_weight;
            let base = ideal
                .floor()
                .to_i128()
                .ok_or_else(|| anyhow::anyhow!("T-Bank commission allocation overflow"))?;
            allocations[index] = base;
            remainders[index] = ideal - Decimal::from_i128_with_scale(base, 0);
        }
    } else {
        let count = i128::try_from(weights.len())?;
        let base = total_minor / count;
        let remainder = usize::try_from(total_minor % count)?;
        allocations.fill(base);
        for index in 0..remainder {
            remainders[index] = Decimal::ONE;
        }
    }

    let allocated_minor = allocations.iter().copied().sum::<i128>();
    let remaining_minor = total_minor - allocated_minor;
    anyhow::ensure!(
        (0..=i128::try_from(weights.len())?).contains(&remaining_minor),
        "T-Bank commission allocation overflow: total={total_minor}, allocated={allocated_minor}"
    );
    let mut remainder_order = (0..weights.len()).collect::<Vec<_>>();
    remainder_order.sort_by(|left, right| {
        remainders[*right]
            .partial_cmp(&remainders[*left])
            .expect("Decimal values are finite")
            .then_with(|| left.cmp(right))
    });
    for index in remainder_order
        .into_iter()
        .take(usize::try_from(remaining_minor)?)
    {
        allocations[index] += 1;
    }

    allocations
        .into_iter()
        .map(|allocation| {
            let allocation = if sign < 0 { -allocation } else { allocation };
            let amount =
                Decimal::from_i128_with_scale(allocation, u32::from(total.currency.precision));
            Money::from_decimal(amount, total.currency).map_err(anyhow::Error::from)
        })
        .collect()
}

fn money_to_minor_units(money: Money) -> anyhow::Result<i128> {
    let scale = 10_i128
        .checked_pow(u32::from(money.currency.precision))
        .ok_or_else(|| anyhow::anyhow!("currency precision is too large for minor units"))?;
    let minor = money.as_decimal() * Decimal::from_i128_with_scale(scale, 0);
    let integer = minor.trunc();
    anyhow::ensure!(
        integer == minor,
        "money amount is not representable in currency minor units: {money}"
    );
    integer
        .to_i128()
        .ok_or_else(|| anyhow::anyhow!("money amount exceeds minor-unit range: {money}"))
}

fn commission_status_precedence(commission: TbankFillCommission) -> u8 {
    match commission {
        TbankFillCommission::Unknown => 0,
        TbankFillCommission::Allocated(_) => 1,
        TbankFillCommission::Reported(_) => 2,
    }
}

fn tracked_commission_precedence(tracked: TbankTrackedFillCommission) -> (u8, Decimal, Decimal) {
    (
        commission_status_precedence(tracked.commission),
        tracked.resolved_quantity,
        tracked.quantity,
    )
}

fn commission_with_status(template: TbankFillCommission, amount: Money) -> TbankFillCommission {
    match template {
        TbankFillCommission::Reported(_) => TbankFillCommission::Reported(amount),
        TbankFillCommission::Allocated(_) => TbankFillCommission::Allocated(amount),
        TbankFillCommission::Unknown => TbankFillCommission::Unknown,
    }
}

fn add_commission(
    current: TbankFillCommission,
    additional: TbankFillCommission,
) -> anyhow::Result<TbankFillCommission> {
    let Some(current_amount) = current.amount() else {
        return Ok(additional);
    };
    let Some(additional_amount) = additional.amount() else {
        return Ok(current);
    };
    anyhow::ensure!(
        current_amount.currency == additional_amount.currency,
        "T-Bank fill commission currencies differ: current={}, additional={}",
        current_amount.currency,
        additional_amount.currency,
    );
    let combined = Money::from_decimal(
        current_amount.as_decimal() + additional_amount.as_decimal(),
        current_amount.currency,
    )?;
    let status = if matches!(current, TbankFillCommission::Reported(_))
        && matches!(additional, TbankFillCommission::Reported(_))
    {
        TbankFillCommission::Reported(combined)
    } else {
        TbankFillCommission::Allocated(combined)
    };
    Ok(status)
}

fn recompute_commission_tracking(order: &mut TbankOrderFillProjection) {
    let mut unknown_quantity = Decimal::ZERO;
    let mut reported_total: Option<(Decimal, nautilus_model::types::Currency)> = None;
    for tracked in order.commission_by_trade_id.values() {
        match tracked.commission {
            TbankFillCommission::Unknown => unknown_quantity += tracked.quantity,
            TbankFillCommission::Reported(money) | TbankFillCommission::Allocated(money) => {
                // A partially operations-resolved entry covers only `resolved_quantity` lots;
                // the rest of the fill still has unknown commission provenance.
                unknown_quantity += tracked.quantity - tracked.resolved_quantity;
                if let Some((total, currency)) = reported_total.as_mut() {
                    if *currency == money.currency {
                        *total += money.as_decimal();
                    } else {
                        reported_total = None;
                        break;
                    }
                } else {
                    reported_total = Some((money.as_decimal(), money.currency));
                }
            }
        }
    }
    order.unknown_commission_quantity = unknown_quantity;
    order.emitted_commission = reported_total.map(|(total, currency)| {
        Money::from_decimal(total, currency).expect("sum of valid T-Bank commissions is valid")
    });
}

fn scale_commission(
    commission: Money,
    numerator: Decimal,
    denominator: Decimal,
) -> anyhow::Result<Money> {
    if commission.as_decimal() == Decimal::ZERO || numerator == denominator {
        return Ok(commission);
    }
    if numerator <= Decimal::ZERO || denominator <= Decimal::ZERO {
        return Money::from_decimal(Decimal::ZERO, commission.currency)
            .map_err(anyhow::Error::from);
    }
    Money::from_decimal(
        commission.as_decimal() * numerator / denominator,
        commission.currency,
    )
    .map_err(anyhow::Error::from)
}

pub(super) fn position_projection_key(
    account_id: AccountId,
    instrument_id: InstrumentId,
) -> String {
    format!("{account_id}:{instrument_id}")
}

pub(super) fn record_position_projection(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    report: &PositionStatusReport,
) -> bool {
    record_position_projection_from_source(
        projection,
        report,
        TbankPositionProjectionSource::SecuritiesSnapshot,
    )
}

pub(super) fn record_position_projection_from_source(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    report: &PositionStatusReport,
    source: TbankPositionProjectionSource,
) -> bool {
    let key = position_projection_key(report.account_id, report.instrument_id);
    let mut projection = projection.lock().expect("position_projection lock");
    if !position_projection_accepts(projection.get(key.as_str()), report, source) {
        return false;
    }
    let position = projected_position_from_report(projection.get(key.as_str()), report, source);
    projection.insert(key, position);
    true
}

#[cfg(test)]
pub(super) fn record_portfolio_position_projection(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    report: &PositionStatusReport,
) -> bool {
    record_position_projection_from_source(
        projection,
        report,
        TbankPositionProjectionSource::PortfolioStream,
    )
}

pub(super) fn reconcile_position_source_snapshot(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    account_id: AccountId,
    reports: &mut Vec<PositionStatusReport>,
    ts_init: UnixNanos,
    source: TbankPositionProjectionSource,
) {
    let current_keys = reports
        .iter()
        .map(|report| position_projection_key(report.account_id, report.instrument_id))
        .collect::<HashSet<_>>();
    let mut projection = projection.lock().expect("position_projection lock");
    let missing = projection
        .iter()
        .filter(|(key, position)| {
            position.account_id == account_id
                && position.source == source
                && !position.is_flat
                && position.ts_last <= ts_init
                && !current_keys.contains(key.as_str())
        })
        .map(|(key, position)| (key.clone(), position.clone()))
        .collect::<Vec<_>>();
    reports.retain(|report| {
        let key = position_projection_key(report.account_id, report.instrument_id);
        if !position_projection_accepts(projection.get(key.as_str()), report, source) {
            return false;
        }
        let position = projected_position_from_report(projection.get(key.as_str()), report, source);
        projection.insert(key, position);
        true
    });
    for (key, mut position) in missing {
        position.source = source;
        position.is_flat = true;
        position.ts_last = ts_init;
        position.advance_source_watermark(source, ts_init);
        projection.insert(key, position.clone());
        reports.push(PositionStatusReport::new(
            position.account_id,
            position.instrument_id,
            PositionSide::Flat,
            Quantity::from(0),
            ts_init,
            ts_init,
            Some(UUID4::new()),
            // T-Bank uses NETTING semantics; never propagate a venue position
            // ID from a projection into a Nautilus position status report.
            None,
            None,
        ));
    }
    for (key, position) in projection.iter_mut() {
        if position.account_id == account_id {
            position.advance_source_watermark(source, ts_init);
            if position.source == source
                && position.is_flat
                && !current_keys.contains(key.as_str())
                && position.ts_last < ts_init
            {
                position.ts_last = ts_init;
            }
        }
    }
}

#[cfg(test)]
pub(super) fn reconcile_portfolio_snapshot(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    account_id: AccountId,
    reports: &mut Vec<PositionStatusReport>,
    ts_init: UnixNanos,
) {
    reconcile_position_source_snapshot(
        projection,
        account_id,
        reports,
        ts_init,
        TbankPositionProjectionSource::PortfolioStream,
    );
}

#[cfg(test)]
pub(super) fn reconcile_position_snapshot(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    account_id: AccountId,
    reports: &mut Vec<PositionStatusReport>,
    ts_init: UnixNanos,
) {
    reconcile_position_source_snapshot(
        projection,
        account_id,
        reports,
        ts_init,
        TbankPositionProjectionSource::SecuritiesSnapshot,
    );
}

pub(super) fn apply_position_snapshot(
    projection: &Arc<Mutex<HashMap<String, TbankProjectedPosition>>>,
    account_id: AccountId,
    reports: &mut Vec<PositionStatusReport>,
    ts_init: UnixNanos,
    source: TbankPositionProjectionSource,
    is_complete: bool,
) {
    if is_complete {
        reconcile_position_source_snapshot(projection, account_id, reports, ts_init, source);
    } else {
        reports.retain(|report| record_position_projection_from_source(projection, report, source));
        tracing::warn!(
            ?source,
            "T-Bank position snapshot is incomplete; preserving positions absent from the partial snapshot"
        );
    }
}
