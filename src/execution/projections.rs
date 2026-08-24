use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::{OrderStatus, PositionSideSpecified},
    identifiers::{AccountId, InstrumentId},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

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
}

pub(super) fn merge_fill_projection_alias(
    projection: &mut TbankFillProjection,
    alias_order_id: &str,
    canonical_order_id: &str,
) {
    if alias_order_id.is_empty() || alias_order_id == canonical_order_id {
        return;
    }
    let Some(alias) = projection.orders.remove(alias_order_id) else {
        return;
    };
    let canonical = projection
        .orders
        .entry(canonical_order_id.to_string())
        .or_default();
    canonical.cumulative_filled_quantity = canonical
        .cumulative_filled_quantity
        .max(alias.cumulative_filled_quantity);
    canonical.emitted_fill_quantity = canonical
        .emitted_fill_quantity
        .max(alias.emitted_fill_quantity);
    canonical.emitted_fill_notional = canonical
        .emitted_fill_notional
        .max(alias.emitted_fill_notional);
    canonical.unmatched_emitted_quantity = canonical
        .unmatched_emitted_quantity
        .max(alias.unmatched_emitted_quantity);
    if let Some(alias_commission) = alias.emitted_commission {
        let replace = canonical
            .emitted_commission
            .is_none_or(|canonical_commission| {
                canonical_commission.currency == alias_commission.currency
                    && canonical_commission.as_decimal() < alias_commission.as_decimal()
            });
        if replace {
            canonical.emitted_commission = Some(alias_commission);
        }
    }
    canonical.seen_trade_ids.extend(alias.seen_trade_ids);
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
        enums::PositionSideSpecified, identifiers::InstrumentId, reports::PositionStatusReport,
        types::Quantity,
    };
    use rust_decimal::Decimal;

    fn current_unix_nanos() -> UnixNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after Unix epoch")
            .as_nanos();
        UnixNanos::from(u64::try_from(nanos).expect("current timestamp must fit u64"))
    }

    fn position_report(
        account_id: nautilus_model::identifiers::AccountId,
        instrument_id: InstrumentId,
        position_side: PositionSideSpecified,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
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
        assert_eq!(empty_snapshot[0].position_side, PositionSideSpecified::Flat);
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
            Quantity::from(10),
            current_unix_nanos(),
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let flat = position_report(
            account_id,
            instrument_id,
            PositionSideSpecified::Flat,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Long,
            Quantity::from(10),
            active_ts,
            "SBER-POSITION",
        );
        super::record_position_projection(&projection, &active);
        let mut reports = vec![position_report(
            account_id,
            instrument_id,
            PositionSideSpecified::Flat,
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
            PositionSideSpecified::Long,
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
            PositionSideSpecified::Flat
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
        is_flat: report.position_side == PositionSideSpecified::Flat
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

#[derive(Debug, Clone, Copy)]
pub(super) struct TbankProjectedFill {
    pub(super) quantity: Quantity,
    pub(super) price: Price,
    pub(super) commission: Money,
}

pub(super) fn project_cumulative_order_fill(
    projection: &Arc<Mutex<TbankFillProjection>>,
    order_id: &str,
    cumulative_quantity: Decimal,
    cumulative_notional: Decimal,
    cumulative_commission: Option<Money>,
) -> anyhow::Result<Option<TbankProjectedFill>> {
    if cumulative_quantity <= Decimal::ZERO {
        return Ok(None);
    }
    let mut projection = projection.lock().expect("fill_projection lock");
    let mut order = projection.orders.get(order_id).cloned().unwrap_or_default();
    if cumulative_quantity > order.cumulative_filled_quantity {
        order.cumulative_filled_quantity = cumulative_quantity;
    }
    let quantity_decimal = cumulative_quantity - order.emitted_fill_quantity;
    if quantity_decimal <= Decimal::ZERO {
        projection.orders.insert(order_id.to_string(), order);
        return Ok(None);
    }
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
    projection.orders.insert(order_id.to_string(), order);
    Ok(Some(TbankProjectedFill {
        quantity,
        price,
        commission,
    }))
}

fn project_cumulative_commission(
    order: &mut TbankOrderFillProjection,
    cumulative_commission: Option<Money>,
) -> anyhow::Result<Money> {
    let Some(cumulative_commission) = cumulative_commission else {
        return Money::from_decimal(Decimal::ZERO, Currency::from("RUB"))
            .map_err(anyhow::Error::from);
    };
    let emitted_commission = order
        .emitted_commission
        .filter(|emitted| emitted.currency == cumulative_commission.currency)
        .map(|emitted| emitted.as_decimal())
        .unwrap_or(Decimal::ZERO);
    let incremental_commission =
        (cumulative_commission.as_decimal() - emitted_commission).max(Decimal::ZERO);
    order.emitted_commission = Some(cumulative_commission);
    Money::from_decimal(incremental_commission, cumulative_commission.currency)
        .map_err(anyhow::Error::from)
}

#[cfg(test)]
pub(super) fn project_trade_fill_report(
    projection: &Arc<Mutex<TbankFillProjection>>,
    report: FillReport,
) -> anyhow::Result<Option<FillReport>> {
    let mut projection = projection.lock().expect("fill_projection lock");
    project_trade_fill_report_locked(&mut projection, report)
}

pub(super) fn project_trade_fill_report_locked(
    projection: &mut TbankFillProjection,
    mut report: FillReport,
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
    if !order.seen_trade_ids.insert(trade_id) {
        return Ok(None);
    }
    let mut emit_quantity = source_quantity;
    if order.unmatched_emitted_quantity > Decimal::ZERO {
        let consumed = order.unmatched_emitted_quantity.min(emit_quantity);
        order.unmatched_emitted_quantity -= consumed;
        emit_quantity -= consumed;
    }
    if emit_quantity <= Decimal::ZERO {
        projection.orders.insert(order_id, order);
        return Ok(None);
    }
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
            PositionSideSpecified::Flat,
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
