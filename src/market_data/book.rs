use std::collections::BTreeMap;

use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Side of a T-Bank order book.
pub enum TbankBookSide {
    /// Bid side.
    Bid,
    /// Ask side.
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One price level in a T-Bank order book.
pub struct TbankOrderBookLevel {
    /// Level price.
    pub price: Decimal,
    /// Aggregate quantity in lots.
    pub quantity_lots: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Normalized full order-book snapshot.
pub struct TbankOrderBookSnapshot {
    /// Broker instrument UID.
    pub instrument_uid: String,
    /// Requested book depth.
    pub depth: i32,
    /// Whether the broker marked the snapshot as consistent.
    pub is_consistent: bool,
    /// Bid levels.
    pub bids: Vec<TbankOrderBookLevel>,
    /// Ask levels.
    pub asks: Vec<TbankOrderBookLevel>,
    /// Event timestamp in Unix nanoseconds.
    pub ts_event: i128,
    /// Initialization timestamp in Unix nanoseconds.
    pub ts_init: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Synthetic change between two full order-book snapshots.
pub struct SyntheticBookDelta {
    /// Changed book side.
    pub side: TbankBookSide,
    /// Changed price level.
    pub price: Decimal,
    /// Previous aggregate quantity in lots.
    pub old_quantity_lots: i64,
    /// New aggregate quantity in lots.
    pub new_quantity_lots: i64,
    /// Indicates that the delta was derived rather than broker-supplied.
    pub synthetic: bool,
}

/// Computes synthetic order-book deltas between two snapshots.
pub fn synthetic_deltas(
    previous: Option<&TbankOrderBookSnapshot>,
    next: &TbankOrderBookSnapshot,
) -> Vec<SyntheticBookDelta> {
    let Some(previous) = previous else {
        return Vec::new();
    };

    let mut deltas = Vec::new();
    diff_side(TbankBookSide::Bid, &previous.bids, &next.bids, &mut deltas);
    diff_side(TbankBookSide::Ask, &previous.asks, &next.asks, &mut deltas);
    deltas
}

fn diff_side(
    side: TbankBookSide,
    previous: &[TbankOrderBookLevel],
    next: &[TbankOrderBookLevel],
    deltas: &mut Vec<SyntheticBookDelta>,
) {
    let previous = previous
        .iter()
        .map(|level| (level.price, level.quantity_lots))
        .collect::<BTreeMap<_, _>>();
    let next = next
        .iter()
        .map(|level| (level.price, level.quantity_lots))
        .collect::<BTreeMap<_, _>>();

    for (price, old_quantity) in &previous {
        let new_quantity = next.get(price).copied().unwrap_or_default();
        if *old_quantity != new_quantity {
            deltas.push(SyntheticBookDelta {
                side,
                price: *price,
                old_quantity_lots: *old_quantity,
                new_quantity_lots: new_quantity,
                synthetic: true,
            });
        }
    }

    for (price, new_quantity) in &next {
        if !previous.contains_key(price) {
            deltas.push(SyntheticBookDelta {
                side,
                price: *price,
                old_quantity_lots: 0,
                new_quantity_lots: *new_quantity,
                synthetic: true,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn first_snapshot_after_reconnect_has_no_synthetic_deltas() {
        let snapshot = TbankOrderBookSnapshot {
            instrument_uid: "uid".to_string(),
            depth: 10,
            is_consistent: true,
            bids: vec![TbankOrderBookLevel {
                price: Decimal::new(100, 0),
                quantity_lots: 1,
            }],
            asks: vec![],
            ts_event: 1,
            ts_init: 1,
        };
        assert!(synthetic_deltas(None, &snapshot).is_empty());
    }

    #[test]
    fn snapshot_diff_is_marked_synthetic() {
        let previous = TbankOrderBookSnapshot {
            instrument_uid: "uid".to_string(),
            depth: 10,
            is_consistent: true,
            bids: vec![TbankOrderBookLevel {
                price: Decimal::new(100, 0),
                quantity_lots: 1,
            }],
            asks: vec![],
            ts_event: 1,
            ts_init: 1,
        };
        let next = TbankOrderBookSnapshot {
            bids: vec![TbankOrderBookLevel {
                price: Decimal::new(100, 0),
                quantity_lots: 2,
            }],
            ts_event: 2,
            ts_init: 2,
            ..previous.clone()
        };

        let deltas = synthetic_deltas(Some(&previous), &next);
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].synthetic);
        assert_eq!(deltas[0].new_quantity_lots, 2);
    }
}
