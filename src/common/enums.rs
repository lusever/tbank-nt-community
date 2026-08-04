use crate::grpc::generated::{OrderDirection, OrderType, StopOrderDirection, StopOrderType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Side of an order in T-Bank terms.
pub enum TbankOrderSide {
    /// Buy side.
    Buy,
    /// Sell side.
    Sell,
}

impl TbankOrderSide {
    /// Converts the side to a regular-order direction.
    pub const fn to_order_direction(self) -> OrderDirection {
        match self {
            Self::Buy => OrderDirection::Buy,
            Self::Sell => OrderDirection::Sell,
        }
    }

    /// Converts the side to a stop-order direction.
    pub const fn to_stop_order_direction(self) -> StopOrderDirection {
        match self {
            Self::Buy => StopOrderDirection::Buy,
            Self::Sell => StopOrderDirection::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Order types supported by the adapter.
pub enum TbankOrderType {
    /// Market order.
    Market,
    /// Limit order.
    Limit,
    /// Stop-market order.
    StopMarket,
    /// Take-profit market order.
    TakeProfitMarket,
    /// Trailing stop-market order.
    TrailingStopMarket,
    /// Trailing stop-limit order.
    TrailingStopLimit,
}

impl TbankOrderType {
    /// Converts the adapter order type to a regular-order type when supported.
    pub const fn to_order_type(self) -> Option<OrderType> {
        match self {
            Self::Market => Some(OrderType::Market),
            Self::Limit => Some(OrderType::Limit),
            Self::StopMarket
            | Self::TakeProfitMarket
            | Self::TrailingStopMarket
            | Self::TrailingStopLimit => None,
        }
    }

    /// Converts the adapter order type to a stop-order type when supported.
    pub const fn to_stop_order_type(self) -> Option<StopOrderType> {
        match self {
            Self::StopMarket => Some(StopOrderType::StopLoss),
            Self::TakeProfitMarket | Self::TrailingStopMarket | Self::TrailingStopLimit => {
                Some(StopOrderType::TakeProfit)
            }
            Self::Market | Self::Limit => None,
        }
    }
}
