//! T-Bank execution integration.

mod broker_order_index;
/// Nautilus execution client.
pub mod client;
/// Broker order request models and builders.
pub mod orders;
mod projections;
/// Stop-order request builders.
pub mod stop_orders;

pub use client::{
    TBANK_CONFIRM_MARGIN_TRADE_PARAM, TbankExecutionClient, TbankSubmitResponse, tbank_account_id,
    tbank_broker_request_id_for_client_order_id,
};
pub use orders::{
    TbankExecutionService, TbankSubmitOrder, TbankTrailingStopParams, build_post_order_request,
};
pub use stop_orders::build_post_stop_order_request;
