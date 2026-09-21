//! T-Bank execution integration.

mod broker_order_index;
/// Nautilus execution client.
pub mod client;
/// Typed execution events published through the Nautilus custom-data pipeline.
pub mod events;
/// Broker order request models and builders.
pub mod orders;
mod projections;
/// Stop-order request builders.
pub mod stop_orders;

pub use client::{
    TBANK_CONFIRM_MARGIN_TRADE_PARAM, TBANK_TOTAL_VAR_MARGIN_INFO_KEY,
    TBANK_TOTAL_VAR_MARGIN_SETTLED_INFO_KEY, TbankExecutionClient, TbankPendingSubmitStage,
    TbankSubmitResponse, TbankUnresolvedSubmit, tbank_account_id,
    tbank_broker_request_id_for_client_order_id,
};
pub use events::{
    TbankExecutionEvent, TbankFillCommission, TbankFillCommissionSource, TbankFillCommissionStatus,
    register_tbank_execution_custom_data,
};
pub use orders::{
    TbankExecutionService, TbankSubmitOrder, TbankTrailingStopParams, build_post_order_request,
};
pub use stop_orders::build_post_stop_order_request;
