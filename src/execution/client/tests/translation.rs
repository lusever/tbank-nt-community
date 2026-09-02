// Direct unit tests for broker-to-Nautilus translation primitives.

// Note: this file is include!d into the shared `tests` module, so common
// imports (Decimal, OrderStatus, MoneyValue, StopOrder, Arc, ...) come from
// the module scope established by lifecycle.rs. Only unique imports are
// declared here.

use crate::grpc::generated::OrderStage;
use std::str::FromStr;

#[test]
fn stop_order_type_classification_handles_every_tbank_subtype() {
    let trailing_limit = StopOrder {
        take_profit_type: TakeProfitType::Trailing as i32,
        exchange_order_type: ExchangeOrderType::Limit as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        super::nautilus_stop_order_type(&trailing_limit),
        OrderType::TrailingStopLimit
    );

    let trailing_market = StopOrder {
        take_profit_type: TakeProfitType::Trailing as i32,
        exchange_order_type: ExchangeOrderType::Market as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        super::nautilus_stop_order_type(&trailing_market),
        OrderType::TrailingStopMarket
    );

    let stop_loss = StopOrder {
        order_type: StopOrderType::StopLoss as i32,
        ..StopOrder::default()
    };
    assert_eq!(super::nautilus_stop_order_type(&stop_loss), OrderType::StopMarket);

    let stop_limit = StopOrder {
        order_type: StopOrderType::StopLimit as i32,
        ..StopOrder::default()
    };
    assert_eq!(super::nautilus_stop_order_type(&stop_limit), OrderType::StopLimit);

    let take_profit_limit = StopOrder {
        order_type: StopOrderType::TakeProfit as i32,
        exchange_order_type: ExchangeOrderType::Limit as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        super::nautilus_stop_order_type(&take_profit_limit),
        OrderType::LimitIfTouched
    );

    let take_profit_market = StopOrder {
        order_type: StopOrderType::TakeProfit as i32,
        exchange_order_type: ExchangeOrderType::Market as i32,
        ..StopOrder::default()
    };
    assert_eq!(
        super::nautilus_stop_order_type(&take_profit_market),
        OrderType::MarketIfTouched
    );

    let unspecified = StopOrder::default();
    assert_eq!(super::nautilus_stop_order_type(&unspecified), OrderType::StopMarket);
}

#[test]
fn stream_stop_order_type_maps_each_adapter_type() {
    assert_eq!(super::nautilus_stream_stop_order_type(None), OrderType::StopMarket);
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::StopMarket)),
        OrderType::StopMarket
    );
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::MarketIfTouched)),
        OrderType::MarketIfTouched
    );
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::Market)),
        OrderType::Market
    );
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::Limit)),
        OrderType::Limit
    );
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::TrailingStopMarket)),
        OrderType::TrailingStopMarket
    );
    assert_eq!(
        super::nautilus_stream_stop_order_type(Some(TbankOrderType::TrailingStopLimit)),
        OrderType::TrailingStopLimit
    );
}

#[test]
fn stream_stop_order_type_prefers_context_report_type() {
    let context = super::TbankManagedOrderContext {
        side: None,
        order_type: Some(TbankOrderType::StopMarket),
        report_order_type: Some(OrderType::TrailingStopLimit),
        time_in_force: None,
        quantity_units: None,
        trailing: None,
    };
    assert_eq!(
        super::nautilus_stream_stop_order_type_from_context(&context),
        OrderType::TrailingStopLimit
    );

    let context = super::TbankManagedOrderContext {
        side: None,
        order_type: Some(TbankOrderType::TrailingStopLimit),
        report_order_type: None,
        time_in_force: None,
        quantity_units: None,
        trailing: None,
    };
    assert_eq!(
        super::nautilus_stream_stop_order_type_from_context(&context),
        OrderType::TrailingStopLimit
    );

    let context = super::TbankManagedOrderContext {
        side: None,
        order_type: None,
        report_order_type: None,
        time_in_force: None,
        quantity_units: None,
        trailing: None,
    };
    assert_eq!(
        super::nautilus_stream_stop_order_type_from_context(&context),
        OrderType::StopMarket
    );
}

#[test]
fn order_status_maps_terminal_and_partial_states() {
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            1,
            1
        ),
        OrderStatus::Filled
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusRejected as i32,
            1,
            0
        ),
        OrderStatus::Rejected
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusCancelled as i32,
            1,
            0
        ),
        OrderStatus::Canceled
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32,
            2,
            1
        ),
        OrderStatus::PartiallyFilled
    );
    // New with executed lots resolves to Filled or PartiallyFilled.
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            2,
            2
        ),
        OrderStatus::Filled
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            2,
            1
        ),
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            2,
            0
        ),
        OrderStatus::Accepted
    );
    assert_eq!(
        super::nautilus_order_status(
            OrderExecutionReportStatus::ExecutionReportStatusUnspecified as i32,
            2,
            0
        ),
        OrderStatus::Accepted
    );
}

#[test]
fn stream_order_status_cancels_when_partial_fill_is_completed() {
    let cancelled = order_state_stream_response::OrderState {
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 2,
        lots_executed: 1,
        completion_time: Some(prost_types::Timestamp::default()),
        lots_cancelled: 1,
        ..order_state_stream_response::OrderState::default()
    };
    assert_eq!(super::nautilus_stream_order_status(&cancelled), OrderStatus::Canceled);

    let consumed = order_state_stream_response::OrderState {
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 2,
        lots_executed: 1,
        completion_time: Some(prost_types::Timestamp::default()),
        lots_left: 0,
        ..order_state_stream_response::OrderState::default()
    };
    assert_eq!(super::nautilus_stream_order_status(&consumed), OrderStatus::Canceled);

    let ongoing = order_state_stream_response::OrderState {
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 2,
        lots_executed: 1,
        lots_cancelled: 1,
        ..order_state_stream_response::OrderState::default()
    };
    assert_eq!(
        super::nautilus_stream_order_status(&ongoing),
        OrderStatus::PartiallyFilled
    );
}

#[test]
fn time_in_force_mapping_forces_ioc_for_market_orders() {
    assert_eq!(
        super::nautilus_time_in_force(TbankTimeInForceType::TimeInForceUnspecified as i32),
        TimeInForce::Day
    );
    assert_eq!(
        super::nautilus_stream_time_in_force(0, 0, None),
        TimeInForce::Ioc,
        "unspecified order type falls back to market semantics"
    );
    assert_eq!(
        super::nautilus_stream_time_in_force(
            crate::grpc::generated::OrderType::Limit as i32,
            TbankTimeInForceType::TimeInForceUnspecified as i32,
            None
        ),
        TimeInForce::Day,
        "limit order keeps the native day TIF"
    );
    assert_eq!(
        super::nautilus_stream_time_in_force(
            crate::grpc::generated::OrderType::Market as i32,
            0,
            Some(TimeInForce::Gtc)
        ),
        TimeInForce::Gtc,
        "managed TIF wins over market IOC forcing"
    );
}

#[test]
fn stop_order_status_maps_each_broker_status() {
    assert_eq!(
        super::nautilus_stop_order_status(StopOrderStatusOption::StopOrderStatusExecuted as i32),
        OrderStatus::Triggered
    );
    assert_eq!(
        super::nautilus_stop_order_status(StopOrderStatusOption::StopOrderStatusCanceled as i32),
        OrderStatus::Canceled
    );
    assert_eq!(
        super::nautilus_stop_order_status(StopOrderStatusOption::StopOrderStatusExpired as i32),
        OrderStatus::Expired
    );
    assert_eq!(
        super::nautilus_stop_order_status(StopOrderStatusOption::StopOrderStatusActive as i32),
        OrderStatus::Accepted
    );
    assert_eq!(super::nautilus_stop_order_status(9999), OrderStatus::Accepted);
}

#[test]
fn position_side_classifies_sign_of_balance() {
    assert_eq!(super::position_side(Decimal::from(5)), PositionSideSpecified::Long);
    assert_eq!(super::position_side(Decimal::from(-5)), PositionSideSpecified::Short);
    assert_eq!(super::position_side(Decimal::ZERO), PositionSideSpecified::Flat);
}

#[test]
fn lots_to_quantity_multiplies_by_lot_size() {
    assert_eq!(
        super::lots_to_quantity(2, 10).unwrap().as_decimal(),
        Decimal::from(20)
    );
    assert!(super::lots_to_quantity(-1, 10).is_err());
}

#[test]
fn timestamp_conversion_rejects_negative_and_overflow() {
    assert_eq!(
        super::timestamp_to_unix_nanos(&prost_types::Timestamp {
            seconds: 1,
            nanos: 500_000_000
        })
        .unwrap()
        .as_u64(),
        1_500_000_000
    );
    assert!(super::timestamp_to_unix_nanos(&prost_types::Timestamp {
        seconds: -1,
        nanos: 0
    })
    .is_err());
    assert!(super::timestamp_to_unix_nanos(&prost_types::Timestamp {
        seconds: i64::MAX,
        nanos: 0
    })
    .is_err());
}

#[test]
fn account_id_prefixes_raw_ids_and_keeps_qualified_ids() {
    assert_eq!(
        super::nautilus_account_id("12345678").to_string(),
        "TBANK-12345678"
    );
    assert_eq!(
        super::nautilus_account_id("12345678-abcd").to_string(),
        "12345678-abcd"
    );
    assert_eq!(super::nautilus_account_id("").to_string(), "TBANK-UNKNOWN");
    assert_eq!(
        super::nautilus_account_id("  12345678  ").to_string(),
        "TBANK-12345678"
    );
}

#[test]
fn stop_trigger_type_defaults_only_for_stop_families() {
    let market = base_order_status_report(OrderType::Market);
    assert_eq!(
        super::with_default_stop_trigger_type(market).trigger_type,
        None,
        "market orders carry no default stop trigger"
    );

    for order_type in [
        OrderType::StopMarket,
        OrderType::StopLimit,
        OrderType::TrailingStopMarket,
        OrderType::TrailingStopLimit,
        OrderType::MarketIfTouched,
        OrderType::LimitIfTouched,
    ] {
        let report = super::with_default_stop_trigger_type(base_order_status_report(order_type));
        assert_eq!(
            report.trigger_type,
            Some(TriggerType::Default),
            "stop family {order_type} gets a default trigger"
        );
    }

    let explicit = base_order_status_report(OrderType::StopMarket)
        .with_trigger_type(TriggerType::LastPrice);
    assert_eq!(
        super::with_default_stop_trigger_type(explicit).trigger_type,
        Some(TriggerType::LastPrice)
    );
}

#[test]
fn activated_stop_child_status_keeps_triggered_identity() {
    assert_eq!(
        super::activated_stop_child_status(OrderStatus::Accepted),
        OrderStatus::Triggered
    );
    assert_eq!(
        super::activated_stop_child_status(OrderStatus::PartiallyFilled),
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        super::activated_stop_child_status(OrderStatus::Filled),
        OrderStatus::Filled
    );
}

#[test]
fn operation_side_maps_regular_card_and_margin_buys_and_sells() {
    for operation in [
        crate::grpc::generated::OperationType::Buy,
        crate::grpc::generated::OperationType::BuyCard,
        crate::grpc::generated::OperationType::BuyMargin,
    ] {
        assert_eq!(
            super::fill_side_from_operation_type(operation as i32),
            Some(OrderSide::Buy)
        );
    }
    for operation in [
        crate::grpc::generated::OperationType::Sell,
        crate::grpc::generated::OperationType::SellCard,
        crate::grpc::generated::OperationType::SellMargin,
    ] {
        assert_eq!(
            super::fill_side_from_operation_type(operation as i32),
            Some(OrderSide::Sell)
        );
    }
    assert_eq!(super::fill_side_from_operation_type(9999), None);
}

#[test]
fn futures_money_price_converts_to_points_when_metadata_requests_it() {
    let mut metadata = sber_metadata();
    metadata.price_in_points = true;
    metadata.min_price_increment = Decimal::ONE;
    metadata.min_price_increment_amount = Some(Decimal::new(125, 1));

    let price = super::price_from_money_value_for_instrument(
        &MoneyValue {
            currency: "rub".to_string(),
            units: 875,
            nano: 0,
        },
        Some(&metadata),
    )
    .unwrap();
    assert_eq!(price.as_decimal(), Decimal::from(70));

    let raw = super::price_from_money_value_for_instrument(
        &MoneyValue {
            currency: "rub".to_string(),
            units: 875,
            nano: 0,
        },
        None,
    )
    .unwrap();
    assert_eq!(raw.as_decimal(), Decimal::from(875));

    metadata.min_price_increment_amount = None;
    assert!(super::price_from_money_value_for_instrument(
        &MoneyValue {
            currency: "rub".to_string(),
            units: 875,
            nano: 0,
        },
        Some(&metadata),
    )
    .is_err());
}

#[test]
fn execution_average_price_allows_between_tick_futures_value() {
    let mut metadata = sber_metadata();
    metadata.price_in_points = true;
    metadata.min_price_increment = Decimal::ONE;
    metadata.min_price_increment_amount = Some(Decimal::new(125, 1));

    let price = super::execution_price_from_money_value_for_instrument(
        &MoneyValue {
            currency: "rub".to_string(),
            units: 875,
            nano: 600_000_000,
        },
        Some(&metadata),
    )
    .unwrap();
    assert_eq!(price.as_decimal(), Decimal::from_str("70.048").unwrap());
}

#[test]
fn quotation_price_is_required_for_point_valued_fills() {
    let value = Quotation {
        units: 70_000,
        nano: 0,
    };
    assert_eq!(
        super::quotation_price_from_points_required(Some(&value))
            .unwrap()
            .as_decimal(),
        Decimal::from(70_000)
    );
    assert!(super::quotation_price_from_points_required(None).is_err());
}

#[test]
fn average_price_from_executed_order_price_ignores_zero_lots() {
    assert_eq!(
        super::average_price_from_executed_order_price(
            &MoneyValue {
                currency: "rub".to_string(),
                units: 100,
                nano: 0,
            },
            0,
            None,
        )
        .unwrap(),
        None
    );
    assert_eq!(
        super::average_price_from_executed_order_price(
            &MoneyValue {
                currency: "rub".to_string(),
                units: 220,
                nano: 0,
            },
            2,
            None,
        )
        .unwrap(),
        Some(Decimal::from(110))
    );
}

#[test]
fn average_price_from_stages_requires_complete_consistent_stages() {
    let mk = |units: i64, quantity: i64| OrderStage {
        price: Some(MoneyValue {
            currency: "rub".to_string(),
            units,
            nano: 0,
        }),
        quantity,
        ..OrderStage::default()
    };

    let report_from = |stages: Vec<OrderStage>, lots_executed: i64| {
        super::order_status_report_from_state_with_metadata(
            "TBANK-001".into(),
            OrderState {
                order_id: "order-1".to_string(),
                execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill
                    as i32,
                lots_requested: lots_executed,
                lots_executed,
                stages,
                direction: OrderDirection::Buy as i32,
                order_type: crate::grpc::generated::OrderType::Market as i32,
                instrument_uid: "sber-uid".to_string(),
                ticker: "SBER".to_string(),
                class_code: "TQBR".to_string(),
                ..OrderState::default()
            },
            current_unix_nanos(),
            "SBER_TQBR.MOEX".parse().unwrap(),
            10,
            None,
        )
        .unwrap()
    };

    let complete = report_from(vec![mk(100, 1), mk(120, 2)], 3);
    assert_eq!(
        complete.avg_px.unwrap().round_dp(2),
        Decimal::from_str("113.33").unwrap(),
        "stage average is value-weighted"
    );

    assert_eq!(
        report_from(vec![mk(100, 1), mk(120, 2)], 2).avg_px,
        None,
        "stage quantities must sum to executed lots"
    );

    let missing_price = OrderStage {
        price: None,
        quantity: 1,
        ..OrderStage::default()
    };
    assert_eq!(report_from(vec![missing_price], 1).avg_px, None);

    let negative_quantity = OrderStage {
        price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100,
            nano: 0,
        }),
        quantity: -1,
        ..OrderStage::default()
    };
    assert_eq!(report_from(vec![negative_quantity], 1).avg_px, None);

    let zero_quantity = mk(100, 0);
    assert_eq!(
        report_from(vec![zero_quantity, mk(100, 1)], 1)
            .avg_px
            .unwrap(),
        Decimal::from(100),
        "zero-quantity stages are skipped"
    );
}

#[test]
fn instrument_id_resolution_prefers_ticker_class_then_uid_then_figi() {
    assert_eq!(
        super::instrument_id_from_ticker_class_or_identity("SBER", "TQBR", "", "").unwrap(),
        "SBER_TQBR.MOEX".parse::<InstrumentId>().unwrap()
    );
    assert_eq!(
        super::instrument_id_from_ticker_class_or_identity("", "", "sber-uid.MOEX", "").unwrap(),
        "sber-uid.MOEX".parse::<InstrumentId>().unwrap()
    );
    assert_eq!(
        super::instrument_id_from_ticker_class_or_identity("", "", "", "BBG.MOEX").unwrap(),
        "BBG.MOEX".parse::<InstrumentId>().unwrap()
    );
    assert!(super::instrument_id_from_ticker_class_or_identity("", "", "", "").is_err());
}

#[test]
fn nonempty_client_order_id_rejects_empty_values() {
    assert!(super::nonempty_client_order_id("").is_none());
    assert_eq!(
        super::nonempty_client_order_id("client-1").unwrap().to_string(),
        "client-1"
    );
}

#[test]
fn cached_instrument_metadata_matches_by_uid_figi_or_native_id() {
    let metadata = sber_metadata();
    let cache = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));

    assert_eq!(
        super::cached_instrument_metadata(Some(&cache), &metadata.instrument_uid, "", "").unwrap(),
        metadata
    );
    assert_eq!(
        super::cached_instrument_metadata(Some(&cache), "", &metadata.figi, "").unwrap(),
        metadata
    );
    assert_eq!(
        super::cached_instrument_metadata(Some(&cache), "", "", &metadata.instrument_id).unwrap(),
        metadata
    );
    assert!(super::cached_instrument_metadata(Some(&cache), "unknown-uid", "", "").is_none());
    assert!(super::cached_instrument_metadata(None, &metadata.instrument_uid, "", "").is_none());
}

fn base_order_status_report(order_type: OrderType) -> OrderStatusReport {
    let ts = current_unix_nanos();
    OrderStatusReport::new(
        "TBANK-001".into(),
        "SBER_TQBR.MOEX".parse().unwrap(),
        Some(ClientOrderId::from("client-1")),
        VenueOrderId::from("venue-1"),
        OrderSide::Buy,
        order_type,
        TimeInForce::Gtc,
        OrderStatus::Accepted,
        Quantity::from(10),
        Quantity::from(0),
        ts,
        ts,
        ts,
        Some(UUID4::new()),
    )
}
