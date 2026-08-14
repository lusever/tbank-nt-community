fn si_futures_metadata() -> crate::instruments::TbankInstrumentMetadata {
    let mut metadata = sber_metadata();
    metadata.instrument_id = "Si-9.26_SPBFUT.MOEX".to_string();
    metadata.ticker = "Si-9.26".to_string();
    metadata.class_code = "SPBFUT".to_string();
    metadata.instrument_uid = "si-future-uid".to_string();
    metadata.price_in_points = true;
    metadata.min_price_increment = Decimal::ONE;
    metadata.min_price_increment_amount = Some(Decimal::new(125, 1));
    metadata.lot = 1;
    metadata
}

#[test]
fn futures_post_order_price_is_converted_to_points() {
    let metadata = si_futures_metadata();
    let cmd = submit_order_cmd_for("Si-9.26_SPBFUT.MOEX", OrderType::Market, None);
    let response = PostOrderResponse {
        order_id: "future-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 2,
        lots_executed: 2,
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 875_006,
            nano: 250_000_000,
        }),
        ..PostOrderResponse::default()
    };

    let report = super::order_status_report_from_post_order_response(
        "TBANK-001".into(),
        &cmd,
        &response,
        &metadata,
        super::current_unix_nanos(),
    )
    .unwrap();

    assert_eq!(report.avg_px, Some(Decimal::new(700005, 1)));
}

#[test]
fn futures_order_state_price_is_normalized_from_cumulative_value() {
    let metadata = si_futures_metadata();
    let report = super::order_status_report_from_state_with_metadata(
        "TBANK-001".into(),
        OrderState {
            order_id: "future-order-1".to_string(),
            lots_requested: 2,
            lots_executed: 2,
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            executed_order_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 1_750_000,
                nano: 0,
            }),
            ..OrderState::default()
        },
        super::current_unix_nanos(),
        "Si-9.26_SPBFUT.MOEX".parse().unwrap(),
        metadata.lot,
        Some(&metadata),
    )
    .unwrap();

    assert_eq!(report.avg_px, Some(Decimal::from(70_000)));
}

#[test]
fn futures_portfolio_fallback_price_is_converted_to_points() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let position = PortfolioPosition {
        instrument_uid: metadata.instrument_uid.clone(),
        figi: metadata.figi.clone(),
        ticker: metadata.ticker.clone(),
        class_code: metadata.class_code.clone(),
        quantity: Some(Quotation { units: 2, nano: 0 }),
        average_position_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 875_000,
            nano: 0,
        }),
        ..PortfolioPosition::default()
    };

    let report = super::position_status_report_from_portfolio_with_instruments(
        "TBANK-001".into(),
        &position,
        super::current_unix_nanos(),
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(report.avg_px_open, Some(Decimal::from(70_000)));
}

#[test]
fn numeric_tbank_account_id_is_prefixed_for_nautilus_reports() {
    assert_eq!(
        super::nautilus_account_id("2289788994").to_string(),
        "TBANK-2289788994"
    );
    assert_eq!(
        super::nautilus_account_id("MOEX-account-1").to_string(),
        "MOEX-account-1"
    );
}

#[test]
fn portfolio_account_state_uses_currency_cash_as_free_balance() {
    let portfolio = PortfolioResponse {
        account_id: "2289788994".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 100_000,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 25_000,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    };

    let state = super::account_state_from_portfolio(&portfolio)
        .unwrap()
        .unwrap();
    let balance = &state.balances[0];

    assert_eq!(state.account_id.to_string(), "TBANK-2289788994");
    assert_eq!(balance.total.as_f64(), 100_000.0);
    assert_eq!(balance.free.as_f64(), 25_000.0);
    assert_eq!(balance.locked.as_f64(), 75_000.0);
}

#[test]
fn portfolio_account_state_supports_kazakhstani_tenge() {
    let portfolio = PortfolioResponse {
        account_id: "2289788994".to_string(),
        total_amount_portfolio: Some(MoneyValue {
            currency: "kzt".to_string(),
            units: 100_000,
            nano: 0,
        }),
        total_amount_currencies: Some(MoneyValue {
            currency: "kzt".to_string(),
            units: 25_000,
            nano: 0,
        }),
        ..PortfolioResponse::default()
    };

    let state = super::account_state_from_portfolio(&portfolio)
        .unwrap()
        .unwrap();
    let balance = &state.balances[0];

    assert_eq!(balance.currency.code.as_str(), "KZT");
    assert_eq!(balance.total.as_f64(), 100_000.0);
    assert_eq!(balance.free.as_f64(), 25_000.0);
}

#[test]
fn trades_stream_fill_uses_figi_when_uid_is_empty() {
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "cached-sber-uid".to_string();
    metadata.figi = "BBG004730N88".to_string();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));

    let instrument_id = super::instrument_id_from_ticker_class_or_cached_identity(
        "",
        "",
        "",
        "BBG004730N88",
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(instrument_id.to_string(), "SBER_TQBR.MOEX");
}

#[test]
fn cached_identity_does_not_fallback_after_authoritative_identity_miss() {
    let metadata = sber_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));

    let error = super::instrument_id_from_ticker_class_or_cached_identity(
        "SBER",
        "TQBR",
        "stale-broker-uid",
        "stale-figi",
        Some(&instruments),
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("refusing to infer venue"));
}

#[test]
fn cached_identity_uses_unique_ticker_class_only_without_authoritative_identity() {
    let metadata = sber_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));

    let instrument_id = super::instrument_id_from_ticker_class_or_cached_identity(
        "SBER",
        "TQBR",
        "",
        "",
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(instrument_id.to_string(), "SBER_TQBR.MOEX");
}

#[test]
fn order_state_stream_uses_instrument_uid_for_cached_venue_resolution() {
    let mut metadata = sber_metadata();
    metadata.instrument_id = "AAPL_SPBXM.SPBE".to_string();
    metadata.ticker = "AAPL".to_string();
    metadata.class_code = "SPBXM".to_string();
    metadata.instrument_uid = "aapl-spbe-uid".to_string();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata,
    )])));

    let report = super::stream_order_status_report_from_state_with_instruments(
        order_state_stream_response::OrderState {
            order_id: "exchange-order-1".to_string(),
            trade_order_id: "trade-order-1".to_string(),
            instrument_uid: "aapl-spbe-uid".to_string(),
            ticker: "AAPL".to_string(),
            class_code: "SPBXM".to_string(),
            lot_size: 1,
            ..order_state_stream_response::OrderState::default()
        },
        "exchange-order-1",
        super::current_unix_nanos(),
        None,
        None,
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(report.instrument_id.to_string(), "AAPL_SPBXM.SPBE");
}

#[test]
fn futures_order_state_stream_price_is_already_in_points() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let report = super::stream_order_status_report_from_state_with_instruments(
        order_state_stream_response::OrderState {
            order_id: "future-stream-order-1".to_string(),
            trade_order_id: "future-stream-trade-1".to_string(),
            instrument_uid: metadata.instrument_uid,
            ticker: metadata.ticker,
            class_code: metadata.class_code,
            lot_size: 1,
            order_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "future-stream-order-1",
        super::current_unix_nanos(),
        None,
        None,
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(
        report.price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
}

#[test]
fn order_state_stream_average_price_is_not_divided_by_lots() {
    let metadata = sber_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let report = super::stream_order_status_report_from_state_with_instruments(
        order_state_stream_response::OrderState {
            order_id: "stream-order-with-fill".to_string(),
            trade_order_id: "stream-trade-with-fill".to_string(),
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
            instrument_uid: metadata.instrument_uid,
            ticker: metadata.ticker,
            class_code: metadata.class_code,
            lot_size: 10,
            lots_requested: 2,
            lots_executed: 2,
            executed_order_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            ..order_state_stream_response::OrderState::default()
        },
        "stream-order-with-fill",
        super::current_unix_nanos(),
        None,
        None,
        Some(&instruments),
    )
    .unwrap();

    assert_eq!(report.avg_px, Some(Decimal::from(275)));
}

#[test]
fn stop_order_maps_to_order_status_report() {
    let ts_init = super::current_unix_nanos();
    let report = super::stop_order_status_report(
        "account-1".into(),
        StopOrder {
            stop_order_id: "stop-1".to_string(),
            lots_requested: 2,
            direction: StopOrderDirection::Sell as i32,
            order_type: StopOrderType::StopLoss as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        ts_init,
        10,
    )
    .unwrap();

    assert_eq!(report.instrument_id.to_string(), "SBER_TQBR.MOEX");
    assert_eq!(report.venue_order_id.to_string(), "stop-1");
    assert_eq!(report.order_side, OrderSide::Sell);
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.trigger_type, Some(TriggerType::Default));
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(
        report.trigger_price.unwrap().as_decimal(),
        Decimal::from(275)
    );
}

#[test]
fn managed_market_if_touched_type_survives_stop_query_translation() {
    let report = super::stop_order_status_report_with_context(
        "account-1".into(),
        StopOrder {
            stop_order_id: "mit-stop-1".to_string(),
            direction: StopOrderDirection::Buy as i32,
            order_type: StopOrderType::TakeProfit as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        super::current_unix_nanos(),
        10,
        None,
        Some(TbankOrderType::MarketIfTouched),
    )
    .unwrap();

    assert_eq!(report.order_type, OrderType::MarketIfTouched);
    assert_eq!(report.trigger_price.unwrap().as_decimal(), Decimal::from(275));
}

#[test]
fn limit_take_profit_stop_query_preserves_limit_if_touched_type() {
    let report = super::stop_order_status_report(
        "account-1".into(),
        StopOrder {
            stop_order_id: "limit-tp-stop-1".to_string(),
            direction: StopOrderDirection::Buy as i32,
            order_type: StopOrderType::TakeProfit as i32,
            exchange_order_type: ExchangeOrderType::Limit as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        super::current_unix_nanos(),
        10,
    )
    .unwrap();

    assert_eq!(report.order_type, OrderType::LimitIfTouched);
}

#[test]
fn futures_stop_query_report_preserves_point_prices() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let report = super::stop_order_status_report_with_context(
        "account-1".into(),
        StopOrder {
            stop_order_id: "future-stop-1".to_string(),
            direction: StopOrderDirection::Sell as i32,
            order_type: StopOrderType::StopLoss as i32,
            instrument_uid: metadata.instrument_uid.clone(),
            ticker: metadata.ticker.clone(),
            class_code: metadata.class_code.clone(),
            price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        super::current_unix_nanos(),
        1,
        Some(&instruments),
        None,
    )
    .unwrap();

    assert_eq!(
        report.price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
    assert_eq!(
        report.trigger_price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
}

#[test]
fn futures_stop_stream_report_preserves_point_prices() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let client_order_id = "524b1a03-efdd-4cd0-bd56-7cc6570c7156";
    let pending_submits = Arc::new(Mutex::new(HashMap::from([(
        client_order_id.to_string(),
        TbankPendingSubmit {
            instrument_id: metadata.instrument_id.clone(),
            submitted_ts: super::current_unix_nanos(),
            quantity_units: Decimal::ONE,
            side: TbankOrderSide::Sell,
            order_type: TbankOrderType::StopMarket,
            time_in_force: TimeInForce::Gtc,
            trailing: None,
            venue_order_id: Some("future-stop-stream-1".to_string()),
            last_reconciliation_ts: None,
            stage: TbankPendingSubmitStage::Submitted,
        },
    )])));
    let broker_order_index = Arc::new(Mutex::new(TbankBrokerOrderIndex::default()));
    broker_order_index.lock().unwrap().record_mapping(
        TbankBrokerOrderRoute::StopOrder,
        client_order_id,
        "future-stop-stream-1",
    );

    let report = super::stream_stop_order_status_report_from_state_with_instruments(
        order_state_stream_response::StopOrderState {
            stop_order_id: "future-stop-stream-1".to_string(),
            account_id: "account-1".to_string(),
            direction: OrderDirection::Sell as i32,
            price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            instrument_uid: metadata.instrument_uid,
            ticker: metadata.ticker,
            class_code: metadata.class_code,
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..order_state_stream_response::StopOrderState::default()
        },
        super::current_unix_nanos(),
        &pending_submits,
        &broker_order_index,
        Some(&instruments),
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        report.price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
    assert_eq!(
        report.trigger_price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
}

#[test]
fn take_profit_stop_query_falls_back_to_market_if_touched() {
    let report = super::stop_order_status_report(
        "account-1".into(),
        StopOrder {
            stop_order_id: "mit-stop-after-restart".to_string(),
            direction: StopOrderDirection::Buy as i32,
            order_type: StopOrderType::TakeProfit as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        super::current_unix_nanos(),
        10,
    )
    .unwrap();

    assert_eq!(report.order_type, OrderType::MarketIfTouched);
}

#[test]
fn trailing_stop_query_preserves_native_offsets() {
    let ts_init = super::current_unix_nanos();
    let report = super::stop_order_status_report(
        "account-1".into(),
        StopOrder {
            stop_order_id: "trailing-1".to_string(),
            lots_requested: 2,
            direction: StopOrderDirection::Sell as i32,
            order_type: StopOrderType::TakeProfit as i32,
            take_profit_type: TakeProfitType::Trailing as i32,
            exchange_order_type: ExchangeOrderType::Limit as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            trailing_data: Some(stop_order::TrailingData {
                indent: Some(Quotation {
                    units: 1,
                    nano: 250_000_000,
                }),
                indent_type: TrailingValueType::TrailingValueRelative as i32,
                spread: Some(Quotation {
                    units: 0,
                    nano: 500_000_000,
                }),
                spread_type: TrailingValueType::TrailingValueRelative as i32,
                ..stop_order::TrailingData::default()
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        ts_init,
        10,
    )
    .unwrap();

    assert_eq!(report.order_type, OrderType::TrailingStopLimit);
    assert_eq!(
        report.activation_price.unwrap().as_decimal(),
        Decimal::from(275)
    );
    assert!(report.trigger_price.is_none());
    assert_eq!(report.trailing_offset, Some(Decimal::from(125)));
    assert_eq!(report.limit_offset, Some(Decimal::from(50)));
    assert_eq!(report.trailing_offset_type, TrailingOffsetType::BasisPoints);
    assert_eq!(report.trigger_type, Some(TriggerType::LastPrice));
}

#[test]
fn futures_trailing_stop_report_preserves_point_activation_and_offsets() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let report = super::stop_order_status_report_with_context(
        "account-1".into(),
        StopOrder {
            stop_order_id: "future-trailing-1".to_string(),
            lots_requested: 1,
            direction: StopOrderDirection::Sell as i32,
            order_type: StopOrderType::TakeProfit as i32,
            take_profit_type: TakeProfitType::Trailing as i32,
            exchange_order_type: ExchangeOrderType::Limit as i32,
            instrument_uid: metadata.instrument_uid.clone(),
            ticker: metadata.ticker.clone(),
            class_code: metadata.class_code.clone(),
            stop_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 70_000,
                nano: 0,
            }),
            trailing_data: Some(stop_order::TrailingData {
                indent: Some(Quotation {
                    units: 10,
                    nano: 0,
                }),
                indent_type: TrailingValueType::TrailingValueAbsolute as i32,
                spread: Some(Quotation { units: 2, nano: 0 }),
                spread_type: TrailingValueType::TrailingValueAbsolute as i32,
                ..stop_order::TrailingData::default()
            }),
            status: StopOrderStatusOption::StopOrderStatusActive as i32,
            ..StopOrder::default()
        },
        super::current_unix_nanos(),
        1,
        Some(&instruments),
        None,
    )
    .unwrap();

    assert_eq!(
        report.activation_price.map(|price| price.as_decimal()),
        Some(Decimal::from(70_000))
    );
    assert_eq!(report.trailing_offset, Some(Decimal::from(10)));
    assert_eq!(report.limit_offset, Some(Decimal::from(2)));
}

#[test]
fn futures_cursor_operation_trade_price_is_already_in_points() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let reports = super::fill_reports_from_cursor_operation_with_instruments(
        "TBANK-001".into(),
        &OperationItem {
            id: "future-operation-1".to_string(),
            r#type: TbankOperationType::Buy as i32,
            instrument_uid: metadata.instrument_uid.clone(),
            ticker: metadata.ticker.clone(),
            class_code: metadata.class_code.clone(),
            trades_info: Some(OperationItemTrades {
                trades: vec![OperationItemTrade {
                    num: "future-trade-1".to_string(),
                    quantity: 1,
                    price: Some(MoneyValue {
                        currency: "rub".to_string(),
                        units: 70_000,
                        nano: 0,
                    }),
                    ..OperationItemTrade::default()
                }],
            }),
            ..OperationItem::default()
        },
        super::current_unix_nanos(),
        Some(&instruments),
    );

    let report = reports.into_iter().next().unwrap().unwrap();
    assert_eq!(report.last_px.as_decimal(), Decimal::from(70_000));
}

#[test]
fn futures_order_trade_price_is_already_in_points() {
    let metadata = si_futures_metadata();
    let instruments = Arc::new(Mutex::new(HashMap::from([(
        metadata.instrument_id.clone(),
        metadata.clone(),
    )])));
    let trade = OrderTrade {
        price: Some(Quotation {
            units: 70_000,
            nano: 0,
        }),
        quantity: 1,
        trade_id: "future-trade-1".to_string(),
        ..OrderTrade::default()
    };

    let report = super::fill_report_from_order_trade(
        &OrderTrades {
            order_id: "future-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: metadata.instrument_uid,
            trades: vec![trade.clone()],
            ..OrderTrades::default()
        },
        &trade,
        super::current_unix_nanos(),
        &instruments,
    )
    .unwrap();

    assert_eq!(report.last_px.as_decimal(), Decimal::from(70_000));
}

#[test]
fn trailing_stop_query_rejects_mixed_offset_units() {
    let stop = StopOrder {
        take_profit_type: TakeProfitType::Trailing as i32,
        exchange_order_type: ExchangeOrderType::Limit as i32,
        trailing_data: Some(stop_order::TrailingData {
            indent: Some(Quotation { units: 5, nano: 0 }),
            indent_type: TrailingValueType::TrailingValueAbsolute as i32,
            spread: Some(Quotation {
                units: 0,
                nano: 500_000_000,
            }),
            spread_type: TrailingValueType::TrailingValueRelative as i32,
            ..stop_order::TrailingData::default()
        }),
        ..StopOrder::default()
    };

    let error = super::trailing_params_from_stop(&stop).unwrap_err();
    assert!(error.to_string().contains("cannot be represented"));
}

#[test]
fn open_order_status_report_converts_lots_to_shares() {
    let ts_init = super::current_unix_nanos();
    let report = super::order_status_report_from_state(
        "account-1".into(),
        OrderState {
            order_id: "order-1".to_string(),
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            lots_requested: 2,
            lots_executed: 1,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            instrument_uid: "sber-uid".to_string(),
            ticker: "SBER".to_string(),
            class_code: "TQBR".to_string(),
            ..OrderState::default()
        },
        ts_init,
        "SBER_TQBR.MOEX".parse().unwrap(),
        10,
    )
    .unwrap();

    assert_eq!(report.instrument_id.to_string(), "SBER_TQBR.MOEX");
    assert_eq!(report.time_in_force, TimeInForce::Ioc);
    assert_eq!(report.quantity.as_decimal(), Decimal::from(20));
    assert_eq!(report.filled_qty.as_decimal(), Decimal::from(10));
}

#[test]
fn order_status_fill_projection_dedupes_late_stream_fill() {
    let ts_init = super::current_unix_nanos();
    let client = test_client(TbankExecutionClientConfig::default());
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    let report = super::order_status_report_from_state(
        "account-1".into(),
        OrderState {
            order_id: "exchange-order-1".to_string(),
            order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
                as i32,
            lots_requested: 2,
            lots_executed: 1,
            direction: OrderDirection::Buy as i32,
            order_type: crate::grpc::generated::OrderType::Market as i32,
            instrument_uid: "sber-uid".to_string(),
            executed_order_price: Some(MoneyValue {
                currency: "rub".to_string(),
                units: 275,
                nano: 0,
            }),
            ..OrderState::default()
        },
        ts_init,
        "SBER_TQBR.MOEX".parse().unwrap(),
        10,
    )
    .unwrap();

    let projected = client
        .runtime
        .project_order_status_fill_report(
            &report,
            "exchange-order-1",
            "SUBMIT-exchange-order-1-10",
            ts_init,
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(projected.last_qty.as_decimal(), Decimal::from(10));

    let stream_report = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            figi: "BBG004730N88".to_string(),
            trades: Vec::new(),
            ..OrderTrades::default()
        },
        &OrderTrade {
            price: Some(Quotation {
                units: 275,
                nano: 0,
            }),
            quantity: 10,
            trade_id: "trade-1".to_string(),
            ..OrderTrade::default()
        },
        ts_init,
        &client.runtime.instruments,
    )
    .unwrap();
    assert!(
        client
            .runtime
            .project_trade_fill_report(stream_report)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn open_order_status_report_uses_cached_uid_metadata() {
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.runtime.record_managed_order_context(
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        TbankManagedOrderContext {
            side: Some(TbankOrderSide::Buy),
            order_type: Some(TbankOrderType::Limit),
            report_order_type: None,
            time_in_force: Some(TimeInForce::Fok),
            quantity_units: Some(Decimal::from(10)),
            trailing: None,
        },
    );
    client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .get_or_allocate_request_mapping(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        )
        .unwrap();

    let report = client
        .runtime
        .order_status_report_from_state_with_lots(
            "account-1".into(),
            OrderState {
                order_id: "order-1".to_string(),
                order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
                execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew
                    as i32,
                lots_requested: 1,
                lots_executed: 0,
                direction: OrderDirection::Buy as i32,
                order_type: crate::grpc::generated::OrderType::Limit as i32,
                instrument_uid: "sber-uid".to_string(),
                ..OrderState::default()
            },
            super::current_unix_nanos(),
        )
        .await
        .unwrap();

    assert_eq!(report.instrument_id.to_string(), "SBER_TQBR.MOEX");
    assert_eq!(report.time_in_force, TimeInForce::Fok);
    assert_eq!(report.quantity.as_decimal(), Decimal::from(10));
}

#[tokio::test]
async fn unary_order_report_does_not_promote_unknown_broker_request_id() {
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);

    let report = client
        .runtime
        .order_status_report_from_state_with_lots(
            "account-1".into(),
            OrderState {
                order_id: "external-order-1".to_string(),
                order_request_id: "external-broker-uuid".to_string(),
                execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew
                    as i32,
                lots_requested: 1,
                direction: OrderDirection::Buy as i32,
                order_type: crate::grpc::generated::OrderType::Limit as i32,
                instrument_uid: "sber-uid".to_string(),
                ..OrderState::default()
            },
            current_unix_nanos(),
        )
        .await
        .unwrap();

    assert!(report.client_order_id.is_none());
    assert_eq!(report.venue_order_id.to_string(), "external-order-1");
    assert!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .identity_for(Some("external-broker-uuid"), None)
            .is_none()
    );
}

#[tokio::test]
async fn unary_order_report_keeps_canonical_stop_identity_for_child_alias() {
    let stop_orders_service = MockStopOrdersService::default();
    let mut stop = active_sber_stop_order("stop-order-1");
    stop.status = StopOrderStatusOption::StopOrderStatusExecuted as i32;
    stop.exchange_order_id = Some("exchange-child-1".to_string());
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![stop],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "client-stop-1",
        "stop-order-1",
    );
    client.runtime.record_activated_stop_child_mapping(
        "client-stop-1",
        "stop-order-1",
        "exchange-child-1",
    );
    client.connect_for_queries().await.unwrap();

    let report = client
        .runtime
        .order_status_report_from_state_with_lots(
            "account-1".into(),
            OrderState {
                order_id: "exchange-child-1".to_string(),
                order_request_id: "stop-order-1".to_string(),
                execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill
                    as i32,
                lots_requested: 1,
                lots_executed: 1,
                direction: OrderDirection::Sell as i32,
                order_type: crate::grpc::generated::OrderType::Market as i32,
                instrument_uid: "sber-uid".to_string(),
                ..OrderState::default()
            },
            current_unix_nanos(),
        )
        .await
        .unwrap();

    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("client-stop-1".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_type, OrderType::StopMarket);
    assert_eq!(report.time_in_force, TimeInForce::Gtc);
    assert_eq!(report.trigger_type, Some(TriggerType::Default));
    assert_eq!(
        client
            .runtime
            .known_broker_order_identity(Some(&ClientOrderId::from("client-stop-1")), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::RegularOrder,
            broker_order_id: "exchange-child-1".to_string(),
        })
    );
}

#[tonic::async_trait]
impl OrdersService for MockOrdersService {
    async fn post_order(
        &self,
        request: Request<PostOrderRequest>,
    ) -> std::result::Result<Response<PostOrderResponse>, Status> {
        let request = request.into_inner();
        self.calls.lock().unwrap().push(request.clone());
        if let Some((code, message)) = self.post_error.lock().unwrap().clone() {
            return Err(Status::new(code, message));
        }
        if let Some(response) = self.post_response.lock().unwrap().clone() {
            return Ok(Response::new(response));
        }
        Ok(Response::new(PostOrderResponse {
            order_id: "exchange-order-1".to_string(),
            execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
            lots_requested: request.quantity,
            lots_executed: 0,
            direction: OrderDirection::Buy as i32,
            order_type: request.order_type,
            instrument_uid: request.instrument_id,
            order_request_id: request.order_id,
            ..PostOrderResponse::default()
        }))
    }

    async fn post_order_async(
        &self,
        _request: Request<PostOrderAsyncRequest>,
    ) -> std::result::Result<Response<PostOrderAsyncResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> std::result::Result<Response<CancelOrderResponse>, Status> {
        self.cancel_calls.lock().unwrap().push(request.into_inner());
        if let Some((code, message)) = self.cancel_error.lock().unwrap().clone() {
            return Err(Status::new(code, message));
        }
        Ok(Response::new(CancelOrderResponse::default()))
    }

    async fn get_order_state(
        &self,
        request: Request<GetOrderStateRequest>,
    ) -> std::result::Result<Response<OrderState>, Status> {
        let request = request.into_inner();
        self.state_calls.lock().unwrap().push(request.clone());
        if let Some((code, message)) = self.state_error.lock().unwrap().clone() {
            return Err(Status::new(code, message));
        }
        Ok(Response::new(
            self.state_response
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| OrderState {
                    order_id: "exchange-order-1".to_string(),
                    order_request_id: request.order_id,
                    execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew
                        as i32,
                    lots_requested: 1,
                    lots_executed: 0,
                    direction: OrderDirection::Buy as i32,
                    order_type: crate::grpc::generated::OrderType::Market as i32,
                    instrument_uid: "sber-uid".to_string(),
                    ticker: "SBER".to_string(),
                    class_code: "TQBR".to_string(),
                    ..OrderState::default()
                }),
        ))
    }

    async fn get_orders(
        &self,
        _request: Request<GetOrdersRequest>,
    ) -> std::result::Result<Response<GetOrdersResponse>, Status> {
        self.get_orders_calls.fetch_add(1, Ordering::SeqCst);
        self.get_orders_response
            .lock()
            .unwrap()
            .clone()
            .map(Response::new)
            .ok_or_else(|| Status::unimplemented("not used"))
    }

    async fn replace_order(
        &self,
        _request: Request<ReplaceOrderRequest>,
    ) -> std::result::Result<Response<PostOrderResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_max_lots(
        &self,
        _request: Request<GetMaxLotsRequest>,
    ) -> std::result::Result<Response<GetMaxLotsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_order_price(
        &self,
        _request: Request<GetOrderPriceRequest>,
    ) -> std::result::Result<Response<GetOrderPriceResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

#[tonic::async_trait]
impl StopOrdersService for MockStopOrdersService {
    async fn post_stop_order(
        &self,
        request: Request<PostStopOrderRequest>,
    ) -> std::result::Result<Response<PostStopOrderResponse>, Status> {
        let request = request.into_inner();
        self.post_calls.lock().unwrap().push(request.clone());
        if let Some((code, message)) = self.post_error.lock().unwrap().take() {
            return Err(Status::new(code, message));
        }
        if let Some(response) = self.post_response.lock().unwrap().clone() {
            return Ok(Response::new(response));
        }
        Ok(Response::new(PostStopOrderResponse {
            stop_order_id: "stop-order-1".to_string(),
            order_request_id: request.order_id,
            ..PostStopOrderResponse::default()
        }))
    }

    async fn get_stop_orders(
        &self,
        request: Request<GetStopOrdersRequest>,
    ) -> std::result::Result<Response<GetStopOrdersResponse>, Status> {
        self.get_calls.lock().unwrap().push(request.into_inner());
        if let Some(response) = self.get_responses.lock().unwrap().pop_front() {
            return Ok(Response::new(response));
        }
        Ok(Response::new(
            self.get_response
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_default(),
        ))
    }

    async fn cancel_stop_order(
        &self,
        request: Request<CancelStopOrderRequest>,
    ) -> std::result::Result<Response<CancelStopOrderResponse>, Status> {
        self.cancel_calls.lock().unwrap().push(request.into_inner());
        Ok(Response::new(CancelStopOrderResponse::default()))
    }
}

#[tonic::async_trait]
impl OperationsService for MockOperationsService {
    async fn get_operations(
        &self,
        _request: Request<OperationsRequest>,
    ) -> std::result::Result<Response<OperationsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_portfolio(
        &self,
        _request: Request<PortfolioRequest>,
    ) -> std::result::Result<Response<PortfolioResponse>, Status> {
        self.portfolio_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.portfolio_response
            .lock()
            .unwrap()
            .clone()
            .map(Response::new)
            .ok_or_else(|| Status::unimplemented("not used"))
    }

    async fn get_positions(
        &self,
        _request: Request<PositionsRequest>,
    ) -> std::result::Result<Response<PositionsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_withdraw_limits(
        &self,
        _request: Request<WithdrawLimitsRequest>,
    ) -> std::result::Result<Response<WithdrawLimitsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_broker_report(
        &self,
        _request: Request<BrokerReportRequest>,
    ) -> std::result::Result<Response<BrokerReportResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_dividends_foreign_issuer(
        &self,
        _request: Request<GetDividendsForeignIssuerRequest>,
    ) -> std::result::Result<Response<GetDividendsForeignIssuerResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }

    async fn get_operations_by_cursor(
        &self,
        request: Request<GetOperationsByCursorRequest>,
    ) -> std::result::Result<Response<GetOperationsByCursorResponse>, Status> {
        self.calls.lock().unwrap().push(request.into_inner());
        let page = self
            .pages
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| Status::internal("unexpected page request"))?;
        Ok(Response::new(page))
    }
}

#[tokio::test]
async fn submit_market_live_calls_orders_service() {
    let service = MockOrdersService::default();
    let calls = Arc::clone(&service.calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect().await.unwrap();

    let client_order_id = "524b1a03-efdd-4cd0-bd56-7cc6570c7156";
    let broker_request_id = tbank_broker_request_id_for_client_order_id(client_order_id);
    let response = client
        .runtime
        .submit_order(
            &TbankSubmitOrder {
                instrument_id: "SBER_TQBR.MOEX".to_string(),
                client_order_id: client_order_id.to_string(),
                broker_request_id: broker_request_id.clone(),
                side: TbankOrderSide::Buy,
                order_type: TbankOrderType::Market,
                time_in_force: TimeInForce::Ioc,
                quantity_units: Decimal::from(20),
                limit_price: None,
                trigger_price: None,
                trailing: None,
                confirm_margin_trade: false,
            },
            &sber_metadata(),
        )
        .await
        .unwrap();

    assert!(matches!(response, TbankSubmitResponse::Order(_)));
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].account_id, "account-1");
    assert_eq!(calls[0].quantity, 2);
    assert_eq!(calls[0].order_id, broker_request_id);
}

#[tokio::test]
async fn repeated_nautilus_submit_reuses_deterministic_broker_request_id() {
    let service = MockOrdersService::default();
    let calls = Arc::clone(&service.calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.runtime.connect().await.unwrap();

    let cmd = submit_order_cmd(None);
    let broker_request_id =
        tbank_broker_request_id_for_client_order_id(cmd.client_order_id.as_str());
    submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();
    submit_nautilus_order_reports(&mut client.runtime, &cmd, current_unix_nanos())
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].order_id, calls[1].order_id);
    assert_eq!(calls[0].order_id, broker_request_id);
    assert_ne!(calls[0].order_id, cmd.client_order_id.to_string());
    assert_eq!(calls[0].order_id.len(), 36);
}

#[tokio::test]
async fn generate_order_status_report_queries_tbank_request_id_for_client_order_id() {
    let client_order_id = "524b1a03-efdd-4cd0-bd56-7cc6570c7156";
    let broker_request_id = tbank_broker_request_id_for_client_order_id(client_order_id);
    run_order_status_report_query_test(
        order_status_report_cmd(Some(client_order_id), None),
        OrderIdType::Request,
        broker_request_id.as_str(),
    )
    .await;
}

#[tokio::test]
async fn generate_order_status_report_prefers_exchange_id_when_venue_order_id_is_known() {
    run_order_status_report_query_test(
        order_status_report_cmd(
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
            Some("exchange-order-1"),
        ),
        OrderIdType::Exchange,
        "exchange-order-1",
    )
    .await;
}

#[tokio::test]
async fn generate_order_status_report_routes_known_stop_venue_order_id_to_stop_orders_service() {
    let orders_service = MockOrdersService::default();
    let state_calls = Arc::clone(&orders_service.state_calls);
    let stop_orders_service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&stop_orders_service.get_calls);
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    client
        .runtime
        .record_broker_order_id(TbankBrokerOrderRoute::StopOrder, "stop-order-1");
    client.runtime.connect().await.unwrap();

    let report =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_order_status_report(
            &client,
            &order_status_report_cmd(Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"), Some("stop-order-1")),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(state_calls.lock().unwrap().len(), 0);
    assert_reconciliation_stop_queries(&get_calls.lock().unwrap());
}

#[tokio::test]
async fn generate_order_status_report_recovers_stop_route_when_local_index_is_empty() {
    let orders_service = MockOrdersService::default();
    let state_calls = Arc::clone(&orders_service.state_calls);
    let stop_orders_service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&stop_orders_service.get_calls);
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    client.runtime.connect().await.unwrap();

    let report =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_order_status_report(
            &client,
            &order_status_report_cmd(
                Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
                Some("stop-order-1"),
            ),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(state_calls.lock().unwrap().len(), 0);
    assert_reconciliation_stop_queries(&get_calls.lock().unwrap());
}

#[tokio::test]
async fn generate_order_status_report_resolves_executed_stop_child_after_restart_by_exchange_id() {
    let orders_service = MockOrdersService::default();
    let state_calls = Arc::clone(&orders_service.state_calls);
    *orders_service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-child-1".to_string(),
        order_request_id: "stop-order-1".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 2,
        lots_executed: 2,
        direction: OrderDirection::Sell as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let stop_orders_service = MockStopOrdersService::default();
    let mut stop = active_sber_stop_order("stop-order-1");
    stop.status = StopOrderStatusOption::StopOrderStatusExecuted as i32;
    stop.exchange_order_id = Some("exchange-child-1".to_string());
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![stop],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let client_order_id = "524b1a03-efdd-4cd9-8c1d-a464e2c835a7";
    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    client.runtime.connect().await.unwrap();

    let report =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_order_status_report(
            &client,
            &order_status_report_cmd(Some(client_order_id), Some("exchange-child-1")),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(report.order_status, OrderStatus::Filled);
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some(client_order_id.to_string())
    );
    let state_calls = state_calls.lock().unwrap();
    assert_eq!(state_calls.len(), 1);
    assert_eq!(state_calls[0].order_id, "exchange-child-1");
    assert_eq!(
        state_calls[0].order_id_type,
        Some(OrderIdType::Exchange as i32)
    );
}

#[tokio::test]
async fn generate_order_status_report_routes_known_stop_request_id_to_stop_orders_service() {
    let orders_service = MockOrdersService::default();
    let state_calls = Arc::clone(&orders_service.state_calls);
    let stop_orders_service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&stop_orders_service.get_calls);
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
        "stop-order-1",
    );
    client.runtime.connect().await.unwrap();

    let report =
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_order_status_report(
            &client,
            &order_status_report_cmd(Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"), None),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(state_calls.lock().unwrap().len(), 0);
    assert_reconciliation_stop_queries(&get_calls.lock().unwrap());
}

#[tokio::test]
async fn query_order_routes_known_stop_order_to_stop_orders_service() {
    let orders_service = MockOrdersService::default();
    let state_calls = Arc::clone(&orders_service.state_calls);
    let stop_orders_service = MockStopOrdersService::default();
    let get_calls = Arc::clone(&stop_orders_service.get_calls);
    *stop_orders_service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(orders_service))
            .add_service(StopOrdersServiceServer::new(stop_orders_service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    client
        .runtime
        .record_broker_order_id(TbankBrokerOrderRoute::StopOrder, "stop-order-1");
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    client.runtime.emitter.set_sender(sender);
    client.connect_for_queries().await.unwrap();

    <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::query_order(
        &client,
        QueryOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("RS-SHOCK-LIVE"),
            InstrumentId::from("SBER_TQBR.MOEX"),
            ClientOrderId::from("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
            Some(VenueOrderId::from("stop-order-1")),
            UUID4::new(),
            current_unix_nanos(),
            None,
            None,
        ),
    )
    .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
        panic!("expected order status report");
    };
    assert_eq!(
        report.client_order_id.map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(report.venue_order_id.to_string(), "stop-order-1");
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(state_calls.lock().unwrap().len(), 0);
    assert_reconciliation_stop_queries(&get_calls.lock().unwrap());
}

#[tokio::test]
async fn reconcile_order_by_request_id_maps_remote_open_order_to_accepted_report() {
    let service = MockOrdersService::default();
    let state_calls = Arc::clone(&service.state_calls);
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusNew as i32,
        lots_requested: 2,
        lots_executed: 0,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Limit as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.connect_for_queries().await.unwrap();
    client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .get_or_allocate_request_mapping(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        )
        .unwrap();

    let reports = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(reports.order_report.order_status, OrderStatus::Accepted);
    assert_eq!(
        reports
            .order_report
            .client_order_id
            .map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(
        reports.order_report.venue_order_id.to_string(),
        "exchange-order-1"
    );
    assert!(reports.fill_reports.is_empty());
    assert!(client.runtime.stream_tasks.lock().unwrap().is_empty());
    let state_calls = state_calls.lock().unwrap();
    assert_eq!(state_calls.len(), 1);
    assert_eq!(
        state_calls[0].order_id,
        "524b1a03-efdd-4cd0-bd56-7cc6570c7156"
    );
    assert_eq!(
        state_calls[0].order_id_type,
        Some(OrderIdType::Request as i32)
    );
}

#[tokio::test]
async fn reconcile_order_by_request_id_maps_remote_fill_to_fill_report_once() {
    let service = MockOrdersService::default();
    *service.state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusFill as i32,
        lots_requested: 2,
        lots_executed: 2,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 275,
            nano: 0,
        }),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.connect_for_queries().await.unwrap();
    client
        .runtime
        .broker_order_index
        .lock()
        .unwrap()
        .get_or_allocate_request_mapping(
            "524b1a03-efdd-4cd0-bd56-7cc6570c7156",
            Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156"),
        )
        .unwrap();

    let reports = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(reports.order_report.order_status, OrderStatus::Filled);
    assert_eq!(
        reports.order_report.filled_qty.as_decimal(),
        Decimal::from(20)
    );
    assert_eq!(reports.fill_reports.len(), 1);
    assert_eq!(
        reports.fill_reports[0].venue_order_id.to_string(),
        "exchange-order-1"
    );
    assert_eq!(
        reports.fill_reports[0]
            .client_order_id
            .map(|id| id.to_string()),
        Some("524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string())
    );
    assert_eq!(
        reports.fill_reports[0].last_qty.as_decimal(),
        Decimal::from(20)
    );
    assert_eq!(
        reports.fill_reports[0].last_px.as_decimal(),
        Decimal::new(1375, 1)
    );
    assert!(client.runtime.stream_tasks.lock().unwrap().is_empty());

    let second = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();
    assert!(second.fill_reports.is_empty());

    let stream_report = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: vec![OrderTrade {
                price: Some(Quotation {
                    units: 275,
                    nano: 0,
                }),
                quantity: 20,
                trade_id: "trade-1".to_string(),
                ..OrderTrade::default()
            }],
            ..OrderTrades::default()
        },
        &OrderTrade {
            price: Some(Quotation {
                units: 275,
                nano: 0,
            }),
            quantity: 20,
            trade_id: "trade-1".to_string(),
            ..OrderTrade::default()
        },
        current_unix_nanos(),
        &client.runtime.instruments,
    )
    .unwrap();
    assert!(
        client
            .runtime
            .project_trade_fill_report(stream_report)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn reconcile_order_by_request_id_emits_only_partial_fill_delta() {
    let service = MockOrdersService::default();
    let state_response = Arc::clone(&service.state_response);
    *state_response.lock().unwrap() = Some(OrderState {
        order_id: "exchange-order-1".to_string(),
        order_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        execution_report_status: OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill
            as i32,
        lots_requested: 3,
        lots_executed: 1,
        direction: OrderDirection::Buy as i32,
        order_type: crate::grpc::generated::OrderType::Market as i32,
        instrument_uid: "sber-uid".to_string(),
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        executed_order_price: Some(MoneyValue {
            currency: "rub".to_string(),
            units: 275,
            nano: 0,
        }),
        ..OrderState::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    let mut metadata = sber_metadata();
    metadata.instrument_uid = "sber-uid".to_string();
    metadata.lot = 10;
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(metadata.instrument_id.clone(), metadata);
    client.connect_for_queries().await.unwrap();

    let first = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.fill_reports.len(), 1);
    assert_eq!(
        first.fill_reports[0].last_qty.as_decimal(),
        Decimal::from(10)
    );

    {
        let mut state = state_response.lock().unwrap();
        let state = state.as_mut().unwrap();
        state.lots_executed = 2;
        state.execution_report_status =
            OrderExecutionReportStatus::ExecutionReportStatusPartiallyfill as i32;
    }
    let second = client
        .runtime
        .reconcile_order_by_request_id("524b1a03-efdd-4cd0-bd56-7cc6570c7156", current_unix_nanos())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.fill_reports.len(), 1);
    assert_eq!(
        second.fill_reports[0].last_qty.as_decimal(),
        Decimal::from(10)
    );

    let stream_report = fill_report_from_order_trade(
        &OrderTrades {
            order_id: "exchange-order-1".to_string(),
            direction: OrderDirection::Buy as i32,
            account_id: "account-1".to_string(),
            instrument_uid: "sber-uid".to_string(),
            trades: Vec::new(),
            ..OrderTrades::default()
        },
        &OrderTrade {
            price: Some(Quotation {
                units: 275,
                nano: 0,
            }),
            quantity: 30,
            trade_id: "trade-1".to_string(),
            ..OrderTrade::default()
        },
        current_unix_nanos(),
        &client.runtime.instruments,
    )
    .unwrap();
    let residual = client
        .runtime
        .project_trade_fill_report(stream_report)
        .unwrap()
        .unwrap();
    assert_eq!(residual.last_qty.as_decimal(), Decimal::from(10));
}

#[tokio::test]
async fn cancel_stop_order_live_calls_stop_orders_service() {
    let service = MockStopOrdersService::default();
    let calls = Arc::clone(&service.cancel_calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StopOrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        enable_trading: true,
        allow_live_trading: true,
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect().await.unwrap();

    client
        .runtime
        .cancel_stop_order("stop-order-1")
        .await
        .unwrap();

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].account_id, "account-1");
    assert_eq!(calls[0].stop_order_id, "stop-order-1");
}

#[tokio::test]
async fn stop_order_cancel_uses_submitted_stop_mapping() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "stop-request-1",
        "stop-order-1",
    );

    assert_eq!(
        client
            .runtime
            .resolve_cancel_target("stop-request-1", None)
            .await
            .unwrap(),
        TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
    assert_eq!(
        client
            .runtime
            .resolve_cancel_target("stop-request-1", Some("stop-order-1"))
            .await
            .unwrap(),
        TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
}

#[tokio::test]
async fn pending_stop_order_cancel_waits_for_broker_stop_order_id() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .record_broker_order_route(TbankBrokerOrderRoute::StopOrder, "stop-request-1");

    assert_eq!(
        client
            .runtime
            .resolve_cancel_target("stop-request-1", None)
            .await
            .unwrap(),
        TbankCancelTarget::Pending {
            route: TbankBrokerOrderRoute::StopOrder,
            client_order_id: "stop-request-1".to_string(),
        }
    );
    assert!(client.runtime.record_broker_order_mapping(
        TbankBrokerOrderRoute::StopOrder,
        "stop-request-1",
        "stop-order-1",
    ));
    assert_eq!(
        client
            .runtime
            .known_broker_order_identity(Some(&ClientOrderId::from("stop-request-1")), None),
        Some(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
}

#[tokio::test]
async fn pending_cancel_uses_explicit_venue_order_id_with_pending_route() {
    let mut client = test_client(TbankExecutionClientConfig::default());
    client
        .runtime
        .record_broker_order_route(TbankBrokerOrderRoute::StopOrder, "stop-request-1");

    assert_eq!(
        client
            .runtime
            .resolve_cancel_target("stop-request-1", Some("stop-order-1"))
            .await
            .unwrap(),
        TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
}

#[tokio::test]
async fn cancel_without_broker_order_identity_fails_closed() {
    let mut client = test_client(TbankExecutionClientConfig::default());

    let error = client
        .runtime
        .resolve_cancel_target("client-only-order", None)
        .await
        .expect_err("client order ID must not be used as a broker order ID");

    assert!(matches!(
        error,
        TbankAdapterError::BrokerOrderIdentityUnresolved(_)
    ));
}

#[tokio::test]
async fn submit_route_is_visible_before_async_broker_submit() {
    let client = test_client(TbankExecutionClientConfig::default());
    activate_test_lifecycle(&client);
    let client_order_id = ClientOrderId::from("stop-request-before-spawn");
    let runtime = client.runtime.clone();
    let mut future_runtime = runtime.clone();
    let route_runtime = runtime.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    runtime
        .spawn_mutating_command_task_with(
            async move {
                assert_eq!(
                    future_runtime
                        .resolve_cancel_target(client_order_id.as_str(), None)
                        .await
                        .unwrap(),
                    TbankCancelTarget::Pending {
                        route: TbankBrokerOrderRoute::StopOrder,
                        client_order_id: client_order_id.to_string(),
                    }
                );
                ready_tx.send(()).unwrap();
            },
            move || {
                route_runtime.prepare_submit_route(&client_order_id, OrderType::StopMarket);
            },
        )
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), ready_rx)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn broker_request_id_allocation_is_atomic_stable_and_collision_safe() {
    let client = test_client(TbankExecutionClientConfig::default());
    let handles = (0..16)
        .map(|_| {
            let client = client.runtime.clone();
            std::thread::spawn(move || {
                client
                    .get_or_allocate_broker_request_id("strategy-order-1")
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let ids = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), 1);
    let id = ids.into_iter().next().unwrap();
    assert!(uuid::Uuid::parse_str(id.as_str()).is_ok());

    let restarted = test_client(TbankExecutionClientConfig::default());
    assert_eq!(
        restarted
            .runtime
            .get_or_allocate_broker_request_id("strategy-order-1")
            .unwrap(),
        id
    );
}

#[test]
fn known_regular_request_id_does_not_include_stop_or_external_ids() {
    let mut index = TbankBrokerOrderIndex::default();
    let broker_request_id = "524b1a03-efdd-4cd0-bd56-7cc6570c7156";
    index
        .get_or_allocate_request_mapping("strategy-order-1", Some(broker_request_id))
        .unwrap();
    index.record_client_order_route(TbankBrokerOrderRoute::RegularOrder, "strategy-order-1");
    index.record_venue_order_id(TbankBrokerOrderRoute::StopOrder, "stop-order-1");

    assert!(index.is_known_regular_order_request_id(broker_request_id));
    assert!(!index.is_known_regular_order_request_id("stop-order-1"));
    assert!(!index.is_known_regular_order_request_id("external-request-1"));
}

#[test]
fn custom_broker_request_id_is_rejected() {
    let client = test_client(TbankExecutionClientConfig::default());
    let client_order_id = "strategy-order-1";
    let deterministic = tbank_broker_request_id_for_client_order_id(client_order_id);
    let mut order = TbankSubmitOrder {
        instrument_id: "SBER_TQBR.MOEX".to_string(),
        client_order_id: client_order_id.to_string(),
        broker_request_id: "524b1a03-efdd-4cd0-bd56-7cc6570c7156".to_string(),
        side: TbankOrderSide::Buy,
        order_type: TbankOrderType::Market,
        time_in_force: TimeInForce::Ioc,
        quantity_units: Decimal::from(20),
        limit_price: None,
        trigger_price: None,
        trailing: None,
        confirm_margin_trade: false,
    };
    assert_ne!(order.broker_request_id, deterministic);

    let error = client
        .runtime
        .ensure_broker_request_mapping(&order)
        .unwrap_err();
    assert!(error.to_string().contains("custom broker request id"));

    assert_eq!(
        client
            .runtime
            .get_or_allocate_broker_request_id(client_order_id)
            .unwrap(),
        deterministic
    );

    order.broker_request_id = deterministic.clone();
    client
        .runtime
        .ensure_broker_request_mapping(&order)
        .unwrap();
}

#[tokio::test]
async fn stop_order_cancel_route_recovers_from_broker_after_restart() {
    let service = MockStopOrdersService::default();
    *service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("stop-order-1")],
    });
    let get_calls = Arc::clone(&service.get_calls);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StopOrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect().await.unwrap();

    let target = client
        .runtime
        .resolve_cancel_target("stop-order-1", Some("stop-order-1"))
        .await
        .unwrap();

    assert_eq!(
        target,
        TbankCancelTarget::Ready(TbankBrokerOrderIdentity {
            route: TbankBrokerOrderRoute::StopOrder,
            broker_order_id: "stop-order-1".to_string(),
        })
    );
    assert_eq!(get_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn activated_stop_recovery_uses_order_request_id_without_exchange_order_id() {
    let service = MockStopOrdersService::default();
    *service.get_response.lock().unwrap() = Some(GetStopOrdersResponse {
        stop_orders: vec![active_sber_stop_order("request-shaped-stop-id")],
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(StopOrdersServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect_for_queries().await.unwrap();

    let (stop, client_order_id) = client
        .runtime
        .resolve_activated_stop_mapping(
            "exchange-child-1",
            Some("request-shaped-stop-id"),
        )
        .await
        .unwrap()
        .expect("order_request_id should recover the stop parent");
    assert_eq!(stop.stop_order_id, "request-shaped-stop-id");
    assert!(client_order_id.is_none());
    assert_eq!(
        client
            .runtime
            .broker_order_index
            .lock()
            .unwrap()
            .known_stop_broker_order_ids(),
        vec!["request-shaped-stop-id".to_string()]
    );
}

#[tokio::test]
async fn query_fills_pages_until_cursor_exhausted() {
    let service = MockOperationsService::default();
    let calls = Arc::clone(&service.calls);
    {
        let mut pages = service.pages.lock().unwrap();
        pages.push_back(GetOperationsByCursorResponse {
            has_next: true,
            next_cursor: "cursor-2".to_string(),
            items: vec![OperationItem {
                id: "operation-1".to_string(),
                ..OperationItem::default()
            }],
        });
        pages.push_back(GetOperationsByCursorResponse {
            has_next: false,
            next_cursor: String::new(),
            items: vec![OperationItem {
                id: "operation-2".to_string(),
                ..OperationItem::default()
            }],
        });
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OperationsServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    client.runtime.connect().await.unwrap();

    let response = client
        .runtime
        .query_fills(Some("SBER_TQBR.MOEX".to_string()), None, None)
        .await
        .unwrap();

    assert!(!response.has_next);
    assert_eq!(response.next_cursor, "");
    assert_eq!(
        response
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        vec!["operation-1", "operation-2"]
    );

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].account_id, "account-1");
    assert_eq!(calls[0].instrument_id.as_deref(), Some("SBER_TQBR.MOEX"));
    assert_eq!(calls[0].cursor, None);
    assert_eq!(calls[0].limit, Some(1000));
    assert_eq!(calls[1].cursor.as_deref(), Some("cursor-2"));
}

#[tokio::test]
async fn generate_fill_reports_returns_operation_history_trades() {
    let service = MockOperationsService::default();
    {
        let mut pages = service.pages.lock().unwrap();
        let response = GetOperationsByCursorResponse {
            has_next: false,
            items: vec![
                OperationItem {
                    id: "unsupported-operation".to_string(),
                    r#type: TbankOperationType::Buy as i32,
                    state: OperationState::Executed as i32,
                    instrument_uid: "unsupported-uid".to_string(),
                    figi: "unsupported-figi".to_string(),
                    ticker: "BOND".to_string(),
                    class_code: "TQTF".to_string(),
                    ..OperationItem::default()
                },
                OperationItem {
                    id: "operation-1".to_string(),
                    r#type: TbankOperationType::Buy as i32,
                    state: OperationState::Executed as i32,
                    instrument_uid: "sber-uid".to_string(),
                    figi: "BBG004730N88".to_string(),
                    ticker: "SBER".to_string(),
                    class_code: "TQBR".to_string(),
                    commission: Some(MoneyValue {
                        currency: "rub".to_string(),
                        units: 1,
                        nano: 0,
                    }),
                    trades_info: Some(OperationItemTrades {
                        trades: vec![OperationItemTrade {
                            num: "trade-1".to_string(),
                            quantity: 10,
                            price: Some(MoneyValue {
                                currency: "rub".to_string(),
                                units: 275,
                                nano: 0,
                            }),
                            ..OperationItemTrade::default()
                        }],
                    }),
                    ..OperationItem::default()
                },
            ],
            ..GetOperationsByCursorResponse::default()
        };
        pages.push_back(response.clone());
        pages.push_back(response);
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        Server::builder()
            .add_service(OperationsServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = test_client(TbankExecutionClientConfig {
        environment: TbankEnvironment::Live,
        token: Some("test-token".to_string()),
        account_id: Some("account-1".to_string()),
        endpoint: Some(format!("http://{addr}")),
        ..TbankExecutionClientConfig::default()
    });
    seed_sber_metadata(&mut client);
    let mut out_of_scope_metadata = sber_metadata();
    out_of_scope_metadata.instrument_id = "BOND_TQTF.MOEX".to_string();
    out_of_scope_metadata.instrument_uid = "unsupported-uid".to_string();
    out_of_scope_metadata.figi = "unsupported-figi".to_string();
    out_of_scope_metadata.ticker = "BOND".to_string();
    out_of_scope_metadata.class_code = "TQTF".to_string();
    client
        .runtime
        .instruments
        .lock()
        .unwrap()
        .insert(
            out_of_scope_metadata.instrument_id.clone(),
            out_of_scope_metadata,
        );
    client.connect_for_queries().await.unwrap();

    let generate = || {
        <TbankExecutionClient as nautilus_common::clients::ExecutionClient>::generate_fill_reports(
            &client,
            GenerateFillReports::new(
                UUID4::new(),
                current_unix_nanos(),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
    };
    let first = generate().await.unwrap();
    let second = generate().await.unwrap();

    for reports in [&first, &second] {
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].venue_order_id.to_string(), "operation-1");
        assert_eq!(reports[0].trade_id.to_string(), "trade-1");
        assert_eq!(reports[0].instrument_id.to_string(), "SBER_TQBR.MOEX");
        assert_eq!(reports[0].last_qty.as_decimal(), Decimal::from(10));
        assert_eq!(reports[0].last_px.as_decimal(), Decimal::from(275));
    }
}
