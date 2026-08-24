use super::*;

impl TbankDataClient {
    fn sync_nautilus_order_book_registry(&mut self, instrument_id: InstrumentId) {
        let mut depths = HashSet::new();
        if self.quote_subscriptions.contains(&instrument_id) {
            depths.insert(1);
        }
        if let Some(depth) = self.depth10_subscriptions.get(&instrument_id) {
            depths.insert(*depth);
        }
        self.subscriptions
            .replace_order_books_for_instrument(instrument_id, depths);
    }
}

#[async_trait(?Send)]
impl DataClient for TbankDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        // T-Bank is a multi-venue broker. The command's instrument venue is
        // registered by LiveNode routing and must not be constrained to MOEX.
        None
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        self.clear_market_data_subscription_state();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.disconnect();
        Ok(())
    }

    fn is_connected(&self) -> bool {
        TbankDataClient::is_connected(self)
    }

    fn is_disconnected(&self) -> bool {
        !TbankDataClient::is_connected(self)
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        TbankDataClient::connect(self)
            .await
            .map_err(anyhow::Error::from)
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        TbankDataClient::disconnect(self);
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        self.quote_subscriptions.insert(cmd.instrument_id);
        // Nautilus quotes are projected from the exchange order book at depth one, so the restore
        // snapshot is keyed by the stable instrument identity and resolves its broker route when
        // the request is rebuilt.
        self.sync_nautilus_order_book_registry(cmd.instrument_id);
        self.schedule_quote_stream()
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let stream_id = self.stream_id(cmd.instrument_id);
        self.trade_subscriptions.insert(cmd.instrument_id);
        self.subscriptions
            .subscribe_trades_for_instrument(cmd.instrument_id, stream_id);
        self.schedule_trade_stream(cmd.instrument_id)
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        let spec = cmd.bar_type.spec();
        if spec.aggregation != BarAggregation::Minute || spec.step.get() != 1 {
            anyhow::bail!("T-Bank data client supports only 1-minute bars in v1");
        }
        let stream_id = self.stream_id(cmd.bar_type.instrument_id());
        self.subscriptions
            .subscribe_bars_1m_for_instrument(cmd.bar_type.instrument_id(), stream_id);
        self.bar_subscriptions
            .insert(cmd.bar_type.instrument_id(), cmd.bar_type);
        self.schedule_bar_streams()
    }

    fn subscribe_book_depth10(&mut self, cmd: SubscribeBookDepth10) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("T-Bank data client supports only L2_MBP order book depth");
        }
        let depth = cmd.depth.map_or(DEPTH10_LEN as i32, |depth| {
            depth.get().min(DEPTH10_LEN) as i32
        });
        self.depth10_subscriptions.insert(cmd.instrument_id, depth);
        self.sync_nautilus_order_book_registry(cmd.instrument_id);
        self.schedule_depth10_stream(cmd.instrument_id, depth)
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.quote_subscriptions.remove(&cmd.instrument_id);
        self.sync_nautilus_order_book_registry(cmd.instrument_id);
        self.schedule_quote_stream()
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let stream_id = self.stream_id(cmd.instrument_id);
        self.trade_subscriptions.remove(&cmd.instrument_id);
        self.subscriptions
            .unsubscribe_trades_for_instrument(cmd.instrument_id, stream_id);
        self.abort_stream(&stream_task_key(
            "trades",
            &cmd.instrument_id.to_string(),
            "all",
        ));
        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let instrument_id = cmd.bar_type.instrument_id();
        let stream_id = self.stream_id(instrument_id);
        self.bar_subscriptions.remove(&cmd.bar_type.instrument_id());
        self.subscriptions
            .unsubscribe_bars_1m_for_instrument(instrument_id, stream_id);
        self.schedule_bar_streams()
    }

    fn unsubscribe_book_depth10(&mut self, cmd: &UnsubscribeBookDepth10) -> anyhow::Result<()> {
        self.depth10_subscriptions.remove(&cmd.instrument_id);
        let stream_id = self.stream_id(cmd.instrument_id);
        self.subscriptions
            .unsubscribe_depth_books_for_instrument(cmd.instrument_id, stream_id);
        self.sync_nautilus_order_book_registry(cmd.instrument_id);
        self.abort_stream(&stream_task_key(
            "depth10",
            &cmd.instrument_id.to_string(),
            "book",
        ));
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let clients = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .clone();
        let timestamp_mode = self.config.candle_timestamp_mode;
        let request_timeout = self.config.historical_candle_request_timeout;
        let indicative_instruments = self.config.indicative_instruments.clone();
        let sender = get_data_event_sender();
        let instrument_id = request.instrument_id;
        let resolved_client_id = request.client_id.unwrap_or_else(|| self.client_id());
        let request_id = request.request_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get());
        let params = request.params;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);

        get_runtime().spawn(async move {
            let result = async {
                let mut historical = crate::historical::TbankHistoricalClient::new(
                    clients,
                    timestamp_mode,
                    request_timeout,
                    indicative_instruments,
                );
                let resolved = historical.resolve_instrument(instrument_id).await?;
                let from = start.ok_or_else(|| anyhow::anyhow!("request_trades requires start"))?;
                let to = end.ok_or_else(|| anyhow::anyhow!("request_trades requires end"))?;
                historical
                    .request_trades(
                        &resolved,
                        instrument_id,
                        from,
                        to,
                        crate::historical::DEFAULT_TRADE_SOURCE,
                        limit,
                    )
                    .await
            }
            .await;

            match result {
                Ok(trades) => {
                    let response = DataResponse::Trades(TradesResponse::new(
                        request_id,
                        resolved_client_id,
                        instrument_id,
                        trades,
                        start_nanos,
                        end_nanos,
                        now_unix_nanos(),
                        params,
                    ));
                    if let Err(error) = sender.send(DataEvent::Response(response)) {
                        tracing::error!(%error, "failed to publish T-Bank trades response");
                    }
                }
                Err(error) => tracing::error!(%error, "T-Bank request_trades failed"),
            }
        });

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let clients = self
            .clients
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .clone();
        let timestamp_mode = self.config.candle_timestamp_mode;
        let request_timeout = self.config.historical_candle_request_timeout;
        let retries = self.config.historical_candle_max_retries as usize;
        let indicative_instruments = self.config.indicative_instruments.clone();
        let sender = get_data_event_sender();
        let bar_type = request.bar_type;
        let instrument_id = bar_type.instrument_id();
        let interval = crate::historical::TbankBarInterval::try_from_bar_type(bar_type)?;
        let resolved_client_id = request.client_id.unwrap_or_else(|| self.client_id());
        let request_id = request.request_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get());
        let params = request.params;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);

        get_runtime().spawn(async move {
            let result = async {
                let mut historical = crate::historical::TbankHistoricalClient::new(
                    clients,
                    timestamp_mode,
                    request_timeout,
                    indicative_instruments,
                );
                let resolved = historical.resolve_instrument(instrument_id).await?;
                let from = start.ok_or_else(|| anyhow::anyhow!("request_bars requires start"))?;
                let to = end.ok_or_else(|| anyhow::anyhow!("request_bars requires end"))?;
                historical
                    .request_bars(&resolved, bar_type, interval, from, to, limit, retries)
                    .await
            }
            .await;

            match result {
                Ok(bars) => {
                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        resolved_client_id,
                        bar_type,
                        bars,
                        start_nanos,
                        end_nanos,
                        now_unix_nanos(),
                        params,
                    ));
                    if let Err(error) = sender.send(DataEvent::Response(response)) {
                        tracing::error!(%error, "failed to publish T-Bank bars response");
                    }
                }
                Err(error) => tracing::error!(%error, "T-Bank request_bars failed"),
            }
        });

        Ok(())
    }
}
