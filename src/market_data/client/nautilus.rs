use super::*;
use nautilus_common::clients::{execute_bar_subscriptions, execute_instrument_subscriptions};

fn request_timestamp_to_datetime(
    timestamp: jiff::Timestamp,
) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let nanos = timestamp.as_nanosecond();
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000))?;
    let sub_nanos = u32::try_from(nanos.rem_euclid(1_000_000_000))?;
    chrono::DateTime::from_timestamp(seconds, sub_nanos)
        .ok_or_else(|| anyhow::anyhow!("historical request timestamp is out of range"))
}

impl TbankDataClient {
    fn sync_nautilus_order_book_registry(&mut self, instrument_id: InstrumentId) {
        let mut depths = HashSet::new();
        if self.quote_subscriptions.contains(&instrument_id) {
            depths.insert(1);
        }
        if let Some(depth) = self.order_book_depth_subscriptions.get(&instrument_id) {
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

    // These synchronous hooks request cancellation; async disconnect observes bounded completion.
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
        TbankDataClient::disconnect_async(self).await
    }

    fn subscribe(
        &mut self,
        cmd: nautilus_common::messages::data::SubscribeCustomData,
    ) -> anyhow::Result<()> {
        let key = TbankBookSubscription::from_data_type(&cmd.data_type)?;
        if self.custom_book_subscriptions.contains(&key) {
            return Ok(());
        }
        anyhow::ensure!(
            self.custom_book_subscriptions.len() < MAX_SUBSCRIPTIONS_PER_STREAM,
            "T-Bank custom order book group supports at most {MAX_SUBSCRIPTIONS_PER_STREAM} subscriptions"
        );
        self.custom_book_subscriptions.insert(key);
        if let Err(error) = self.schedule_custom_book_stream() {
            self.custom_book_subscriptions.remove(&key);
            return Err(error);
        }
        Ok(())
    }

    fn unsubscribe(
        &mut self,
        cmd: &nautilus_common::messages::data::UnsubscribeCustomData,
    ) -> anyhow::Result<()> {
        let key = TbankBookSubscription::from_data_type(&cmd.data_type)?;
        if !self.custom_book_subscriptions.remove(&key) {
            return Ok(());
        }
        if self.custom_book_subscriptions.is_empty() {
            self.abort_stream(&Self::custom_book_task_key())?;
        } else if let Err(error) = self.schedule_custom_book_stream() {
            self.custom_book_subscriptions.insert(key);
            return Err(error);
        }
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            if self.quote_subscriptions.insert(id) {
                if let Err(error) = self.schedule_quote_stream() {
                    self.quote_subscriptions.remove(&id);
                    return Err(error);
                }
                self.sync_nautilus_order_book_registry(id);
            }
            Ok(())
        })
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            if !self.trade_subscriptions.contains(&id) {
                self.schedule_trade_stream(id)?;
                self.trade_subscriptions.insert(id);
                self.subscriptions
                    .subscribe_trades_for_instrument(id, self.stream_id(id));
            }
            Ok(())
        })
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        execute_bar_subscriptions(&cmd.bar_types, |bar_type| {
            let spec = bar_type.spec();
            anyhow::ensure!(
                spec.aggregation == BarAggregation::Minute && spec.step.get() == 1,
                "T-Bank data client supports only 1-minute bars in v1"
            );
            let id = bar_type.instrument_id();
            if let Some(existing) = self.bar_subscriptions.get(&id) {
                anyhow::ensure!(
                    *existing == bar_type,
                    "T-Bank already has a different bar subscription for {id}"
                );
                return Ok(());
            }
            self.bar_subscriptions.insert(id, bar_type);
            if let Err(error) = self.schedule_bar_streams() {
                self.bar_subscriptions.remove(&id);
                return Err(error);
            }
            self.subscriptions
                .subscribe_bars_1m_for_instrument(id, self.stream_id(id));
            Ok(())
        })
    }

    fn subscribe_book_depth(&mut self, cmd: SubscribeBookDepth) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "T-Bank data client supports only L2_MBP order book depth"
        );
        let requested_depth = cmd.depth.map_or(DEPTH10_LEN, |depth| depth.get());
        anyhow::ensure!(
            matches!(requested_depth, 1 | 10 | 20 | 30 | 40 | 50),
            "T-Bank supports order book depths 1, 10, 20, 30, 40, and 50"
        );
        let depth = requested_depth as i32;
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            if self.order_book_depth_subscriptions.get(&id) != Some(&depth) {
                self.schedule_order_book_depth_stream(id, depth)?;
                self.order_book_depth_subscriptions.insert(id, depth);
                self.sync_nautilus_order_book_registry(id);
            }
            Ok(())
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            if self.quote_subscriptions.remove(&id) {
                if let Err(error) = self.schedule_quote_stream() {
                    self.quote_subscriptions.insert(id);
                    return Err(error);
                }
                self.sync_nautilus_order_book_registry(id);
            }
            Ok(())
        })
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.abort_stream(&stream_task_key("trades", &id.to_string(), "all"))?;
            self.trade_subscriptions.remove(&id);
            self.subscriptions
                .unsubscribe_trades_for_instrument(id, self.stream_id(id));
            Ok(())
        })
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        execute_bar_subscriptions(&cmd.bar_types, |bar_type| {
            let id = bar_type.instrument_id();
            if self.bar_subscriptions.get(&id) == Some(&bar_type) {
                self.bar_subscriptions.remove(&id);
                if let Err(error) = self.schedule_bar_streams() {
                    self.bar_subscriptions.insert(id, bar_type);
                    return Err(error);
                }
                self.subscriptions
                    .unsubscribe_bars_1m_for_instrument(id, self.stream_id(id));
            }
            Ok(())
        })
    }

    fn unsubscribe_book_depth(&mut self, cmd: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |id| {
            self.abort_stream(&stream_task_key("book_depth", &id.to_string(), "book"))?;
            self.order_book_depth_subscriptions.remove(&id);
            self.sync_nautilus_order_book_registry(id);
            Ok(())
        })
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let spawner = self.session_tasks.spawner().map_err(anyhow::Error::from)?;
        let clients = self
            .grpc_clients_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .clone();
        let publication = Arc::clone(&self.session_publication);
        let session_generation = publication.current();
        let timestamp_mode = self.config.candle_timestamp_mode;
        let request_timeout = self.config.historical_candle_request_timeout;
        let indicative_instruments = self.config.indicative_instruments.clone();
        let sender = get_data_event_sender();
        let instrument_id = request.single_instrument_id()?;
        let resolved_client_id = request.client_id.unwrap_or_else(|| self.client_id());
        let request_id = request.request_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get());
        let params = request.params;
        let start_nanos = start
            .map(nautilus_core::datetime::try_datetime_to_unix_nanos)
            .transpose()?;
        let end_nanos = end
            .map(nautilus_core::datetime::try_datetime_to_unix_nanos)
            .transpose()?;

        self.spawn_session_task(
            &spawner,
            "historical-trades-request",
            TaskCancellation::new(),
            async move {
                let result = async {
                    let mut historical = crate::historical::TbankHistoricalClient::new(
                        clients,
                        timestamp_mode,
                        request_timeout,
                        indicative_instruments,
                    );
                    let resolved = historical.resolve_instrument(instrument_id).await?;
                    let from = request_timestamp_to_datetime(
                        start.ok_or_else(|| anyhow::anyhow!("request_trades requires start"))?,
                    )?;
                    let to = request_timestamp_to_datetime(
                        end.ok_or_else(|| anyhow::anyhow!("request_trades requires end"))?,
                    )?;
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
                        publication.if_current(session_generation, || {
                            if let Err(error) = sender.send(DataEvent::Response(response)) {
                                tracing::error!(%error, "failed to publish T-Bank trades response");
                            }
                        });
                    }
                    Err(error) => tracing::error!(%error, "T-Bank request_trades failed"),
                }
            },
        )?;

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let spawner = self.session_tasks.spawner().map_err(anyhow::Error::from)?;
        let clients = self
            .grpc_clients_ref()
            .ok_or_else(|| anyhow::anyhow!("data client is not connected"))?
            .clone();
        let publication = Arc::clone(&self.session_publication);
        let session_generation = publication.current();
        let timestamp_mode = self.config.candle_timestamp_mode;
        let request_timeout = self.config.historical_candle_request_timeout;
        let retries = self.config.historical_candle_max_retries as usize;
        let indicative_instruments = self.config.indicative_instruments.clone();
        let sender = get_data_event_sender();
        let bar_type = request.single_bar_type()?;
        let instrument_id = bar_type.instrument_id();
        let interval = crate::historical::TbankBarInterval::try_from_bar_type(bar_type)?;
        let resolved_client_id = request.client_id.unwrap_or_else(|| self.client_id());
        let request_id = request.request_id;
        let start = request.start;
        let end = request.end;
        let limit = request.limit.map(|limit| limit.get());
        let params = request.params;
        let start_nanos = start
            .map(nautilus_core::datetime::try_datetime_to_unix_nanos)
            .transpose()?;
        let end_nanos = end
            .map(nautilus_core::datetime::try_datetime_to_unix_nanos)
            .transpose()?;

        self.spawn_session_task(
            &spawner,
            "historical-bars-request",
            TaskCancellation::new(),
            async move {
                let result = async {
                    let mut historical = crate::historical::TbankHistoricalClient::new(
                        clients,
                        timestamp_mode,
                        request_timeout,
                        indicative_instruments,
                    );
                    let resolved = historical.resolve_instrument(instrument_id).await?;
                    let from = request_timestamp_to_datetime(
                        start.ok_or_else(|| anyhow::anyhow!("request_bars requires start"))?,
                    )?;
                    let to = request_timestamp_to_datetime(
                        end.ok_or_else(|| anyhow::anyhow!("request_bars requires end"))?,
                    )?;
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
                        publication.if_current(session_generation, || {
                            if let Err(error) = sender.send(DataEvent::Response(response)) {
                                tracing::error!(%error, "failed to publish T-Bank bars response");
                            }
                        });
                    }
                    Err(error) => tracing::error!(%error, "T-Bank request_bars failed"),
                }
            },
        )?;

        Ok(())
    }
}
