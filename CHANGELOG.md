# Изменения

## Unreleased


## 0.4.0 — 2026-10-17

- Обновлены все прямые crates NautilusTrader до `0.66.0` с `v2.0.0rc7`, минимальная версия Rust — `1.99.0`, `jiff` — `0.2.38`.
- **Несовместимое изменение:** поля `token` в `TbankDataClientConfig` и `TbankExecutionClientConfig` используют `Option<SecretString>` вместо обычной строки. Debug-конфигурации и сериализация конфигураций не раскрывают токен.
- Добавлен `TbankDataClient::connect_historical_download()` для standalone-загрузчиков до сборки `LiveNode`. Он открывает только gRPC-канал unary-запросов истории, без Nautilus data-event sender, публикации инструментов и session/stream-задач. Обычный `DataClient::connect` по-прежнему требует настроенный sender и завершается ошибкой при неудачной публикации определения инструмента.
- Execution client следует lifecycle Nautilus и дожидается принятой работы при остановке; неразрешённые исходы submit/cancel/fill остаются fail-closed и блокируют reset. `CancelAllOrders` ограничен заданными инструментом и стороной для обычных и стоп-заявок; неизвестная broker identity не расширяет область отмены.
- Для снимков книги поддержаны запрошенные глубины до 50 уровней без padding; несогласованные снимки отклоняются.
- Сделки из операций с неположительным количеством отклоняются.

## 0.3.1 — 2026-09-29

- Восстановление неоднозначного исхода submit ограничено общим дедлайном по настройкам in-flight окна `LiveNode` (не более 20 секунд); он начинается до подготовки заявки и общий для всех ног `SubmitOrderList`. Передавайте execution-engine настройки `LiveNode` в адаптер через `.with_live_node_execution_engine_config(...)`. Фоновая recovery ladder выполняет не более пяти дополнительных проверок с задержкой до пяти секунд; каждый `GetOrderState` ограничен тремя секундами. При recovery submit `50005`, таймауты и транспортные ошибки сами по себе не подтверждают отсутствие ордера; `PostOrder` повторно не отправляется. Если брокерский отчёт не подтвердил исход, заявка сохраняется со стадией `Unresolved`, доступна через `unresolved_submits()` и блокирует reset; stop-order без единственного совпадения в ограниченной истории также остаётся неразрешённым.
- Vendored-контракты T-Invest обновлены с 1.49 до 1.51 (commit `798f880d` от 11.09.2026). Добавлены валюта цен в ответах market data, рейтинги облигаций, статистика интереса к инструменту, вариационная маржа портфеля, новые типы счетов и альтернативный идентификатор для `GetTechAnalysis`.
- `total_var_margin` и `total_var_margin_settled` теперь доступны потребителям через `AccountState.info` под ключами `TBANK_TOTAL_VAR_MARGIN_INFO_KEY` и `TBANK_TOTAL_VAR_MARGIN_SETTLED_INFO_KEY`; значения передаются точными decimal-строками.
- История стоп-заявок при переполнении ответа (`30261`) автоматически запрашивается временными окнами с дроблением переполненного окна. Для неограниченного запроса используется 30-дневный fallback, поскольку API не предоставляет cursor для `GetStopOrders`.
- Ошибка лимита стоп-заявок на инструмент (`80007`) отделена от транспортного rate limit и становится определённым отказом заявки; повторять такую заявку автоматически нельзя.
- Комиссия филла больше не подменяется молча нулём. Введён тип `TbankFillCommission` (`Reported(Money)` / `Allocated(Money)` / `Unknown`); `Allocated` означает распределённую по нескольким филлам комиссию, рассчитанную из агрегатной комиссии операции. Каждое построение `FillReport` проходит через `fill_report_commission` с явным источником `TbankFillCommissionSource` (`operations_cursor`, `trades_stream`, `order_state_stream`, `order_state_query`, `submit_response`). Когда вендор не передал комиссию, публикуется типизированное custom data `TbankExecutionEvent::FillCommission` со `status = unknown` и пишется `tracing::warn!`; `FillReport.commission` по-прежнему несёт нулевой плейсхолдер, потому что Nautilus требует непустое `Money`. Потребитель обязан читать провенанс из события, а не из `FillReport.commission`.
- Регистрация custom data разделена: `market_data::register_tbank_custom_data` переименована в `market_data::register_tbank_market_data_custom_data` (только market data), добавлена `execution::register_tbank_execution_custom_data` (только execution); корневая `register_tbank_custom_data()` регистрирует оба типа. Старый путь `market_data::register_tbank_custom_data` удалён без алиасов, вызывающий код должен перейти на корневую функцию.
- Добавлены публичные константы провенанса `TBANK_ADAPTER_CRATE_VERSION` (версия crate из `Cargo.toml`, снимается на compile time) и `TBANK_ADAPTER_GIT_REVISION` (Git `HEAD`, снимается `build.rs`); реэкспортируются из корня crate. Незакоммиченные изменения намеренно не входят в ревизию, но продолжают собираться. Если Git-метаданные или исполняемый файл `git` недоступны, `TBANK_ADAPTER_GIT_REVISION` равен `unknown`, а сборка выдаёт предупреждение. Потребители обязаны привязывать артефакты прогонов к ревизии адаптера через `TBANK_ADAPTER_GIT_REVISION`, а не через чтение `Cargo.toml`/`Cargo.lock` адаптера.

## 0.3.0 — 2026-09-10

- Адаптер переведён на NautilusTrader v2.0.0rc4 и согласованные crates версии 0.63.0.
- Обновлены контракты execution client, market data client, фабрик и преобразования исполнения.
- Добавлена stage-aware обработка неоднозначных исходов submit/cancel и расширено покрытие submit-пайплайна.
- Минимальная версия Rust повышена до 1.98.0.
