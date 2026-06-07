# /add-clusters — Инжест кластеров новой биржи (сервер) + деплой

## Когда
Добавить footprint-кластера (live + историю) для новой биржи в
`trading-terminal-clusters`, чтобы терминал тянул серверную историю и мёржил с
live. Эталон — **OKX** (commit `c25770a`): крейт `exchange-okx` + прошивка в
`cluster-ingest`. Парный скилл в репозитории терминала — `/add-clusters` там.

## 🚨 ГЛАВНАЯ ГРАБЛЯ — scale-синхронизация
Серверная математика `price_scale` / `qty_scale` / `qty` для биржи ДОЛЖНА
совпадать **байт-в-байт** с live-движком терминала
(`fat-trading-terminal/rust-ws-engine/src/<x>`). Иначе клиентский merge
(`max` per `(price, side)`) разъезжается — серверные и live-бакеты не сходятся,
history-колонка двоится. **Порти scale-логику из live-движка дословно.**

Контрактные рынки (OKX `ctVal`, и т.п.): WS-qty в КОНТРАКТАХ → переводи в базу:
`qty_scale = decimals(lotSz * ctVal)`, `qty = parse_scaled(sz) * ct_num / ct_den`.
`SymbolSpec` поля ctVal не несёт — держи множитель в crate-local map
(см. `exchange-okx/src/scale.rs`: `set_ct` пишет при discovery только для свопа,
`get_ct` читает в парсере только для `MarketType::Perp`, иначе одноимённый спот
подхватит множитель свопа).

## Шаги

### 1. exchange-core (если биржи нет в enum)
- `crates/exchange-core/src/types.rs`: вариант `Exchange::X` / `XF` + `wire_id()`
  (`"X"` / `"XF"`) + `is_futures()`. (Okx/Bitget/Gate/Kucoin уже есть.)
- `crates/cluster-api/src/stream_server.rs`: маппинг строки `"X"`/`"XF"` → `Exchange`.

### 2. Новый крейт `crates/exchange-<x>/` (копируй `exchange-okx`)
- `Cargo.toml` — deps: exchange-core, async-trait, serde, serde_json, tracing, reqwest.
- `src/scale.rs` — `count_decimals_trimmed` + `parse_scaled` (одинаковы у всех) +
  биржевые хелперы (instId↔canonical, ctVal-фракция, ct-map) если рынок контрактный.
- `src/instruments_info.rs` — `XInstrumentsInfo`: `ExchangeInfo` (REST instruments →
  `Vec<SymbolSpec>`, canonical symbol `BTCUSDT`, фильтр quote USDT/USDC, только live) +
  `VolumeRanker` (REST tickers → quote notional, ключ — canonical symbol).
- `src/<x>_ws.rs` — `XWs`: `WsConnector` (`ws_url`, `subscribe_payloads_batched` на
  trades-канал, ping policy, `max_subscriptions_per_socket`, `as_any`).
- `src/<x>_trade.rs` — `XTradeParser`: `peek_symbol` (→ canonical) + `parse_value`
  (`Vec<TradePrint>`; aggressor buy=Bid / sell=Ask; price/qty scaled; контракт→база
  для perp).
- `src/lib.rs` — `pub use`.
- Покрой unit-тестами на реальных JSON-фикстурах (как `exchange-okx`).

### 3. Прошивка `cluster-ingest`
- Root `Cargo.toml`: `members += "crates/exchange-<x>"`.
- `crates/cluster-ingest/Cargo.toml`: dep `exchange-<x>`.
- `crates/cluster-ingest/src/<x>_session.rs` — копия `okx_session.rs` со своими типами.
- `binance_supervisor.rs`: `use crate::<x>_session::run_session as run_<x>_session;` +
  `SessionFlavor::X` + arm в `run_session_loop` (downcast `as_any` → `XWs`).
- `main.rs`: `mod <x>_session;` + два блока (perp + spot) как у OKX
  (`XInstrumentsInfo`, `SessionFlavor::X`, `Exchange::XF`/`X`, `MarketType::Perp`/`Spot`,
  cfg = `ingest.exchanges.x_perp` / `x_spot`).
- `config.rs`: `ExchangesConfig += x_perp / x_spot: Option<BinancePerpConfig>`.
- `cluster-ingest.example.toml`: секции + `enabled_exchanges`.

### 4. Деплой (хост 202.182.100.188, ssh-ключ ~/.ssh/clusters_vultr)
- `git commit` + `git push origin main`.
- На сервере: `cd /opt/clusters/repo && git pull --ff-only origin main` →
  `~/.cargo/bin/cargo build --release -p cluster-ingest`. **Сборка не трогает живой
  сервис** (бинарь заменяется на диске, процесс держит старый до рестарта) — при
  ошибке сборки простоя нет, чини и пересобирай.
- `/etc/cluster-ingest/config.toml` (СНАЧАЛА бэкап `cp -a`): добавь `"x_perp","x_spot"`
  в `enabled_exchanges` + секции `[ingest.exchanges.x_perp]`/`[x_spot]` +
  retention-override для `XF`/perp, зеркаля `BINANCEF`/perp
  (30s=1d, 1h=30d, 4h/1d=0/forever, остальное — default 7d).
  Валидируй: `python3 -c "import tomllib; tomllib.load(open(CFG,'rb'))"`.
- `systemctl restart cluster-ingest`; `journalctl -u cluster-ingest` →
  «supervisor started: x_perp/x_spot», без panic.

### 5. Verify
- ClickHouse (docker `clusters-clickhouse-1`, пароль в `/etc/cluster-ingest/secrets.env`):
  `docker exec clusters-clickhouse-1 clickhouse-client --password "$CH_PASSWORD" --query
  "SELECT count() FROM clusters.clusters_30s WHERE exchange IN ('X','XF')"` — растёт.
- REST (нужен Bearer `CLUSTER_INGEST_TOKEN` из secrets, auth force-enabled):
  `curl -H "Authorization: Bearer $TOK" "http://127.0.0.1:8080/v1/clusters/range?exchange=XF&market_type=perp&symbol=BTCUSDT&interval_seconds=60&from_ms=..&to_ms=.."`
  → `n>0`, и `price_scale`/`qty_scale` совпадают с live-движком.

Таблицы: `clusters_{30s,1m,5m,15m,30m,1h,4h,1d}`. Партиции `toYYYYMM(window_start)`,
движок `ReplacingMergeTree(ingested_at)`. Retention = per-table TTL (multi-clause
WHERE по exchange+market_type), применяется на старте из `[ingest.retention]`.
