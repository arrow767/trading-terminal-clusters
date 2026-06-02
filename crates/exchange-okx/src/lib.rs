//! OKX V5 adapter для cluster-ingest.
//!
//! Реализует:
//! - REST `/api/v5/public/instruments?instType=SPOT|SWAP` → `ExchangeInfo`
//! - REST `/api/v5/market/tickers` → `VolumeRanker` (quote notional ≈ volCcy24h·last)
//! - WS `/ws/v5/public` → `WsConnector` (client-initiated text `ping`)
//! - `trades` channel envelope parser → `OkxTradeParser`
//!
//! **Граница спот/своп.** Берём только USDT/USDC-маржинальные (linear) пары —
//! инверсные `*-USD-SWAP` отсекаются фильтром quote∈{USDT,USDC}. Своп-qty в WS
//! приходит В КОНТРАКТАХ; конвертируем в базовый актив через `ctVal`, ровно как
//! live-движок терминала (`rust-ws-engine/src/okx/{rest,ws}.rs`), чтобы серверная
//! и live-картинка кластеров совпадали байт-в-байт по (price, qty) и корректно
//! мёржились на клиенте (`max` per (price, side)).
//!
//! Символы храним в каноне `BTCUSDT` (instId `BTC-USDT` / `BTC-USDT-SWAP` →
//! strip `-`/`-SWAP`), как и остальные биржи и как шлёт терминал в
//! `/v1/clusters/range?...&symbol=`.

mod instruments_info;
mod okx_trade;
mod okx_ws;
mod scale;

pub use instruments_info::{OkxCategory, OkxInstrumentsInfo};
pub use okx_trade::OkxTradeParser;
pub use okx_ws::OkxWs;
