//! BingX adapter для cluster-ingest (swap + spot).
//!
//! Особенности (вся транспорт-специфика в `bingx_session`):
//! - Раздельные WS-URL: swap = `wss://open-api-swap.bingx.com/swap-market`,
//!   spot = `wss://open-api-ws.bingx.com/market`.
//! - Каждый data-фрейм — GZIP-сжатый бинарь → inflate перед JSON.
//! - Heartbeat — JSON `{"ping":"<uuid>"}` → ответ `{"pong":"<uuid>"}`.
//! - Subscribe по одному каналу на сообщение: `{"id","reqType":"sub","dataType":"<SYM>@trade"}`.
//! - Perp qty в БАЗЕ (без контракт-множителя, ct=1/1).
//! - Полярность агрессора: futures `m`→Ask, spot `m`→Bid (инвертирована).

mod bingx_trade;
mod bingx_ws;
mod instruments_info;
mod scale;

pub use bingx_trade::BingxTradeParser;
pub use bingx_ws::BingxWs;
pub use instruments_info::{BingxCategory, BingxInstrumentsInfo};
pub use scale::to_bingx_symbol;
