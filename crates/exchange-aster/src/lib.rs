//! Aster (asterdex.com) adapter для cluster-ingest.
//!
//! Aster — клон combined-stream Binance (тот же wire-формат aggTrade/depth,
//! те же exchangeInfo поля), отличаются только хосты и spot-путь (`/api/v1`).
//! Поэтому крейт даёт только `ExchangeInfo`/`VolumeRanker` + `WsConnector`;
//! парсинг трейдов и session переиспользуют Binance-путь
//! (`SessionFlavor::Binance` + `BinanceFuturesTradeParser`) в cluster-ingest.
//!
//! Реализует:
//! - REST `/fapi/v1/exchangeInfo` + `/api/v1/exchangeInfo` → `ExchangeInfo`
//! - REST `/{fapi/v1,api/v1}/ticker/24hr` → `VolumeRanker`
//! - WS `/stream` (SUBSCRIBE method) → `WsConnector`

mod aster_ws;
mod instruments_info;
mod scale;

pub use aster_ws::AsterWs;
pub use instruments_info::{AsterCategory, AsterInstrumentsInfo};
