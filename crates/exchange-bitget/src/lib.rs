//! Bitget V2 adapter для cluster-ingest.
//!
//! Реализует:
//! - REST `/api/v2/mix/market/contracts` + `/api/v2/spot/public/symbols` →
//!   `ExchangeInfo`
//! - REST `/api/v2/{mix,spot}/market/tickers` → `VolumeRanker`
//! - WS `/v2/ws/public` → `WsConnector` (client-initiated text ping)
//! - `trade`-канал envelope parser → `BitgetTradeParser`
//!
//! USDT-FUTURES линейные (qty в базе) — без контракт-множителя.
//! Использование в supervisor — см. `cluster-ingest::bitget_session`.

mod bitget_trade;
mod bitget_ws;
mod instruments_info;
mod scale;

pub use bitget_trade::BitgetTradeParser;
pub use bitget_ws::BitgetWs;
pub use instruments_info::{BitgetCategory, BitgetInstrumentsInfo};
