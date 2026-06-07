//! MEXC adapter для cluster-ingest.
//!
//! Особенности (вся транспорт-специфика в `mexc_session`):
//! - SPOT: protobuf-фреймы на `wss://wbs-api.mexc.com/ws`, канал
//!   `spot@public.aggre.deals.v3.api.pb@100ms@{SYM}`, subscribe JSON
//!   `{"method":"SUBSCRIPTION",...}`, ping `{"method":"PING"}`.
//! - FUTURES: JSON на `wss://contract.mexc.com/edge`, `sub.deal` per symbol
//!   (`BTC_USDT`), ping `{"method":"ping"}`, qty в контрактах → база.
//! - Оба шардятся по соединениям.
//!
//! Реализует `ExchangeInfo`/`VolumeRanker`, `WsConnector` (`MexcWs`), и два
//! парсера трейдов (`MexcSpotTradeParser` protobuf, `MexcFuturesTradeParser`).

mod instruments_info;
mod mexc_trade;
mod mexc_ws;
mod pb;
mod scale;

pub use instruments_info::{MexcCategory, MexcInstrumentsInfo};
pub use mexc_trade::{MexcFuturesTradeParser, MexcSpotTradeParser};
pub use mexc_ws::MexcWs;
