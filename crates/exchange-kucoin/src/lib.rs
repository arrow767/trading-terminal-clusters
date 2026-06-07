//! KuCoin adapter для cluster-ingest.
//!
//! Особенности (см. `kucoin_session` в cluster-ingest):
//! - НЕТ статичного WS URL: сначала `POST /api/v1/bullet-public` → token +
//!   динамический endpoint, потом коннект `wss://{endpoint}?token=..&connectId=..`.
//! - Лимит подписок на коннект (~400, close 509) → шардинг.
//! - Futures qty в КОНТРАКТАХ → база через multiplier (как OKX ctVal).
//! - Алиас `XBT`↔BTC; spot символы вида `BTC-USDT`.
//!
//! Реализует: `ExchangeInfo`/`VolumeRanker` (instruments), `WsConnector`
//! (+ `fetch_bullet`/`venue_symbol`/`exec_topic_prefix`), `KucoinTradeParser`.

mod instruments_info;
mod kucoin_trade;
mod kucoin_ws;
mod scale;

pub use instruments_info::{KucoinCategory, KucoinInstrumentsInfo};
pub use kucoin_trade::KucoinTradeParser;
pub use kucoin_ws::{KucoinBullet, KucoinWs};
