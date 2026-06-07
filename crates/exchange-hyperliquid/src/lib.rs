//! Hyperliquid adapter для cluster-ingest (perp only).
//!
//! - REST `POST /info {"type":"meta"}` → `ExchangeInfo` (фикс MAX-precision
//!   scale: price_scale = 6 - szDecimals, qty_scale = szDecimals).
//! - WS `wss://api.hyperliquid.xyz/ws` → `WsConnector` (subscribe per coin,
//!   text ping); парсер `trades` → `HyperliquidTradeParser`.
//! - HL — USDC-маржинальный perp DEX; canonical symbol `{COIN}USDC`, нативный
//!   wire-coin (`BTC`/`kPEPE`) хранится в map при discovery.
//!
//! Транспорт (subscribe per coin, шардинг, ping) — в `hyperliquid_session`.

mod hyperliquid_trade;
mod hyperliquid_ws;
mod instruments_info;
mod scale;

pub use hyperliquid_trade::HyperliquidTradeParser;
pub use hyperliquid_ws::HyperliquidWs;
pub use instruments_info::HyperliquidInstrumentsInfo;
