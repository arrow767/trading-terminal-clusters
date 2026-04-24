use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    BinanceF,
    Bybit,
    BybitF,
    Bitget,
    BitgetF,
    Okx,
    OkxF,
    Hyperliquid,
    Kucoin,
    KucoinF,
    Gate,
    GateF,
}

impl Exchange {
    pub fn wire_id(self) -> &'static str {
        match self {
            Exchange::Binance => "BINANCE",
            Exchange::BinanceF => "BINANCEF",
            Exchange::Bybit => "BYBIT",
            Exchange::BybitF => "BYBITF",
            Exchange::Bitget => "BITGET",
            Exchange::BitgetF => "BITGETF",
            Exchange::Okx => "OKX",
            Exchange::OkxF => "OKXF",
            Exchange::Hyperliquid => "HYPERLIQUID",
            Exchange::Kucoin => "KUCOIN",
            Exchange::KucoinF => "KUCOINF",
            Exchange::Gate => "GATE",
            Exchange::GateF => "GATEF",
        }
    }

    pub fn is_futures(self) -> bool {
        matches!(
            self,
            Exchange::BinanceF
                | Exchange::BybitF
                | Exchange::BitgetF
                | Exchange::OkxF
                | Exchange::Hyperliquid
                | Exchange::KucoinF
                | Exchange::GateF
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    Spot,
    Perp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Quote {
    Usdt,
    Usdc,
}

/// Side of the resting order that was hit — i.e. aggressor direction.
/// Bid = aggressor was buying (lifted the ask); Ask = aggressor was selling (hit the bid).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AggressorSide {
    Bid = 0,
    Ask = 1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSpec {
    pub exchange: Exchange,
    pub market_type: MarketType,
    pub quote: Quote,
    pub symbol: String,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub tick_size: i64,
    pub step_size: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TradePrint {
    pub exchange_ts_ns: i64,
    pub aggressor: AggressorSide,
    pub price: i64,
    pub qty: i64,
    pub trade_id: u64,
}

/// Single footprint bucket: one price level within one time window.
/// Matches the fat-terminal `ClusterBucket` field layout so wire frames
/// stay byte-compatible after MessagePack encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBucket {
    pub price: i64,
    pub bid_qty: i64,
    pub ask_qty: i64,
    pub trades: u32,
}

/// Cheap-to-clone identity of a subscribed instrument. Used as a key in
/// the broadcast bus and various registries; `Arc<str>` keeps clones
/// pointer-sized.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SymbolKey {
    pub exchange: Exchange,
    pub market_type: MarketType,
    pub symbol: Arc<str>,
}

impl SymbolKey {
    pub fn new(exchange: Exchange, market_type: MarketType, symbol: impl Into<Arc<str>>) -> Self {
        Self {
            exchange,
            market_type,
            symbol: symbol.into(),
        }
    }
}

/// Full state of the current (still-open) time window for one instrument.
/// Wire-equivalent of fat-terminal's `AnalyticsSnapshot` (union id 104).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsSnapshot {
    pub window_start_ns: i64,
    pub sequence: i64,
    pub clusters: Vec<ClusterBucket>,
}

/// Incremental update since the last snapshot/diff for the same window.
/// Wire-equivalent of fat-terminal's `AnalyticsDiff` (union id 105).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsDiff {
    pub window_start_ns: i64,
    pub sequence: i64,
    pub upserts: Vec<ClusterBucket>,
    pub removes: Vec<i64>,
}

/// Unit of broadcast through the bus. Arc-wrapped so fan-out to N
/// subscribers does not clone the inner Vec.
#[derive(Debug, Clone)]
pub enum ClusterFrame {
    Snapshot(Arc<AnalyticsSnapshot>),
    Diff(Arc<AnalyticsDiff>),
}

impl ClusterFrame {
    pub fn sequence(&self) -> i64 {
        match self {
            ClusterFrame::Snapshot(s) => s.sequence,
            ClusterFrame::Diff(d) => d.sequence,
        }
    }
}
