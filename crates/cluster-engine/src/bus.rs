use dashmap::DashMap;
use exchange_core::{ClusterFrame, StreamKey};
use tokio::sync::broadcast;

const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Fan-out hub from the cluster aggregator to any number of live consumers
/// (terminal push adapter, gRPC stream, metrics tap, etc.). One
/// `tokio::sync::broadcast` channel per `StreamKey` (symbol × timeframe),
/// created lazily on first publish or subscribe. Один и тот же символ
/// на 30s/1m/5m/... — разные каналы; consumer выбирает нужный TF на
/// `subscribe`. Slow consumers see `RecvError::Lagged` and must recover
/// by requesting a fresh snapshot.
pub struct ClusterBus {
    channels: DashMap<StreamKey, broadcast::Sender<ClusterFrame>>,
    capacity: usize,
}

impl ClusterBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            channels: DashMap::new(),
            capacity,
        }
    }

    pub fn publish(&self, key: &StreamKey, frame: ClusterFrame) {
        let sender = self.sender_for(key);
        // send returns Err when there are no subscribers; that is the
        // normal idle case and must be ignored.
        let _ = sender.send(frame);
    }

    pub fn subscribe(&self, key: &StreamKey) -> broadcast::Receiver<ClusterFrame> {
        self.sender_for(key).subscribe()
    }

    pub fn active_keys(&self) -> usize {
        self.channels.len()
    }

    fn sender_for(&self, key: &StreamKey) -> broadcast::Sender<ClusterFrame> {
        if let Some(existing) = self.channels.get(key) {
            return existing.clone();
        }
        self.channels
            .entry(key.clone())
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .clone()
    }
}

impl Default for ClusterBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use exchange_core::{AnalyticsSnapshot, ClusterBucket, Exchange, MarketType, SymbolKey};

    use super::*;

    fn snap(seq: i64) -> ClusterFrame {
        ClusterFrame::Snapshot(Arc::new(AnalyticsSnapshot {
            window_start_ns: 0,
            sequence: seq,
            clusters: vec![ClusterBucket {
                price: 100,
                bid_qty: 1,
                ask_qty: 2,
                trades: 3,
            }],
            ..Default::default()
        }))
    }

    fn key(sym: &str, tf: u32) -> StreamKey {
        StreamKey::new(
            SymbolKey::new(Exchange::BinanceF, MarketType::Perp, sym),
            tf,
        )
    }

    #[tokio::test]
    async fn publish_reaches_all_subscribers() {
        let bus = ClusterBus::new();
        let k = key("BTCUSDT", 60);
        let mut rx_a = bus.subscribe(&k);
        let mut rx_b = bus.subscribe(&k);

        bus.publish(&k, snap(1));

        assert_eq!(rx_a.recv().await.unwrap().sequence(), 1);
        assert_eq!(rx_b.recv().await.unwrap().sequence(), 1);
    }

    #[tokio::test]
    async fn different_symbols_are_isolated() {
        let bus = ClusterBus::new();
        let key_a = key("BTCUSDT", 60);
        let key_b = key("ETHUSDT", 60);
        let mut rx_b = bus.subscribe(&key_b);

        bus.publish(&key_a, snap(7));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), rx_b.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn same_symbol_different_intervals_are_isolated() {
        // 30s и 1m для одного и того же символа — это разные стримы,
        // нельзя чтобы 30s subscriber случайно получал 1m кадры.
        let bus = ClusterBus::new();
        let k30 = key("BTCUSDT", 30);
        let k60 = key("BTCUSDT", 60);
        let mut rx_30 = bus.subscribe(&k30);
        let mut rx_60 = bus.subscribe(&k60);

        bus.publish(&k30, snap(1));
        assert_eq!(rx_30.recv().await.unwrap().sequence(), 1);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), rx_60.recv())
                .await
                .is_err(),
            "1m subscriber must NOT see 30s frame"
        );

        bus.publish(&k60, snap(2));
        assert_eq!(rx_60.recv().await.unwrap().sequence(), 2);
    }
}
