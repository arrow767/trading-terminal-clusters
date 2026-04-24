use dashmap::DashMap;
use exchange_core::{ClusterFrame, SymbolKey};
use tokio::sync::broadcast;

const DEFAULT_CHANNEL_CAPACITY: usize = 256;

/// Fan-out hub from the cluster aggregator to any number of live consumers
/// (terminal push adapter, gRPC stream, metrics tap, etc.). One
/// `tokio::sync::broadcast` channel per `SymbolKey`, created lazily on
/// first publish or subscribe. Slow consumers see `RecvError::Lagged` and
/// must recover by requesting a fresh snapshot.
pub struct ClusterBus {
    channels: DashMap<SymbolKey, broadcast::Sender<ClusterFrame>>,
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

    pub fn publish(&self, key: &SymbolKey, frame: ClusterFrame) {
        let sender = self.sender_for(key);
        // send returns Err when there are no subscribers; that is the
        // normal idle case and must be ignored.
        let _ = sender.send(frame);
    }

    pub fn subscribe(&self, key: &SymbolKey) -> broadcast::Receiver<ClusterFrame> {
        self.sender_for(key).subscribe()
    }

    pub fn active_keys(&self) -> usize {
        self.channels.len()
    }

    fn sender_for(&self, key: &SymbolKey) -> broadcast::Sender<ClusterFrame> {
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

    use exchange_core::{AnalyticsSnapshot, ClusterBucket, Exchange, MarketType};

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
        }))
    }

    #[tokio::test]
    async fn publish_reaches_all_subscribers() {
        let bus = ClusterBus::new();
        let key = SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT");
        let mut rx_a = bus.subscribe(&key);
        let mut rx_b = bus.subscribe(&key);

        bus.publish(&key, snap(1));

        assert_eq!(rx_a.recv().await.unwrap().sequence(), 1);
        assert_eq!(rx_b.recv().await.unwrap().sequence(), 1);
    }

    #[tokio::test]
    async fn different_keys_are_isolated() {
        let bus = ClusterBus::new();
        let key_a = SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT");
        let key_b = SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "ETHUSDT");
        let mut rx_b = bus.subscribe(&key_b);

        bus.publish(&key_a, snap(7));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), rx_b.recv())
                .await
                .is_err()
        );
    }
}
