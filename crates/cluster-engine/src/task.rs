use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use exchange_core::TradePrint;
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

use crate::aggregator::Aggregator;
use crate::bus::ClusterBus;

/// Drives one `Aggregator` to completion: pulls trades from `trades`,
/// pulses `Aggregator::tick` on `tick_interval`, and publishes every
/// emitted frame onto `bus`. Returns when the trade channel closes.
pub async fn run_aggregator(
    mut agg: Aggregator,
    mut trades: mpsc::Receiver<TradePrint>,
    bus: Arc<ClusterBus>,
    tick_interval: Duration,
) {
    let key = agg.key().clone();
    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            maybe_trade = trades.recv() => {
                match maybe_trade {
                    Some(trade) => {
                        if let Some(frame) = agg.ingest(trade) {
                            bus.publish(&key, frame);
                        }
                    }
                    None => {
                        if let Some(frame) = agg.flush() {
                            bus.publish(&key, frame);
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Some(frame) = agg.tick(now_ns()) {
                    bus.publish(&key, frame);
                }
            }
        }
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{AggressorSide, ClusterFrame, Exchange, MarketType, SymbolKey};

    use super::*;

    #[tokio::test]
    async fn task_publishes_closing_snapshot_on_window_roll() {
        let bus = Arc::new(ClusterBus::new());
        let key = SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT");
        let mut rx = bus.subscribe(&key);

        let agg = Aggregator::new(key.clone(), 10, 60_000, 100);
        let (tx, rx_trades) = mpsc::channel(16);
        let bus_clone = Arc::clone(&bus);
        let handle = tokio::spawn(async move {
            run_aggregator(agg, rx_trades, bus_clone, Duration::from_millis(50)).await;
        });

        tx.send(TradePrint {
            exchange_ts_ns: 1_000,
            aggressor: AggressorSide::Bid,
            price: 100,
            qty: 5,
            trade_id: 1,
        })
        .await
        .unwrap();
        tx.send(TradePrint {
            exchange_ts_ns: 60_000_000_001,
            aggressor: AggressorSide::Ask,
            price: 200,
            qty: 1,
            trade_id: 2,
        })
        .await
        .unwrap();

        let frame = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match rx.recv().await.unwrap() {
                    ClusterFrame::Snapshot(s) => return s,
                    ClusterFrame::Diff(_) => continue,
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(frame.window_start_ns, 0);
        assert_eq!(frame.clusters.len(), 1);
        assert_eq!(frame.clusters[0].bid_qty, 5);

        drop(tx);
        handle.await.unwrap();
    }
}
