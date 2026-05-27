use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use exchange_core::{StreamKey, TradePrint};
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};

use crate::aggregator::Aggregator;
use crate::bus::ClusterBus;

/// Drives one `Aggregator` to completion: pulls trades from `trades`,
/// pulses `Aggregator::tick` on `tick_interval`, and publishes every
/// emitted frame onto `bus` под ключом `stream_key` (symbol + interval).
/// `interval_seconds` в StreamKey должен соответствовать `window_ms` агрегатора
/// (это инвариант, supervisor выставляет их парами).
/// Returns when the trade channel closes.
pub async fn run_aggregator(
    mut agg: Aggregator,
    stream_key: StreamKey,
    mut trades: mpsc::Receiver<TradePrint>,
    bus: Arc<ClusterBus>,
    tick_interval: Duration,
) {
    let mut ticker = interval(tick_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Partial-snapshot тикер: каждые 3с публикуем snapshot текущего
    // (ещё открытого) окна в bus, чтобы CH-sink его записал и REST
    // `/v1/clusters/range` отдавал клиентам in-progress bar данные.
    // Без этого пейн UI, открытый в середине окна, видит только то что
    // его локальный аггрегатор успел собрать с момента подписки —
    // первые N минут окна теряются. ReplacingMergeTree(ingested_at)
    // дедуплицирует на чтении (FINAL), так что повторные записи одного
    // (window_start, price) корректно сводятся к последней версии.
    //
    // Cadence жёстко 3с (не зависит от TF) — пользователь предпочёл
    // ресурсы тратить ради актуальности. Гейтинг по `accum.is_empty()`
    // внутри `current_window_snapshot` отсекает тихие символы / только
    // что закрытые окна.
    let mut partial_ticker = interval(PARTIAL_SNAPSHOT_INTERVAL);
    partial_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            maybe_trade = trades.recv() => {
                match maybe_trade {
                    Some(trade) => {
                        if let Some(frame) = agg.ingest(trade) {
                            bus.publish(&stream_key, frame);
                        }
                    }
                    None => {
                        if let Some(frame) = agg.flush() {
                            bus.publish(&stream_key, frame);
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if let Some(frame) = agg.tick(now_ns()) {
                    bus.publish(&stream_key, frame);
                }
            }
            _ = partial_ticker.tick() => {
                if let Some(frame) = agg.current_window_snapshot() {
                    bus.publish(&stream_key, frame);
                }
            }
        }
    }
}

/// Период публикации snapshot'ов открытого окна в bus. См. комментарий
/// в `run_aggregator`. Менять только осознанно: уменьшение → больше
/// write amplification в ClickHouse, увеличение → дольше gap для UI
/// открывающегося в середине окна.
const PARTIAL_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(3);

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{AggressorSide, ClusterFrame, Exchange, MarketType, StreamKey, SymbolKey};

    use super::*;

    #[tokio::test]
    async fn task_publishes_closing_snapshot_on_window_roll() {
        let bus = Arc::new(ClusterBus::new());
        let sym = SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT");
        let stream_key = StreamKey::new(sym.clone(), 60);
        let mut rx = bus.subscribe(&stream_key);

        let agg = Aggregator::new(sym, 10, 60_000, 100);
        let (tx, rx_trades) = mpsc::channel(16);
        let bus_clone = Arc::clone(&bus);
        let stream_key_clone = stream_key.clone();
        let handle = tokio::spawn(async move {
            run_aggregator(agg, stream_key_clone, rx_trades, bus_clone, Duration::from_millis(50)).await;
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
