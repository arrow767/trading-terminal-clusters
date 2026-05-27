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

    // Partial-snapshot тикер: каждую секунду публикуем DELTA текущего
    // открытого окна в bus → CH-sink её записывает → REST
    // `/v1/clusters/range` отдаёт клиентам in-progress bar данные.
    //
    // **Delta**, не full snapshot: эмитим только бакеты, изменённые
    // с прошлого partial-emit (см. `Aggregator::current_window_delta`).
    // Это критично — full snapshot каждую секунду × 2800 символов × 8 TF
    // = миллионы строк/сек в CH, которые могут вызвать backpressure
    // → переполнение bus → потерю closing-snapshot'ов целых окон.
    // С delta нагрузка масштабируется реальной активностью трейдов.
    //
    // **Корректность при возможной потере delta'ы**: closing-snapshot
    // на window-roll пишет ВСЕ бакеты с финальными значениями. Так как
    // CH использует ReplacingMergeTree(ingested_at), closing-snapshot
    // c самой поздней `ingested_at` всегда выигрывает FINAL дедуп —
    // даже если N delta-эмитов потеряны, закрытие восстанавливает
    // корректное состояние окна.
    let mut partial_ticker = interval(PARTIAL_DELTA_INTERVAL);
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
                if let Some(frame) = agg.current_window_delta() {
                    bus.publish(&stream_key, frame);
                }
            }
        }
    }
}

/// Период публикации delta-snapshot'ов открытого окна в bus. См. коммент
/// в `run_aggregator`. Уменьшение → точнее in-progress bar в UI, но больше
/// нагрузки на CH-write; увеличение → дольше gap для пейна, открытого
/// в середине окна.
const PARTIAL_DELTA_INTERVAL: Duration = Duration::from_secs(1);

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
