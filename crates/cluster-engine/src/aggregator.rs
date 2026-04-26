use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use exchange_core::{
    AggressorSide, AnalyticsDiff, AnalyticsSnapshot, ClusterBucket, ClusterFrame, SymbolKey,
    TradePrint,
};

#[derive(Default)]
struct ClusterAccum {
    bid_qty: i64,
    ask_qty: i64,
    trades: u32,
}

/// Per-symbol footprint aggregator. Pure-functional: no I/O, no clocks
/// pulled internally — the runtime feeds it `TradePrint`s and timer
/// pulses, and consumes the optional `ClusterFrame` that each call may
/// return.
///
/// Window roll happens on the first trade whose timestamp falls into a
/// later bucket; the closing snapshot of the previous window is returned
/// from that same `ingest` call.
///
/// Out-of-order trades that fall into a window already closed are
/// silently dropped — depending on which exchange and how badly the
/// stream lagged, this can happen on reconnect; counters in the runtime
/// task track the rate.
pub struct Aggregator {
    key: SymbolKey,
    bucket_step: i64,
    window_ns: i64,
    diff_interval_ns: i64,

    current_window_start_ns: Option<i64>,
    sequence: i64,
    accum: HashMap<i64, ClusterAccum>,
    dirty: HashSet<i64>,
    last_diff_emit_ns: i64,
    dropped_late_trades: u64,
}

impl Aggregator {
    pub fn new(key: SymbolKey, bucket_step: i64, window_ms: i64, diff_interval_ms: i64) -> Self {
        assert!(bucket_step > 0, "bucket_step must be positive");
        assert!(window_ms > 0, "window_ms must be positive");
        assert!(diff_interval_ms > 0, "diff_interval_ms must be positive");
        Self {
            key,
            bucket_step,
            window_ns: window_ms * 1_000_000,
            diff_interval_ns: diff_interval_ms * 1_000_000,
            current_window_start_ns: None,
            sequence: 0,
            accum: HashMap::new(),
            dirty: HashSet::new(),
            last_diff_emit_ns: 0,
            dropped_late_trades: 0,
        }
    }

    pub fn key(&self) -> &SymbolKey {
        &self.key
    }

    pub fn dropped_late_trades(&self) -> u64 {
        self.dropped_late_trades
    }

    pub fn ingest(&mut self, trade: TradePrint) -> Option<ClusterFrame> {
        let trade_window = (trade.exchange_ts_ns / self.window_ns) * self.window_ns;

        let mut closing = None;
        match self.current_window_start_ns {
            None => {
                self.current_window_start_ns = Some(trade_window);
            }
            Some(curr) if trade_window > curr => {
                closing = Some(self.build_closing_snapshot());
                self.current_window_start_ns = Some(trade_window);
                self.accum.clear();
                self.dirty.clear();
                self.sequence = 0;
                self.last_diff_emit_ns = 0;
            }
            Some(curr) if trade_window < curr => {
                self.dropped_late_trades += 1;
                return None;
            }
            _ => {}
        }

        let bucket_price = (trade.price / self.bucket_step) * self.bucket_step;
        let acc = self.accum.entry(bucket_price).or_default();
        match trade.aggressor {
            AggressorSide::Bid => acc.bid_qty = acc.bid_qty.saturating_add(trade.qty),
            AggressorSide::Ask => acc.ask_qty = acc.ask_qty.saturating_add(trade.qty),
        }
        acc.trades = acc.trades.saturating_add(1);
        self.dirty.insert(bucket_price);

        closing
    }

    pub fn tick(&mut self, now_ns: i64) -> Option<ClusterFrame> {
        if self.dirty.is_empty() {
            return None;
        }
        if now_ns.saturating_sub(self.last_diff_emit_ns) < self.diff_interval_ns {
            return None;
        }
        let window_start_ns = self.current_window_start_ns?;

        self.sequence += 1;
        let upserts: Vec<ClusterBucket> = self
            .dirty
            .iter()
            .map(|&price| {
                let acc = &self.accum[&price];
                ClusterBucket {
                    price,
                    bid_qty: acc.bid_qty,
                    ask_qty: acc.ask_qty,
                    trades: acc.trades,
                }
            })
            .collect();
        self.dirty.clear();
        self.last_diff_emit_ns = now_ns;

        Some(ClusterFrame::Diff(Arc::new(AnalyticsDiff {
            window_start_ns,
            sequence: self.sequence,
            upserts,
            removes: Vec::new(),
        })))
    }

    /// Force-emit the current window as a snapshot, then drop state.
    /// Used at shutdown or when handing the symbol off to another node.
    pub fn flush(&mut self) -> Option<ClusterFrame> {
        if self.accum.is_empty() {
            return None;
        }
        let frame = self.build_closing_snapshot();
        self.accum.clear();
        self.dirty.clear();
        self.current_window_start_ns = None;
        self.sequence = 0;
        self.last_diff_emit_ns = 0;
        Some(frame)
    }

    fn build_closing_snapshot(&mut self) -> ClusterFrame {
        self.sequence += 1;
        let clusters: Vec<ClusterBucket> = self
            .accum
            .iter()
            .map(|(&price, acc)| ClusterBucket {
                price,
                bid_qty: acc.bid_qty,
                ask_qty: acc.ask_qty,
                trades: acc.trades,
            })
            .collect();
        ClusterFrame::Snapshot(Arc::new(AnalyticsSnapshot {
            window_start_ns: self.current_window_start_ns.unwrap_or(0),
            sequence: self.sequence,
            clusters,
        }))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use exchange_core::{Exchange, MarketType};

    use super::*;

    fn key() -> SymbolKey {
        SymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT")
    }

    fn agg() -> Aggregator {
        // bucket_step=10, window=60s, diff=100ms
        Aggregator::new(key(), 10, 60_000, 100)
    }

    fn trade(ts_ns: i64, price: i64, qty: i64, side: AggressorSide) -> TradePrint {
        TradePrint {
            exchange_ts_ns: ts_ns,
            aggressor: side,
            price,
            qty,
            trade_id: 0,
        }
    }

    fn snapshot_buckets(frame: &ClusterFrame) -> Vec<ClusterBucket> {
        match frame {
            ClusterFrame::Snapshot(s) => {
                let mut v = s.clusters.clone();
                v.sort_by_key(|b| b.price);
                v
            }
            ClusterFrame::Diff(_) => panic!("expected snapshot"),
        }
    }

    fn diff_buckets(frame: &ClusterFrame) -> Vec<ClusterBucket> {
        match frame {
            ClusterFrame::Diff(d) => {
                let mut v = d.upserts.clone();
                v.sort_by_key(|b| b.price);
                v
            }
            ClusterFrame::Snapshot(_) => panic!("expected diff"),
        }
    }

    #[test]
    fn first_trade_does_not_emit_and_buckets_correctly() {
        let mut a = agg();
        let r = a.ingest(trade(1_000, 12_345, 7, AggressorSide::Bid));
        assert!(r.is_none());
        let f = a.flush().unwrap();
        let buckets = snapshot_buckets(&f);
        assert_eq!(buckets.len(), 1);
        // 12345 / 10 * 10 = 12340
        assert_eq!(buckets[0].price, 12_340);
        assert_eq!(buckets[0].bid_qty, 7);
        assert_eq!(buckets[0].ask_qty, 0);
        assert_eq!(buckets[0].trades, 1);
    }

    #[test]
    fn trades_in_same_bucket_aggregate_per_side() {
        let mut a = agg();
        a.ingest(trade(1_000, 100, 5, AggressorSide::Bid));
        a.ingest(trade(2_000, 105, 3, AggressorSide::Bid));
        a.ingest(trade(3_000, 109, 9, AggressorSide::Ask));
        let f = a.flush().unwrap();
        let buckets = snapshot_buckets(&f);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].price, 100);
        assert_eq!(buckets[0].bid_qty, 8);
        assert_eq!(buckets[0].ask_qty, 9);
        assert_eq!(buckets[0].trades, 3);
    }

    #[test]
    fn window_roll_emits_closing_snapshot_with_old_window_state() {
        let mut a = agg();
        // Window 1: 0..60_000_000_000 ns
        a.ingest(trade(1_000_000, 100, 4, AggressorSide::Bid));
        a.ingest(trade(50_000_000_000, 110, 2, AggressorSide::Ask));
        // Trade in window 2 triggers close of window 1
        let closing = a
            .ingest(trade(60_000_000_001, 200, 1, AggressorSide::Bid))
            .expect("window roll must emit");
        match &closing {
            ClusterFrame::Snapshot(s) => {
                assert_eq!(s.window_start_ns, 0);
                assert_eq!(s.sequence, 1);
                let mut clusters = s.clusters.clone();
                clusters.sort_by_key(|b| b.price);
                assert_eq!(clusters.len(), 2);
                assert_eq!(clusters[0].price, 100);
                assert_eq!(clusters[0].bid_qty, 4);
                assert_eq!(clusters[1].price, 110);
                assert_eq!(clusters[1].ask_qty, 2);
            }
            _ => panic!("expected snapshot on roll"),
        }

        // After roll, only the new trade should be in state.
        let next = a.flush().unwrap();
        let buckets = snapshot_buckets(&next);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].price, 200);
        assert_eq!(buckets[0].bid_qty, 1);
    }

    #[test]
    fn out_of_order_late_trade_is_dropped() {
        let mut a = agg();
        a.ingest(trade(70_000_000_000, 100, 1, AggressorSide::Bid));
        let r = a.ingest(trade(10_000_000_000, 100, 99, AggressorSide::Bid));
        assert!(r.is_none());
        assert_eq!(a.dropped_late_trades(), 1);
        let f = a.flush().unwrap();
        let buckets = snapshot_buckets(&f);
        assert_eq!(buckets[0].bid_qty, 1, "late trade must not count");
    }

    #[test]
    fn tick_emits_only_dirty_buckets() {
        let mut a = agg();
        a.ingest(trade(1_000, 100, 5, AggressorSide::Bid));
        a.ingest(trade(2_000, 200, 3, AggressorSide::Ask));

        // First tick well after diff_interval — should emit both buckets.
        let frame = a.tick(1_000_000_000).expect("first tick must emit");
        let buckets = diff_buckets(&frame);
        assert_eq!(buckets.len(), 2);

        // Immediate next tick: no new dirty entries, must not emit.
        assert!(a.tick(1_000_000_001).is_none());

        // New trade in only one bucket; next tick after diff_interval emits only that one.
        a.ingest(trade(3_000, 100, 2, AggressorSide::Bid));
        let frame2 = a.tick(2_000_000_000).expect("second tick must emit");
        let buckets2 = diff_buckets(&frame2);
        assert_eq!(buckets2.len(), 1);
        assert_eq!(buckets2[0].price, 100);
        assert_eq!(buckets2[0].bid_qty, 7);
    }

    #[test]
    fn tick_respects_diff_interval() {
        let mut a = agg();
        a.ingest(trade(1_000, 100, 1, AggressorSide::Bid));
        // First tick at t=0: 0 - 0 = 0, NOT >= 100ms, must not emit.
        assert!(a.tick(0).is_none());
        // At 99ms: still under threshold.
        assert!(a.tick(99_000_000).is_none());
        // At 100ms: at threshold, must emit.
        assert!(a.tick(100_000_000).is_some());
    }

    #[test]
    fn sequence_resets_on_window_roll() {
        let mut a = agg();
        a.ingest(trade(1_000, 100, 1, AggressorSide::Bid));
        let _ = a.tick(200_000_000); // diff seq=1
        let _ = a.tick(400_000_000); // dirty empty -> None
        a.ingest(trade(1_000, 100, 1, AggressorSide::Ask));
        let _ = a.tick(600_000_000); // diff seq=2
        let closing = a
            .ingest(trade(60_000_000_001, 100, 1, AggressorSide::Bid))
            .unwrap();
        match closing {
            ClusterFrame::Snapshot(s) => assert_eq!(s.sequence, 3),
            _ => panic!(),
        }
        // Next emission in new window starts seq from 1.
        let next = a
            .ingest(trade(120_000_000_001, 100, 1, AggressorSide::Bid))
            .unwrap();
        match next {
            ClusterFrame::Snapshot(s) => assert_eq!(s.sequence, 1),
            _ => panic!(),
        }
    }
}
