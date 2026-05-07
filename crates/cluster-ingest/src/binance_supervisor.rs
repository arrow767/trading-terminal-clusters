use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clickhouse_sink::{rows_from_snapshot, ClusterRow};
use cluster_engine::{run_aggregator, Aggregator, ClusterBus};
use exchange_binance::BinanceFuturesWs;
use exchange_core::{
    ClusterFrame, Exchange, ExchangeInfo, MarketType, Quote, SymbolKey, SymbolSpec, VolumeRanker,
};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;

use crate::binance_session::{run_session, SymbolRoute};
use crate::config::{BinancePerpConfig, IngestConfig, RankBy};

/// One supervisor owns the entire pipeline for a single exchange:
/// REST `ExchangeInfo` polling, the WS session, the per-symbol
/// aggregator tasks, and the per-symbol fan-out into the ClickHouse
/// writer queue.
///
/// Supervisor lifecycle:
/// 1. `new` builds the struct without doing any I/O.
/// 2. `run` drives a discovery loop on `discovery_poll_secs` cadence.
///    Each iteration calls `reconcile_once`, which fetches the latest
///    symbol universe, diffs against currently-handled symbols, and
///    spawns/tears down per-symbol tasks. The new route set is then
///    published through a `watch` channel into the WS session task,
///    which cycles its connection so the new SUBSCRIBE list takes effect.
/// 3. On shutdown, every per-symbol channel is dropped (which makes the
///    aggregator drain and exit), the CH-fanout tasks are aborted, and
///    the WS session task observes the shutdown signal and stops.
///
/// Adding a new symbol that Binance just listed is therefore zero-touch
/// for the operator and zero-effect on already-running symbols beyond a
/// brief WS reconnect window. Adding a *new exchange* (e.g. Bybit) is
/// a different concern handled at deploy time — see the architecture doc
/// in CLAUDE memory.
pub struct BinanceSupervisor {
    info: Arc<dyn ExchangeInfo>,
    ranker: Option<Arc<dyn VolumeRanker>>,
    bus: Arc<ClusterBus>,
    region: String,
    ingest: IngestConfig,
    cfg: BinancePerpConfig,
    handles: Arc<RwLock<HashMap<SymbolKey, SymbolHandle>>>,
    routes_tx: watch::Sender<Vec<SymbolRoute>>,
    ch_tx: mpsc::Sender<ClusterRow>,
}

struct SymbolHandle {
    spec: SymbolSpec,
    trade_tx: mpsc::Sender<exchange_core::TradePrint>,
    aggregator: JoinHandle<()>,
    ch_fanout: JoinHandle<()>,
}

impl BinanceSupervisor {
    pub fn new(
        info: Arc<dyn ExchangeInfo>,
        ranker: Option<Arc<dyn VolumeRanker>>,
        bus: Arc<ClusterBus>,
        region: String,
        ingest: IngestConfig,
        cfg: BinancePerpConfig,
        ch_tx: mpsc::Sender<ClusterRow>,
    ) -> Self {
        let (routes_tx, _) = watch::channel(Vec::new());
        Self {
            info,
            ranker,
            bus,
            region,
            ingest,
            cfg,
            handles: Arc::new(RwLock::new(HashMap::new())),
            routes_tx,
            ch_tx,
        }
    }

    /// Drive discovery + WS session until `shutdown` flips to true.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let connector = BinanceFuturesWs::new();
        let routes_rx = self.routes_tx.subscribe();
        let session_shutdown = shutdown.clone();
        let ws_timeout = self.cfg.ws_connect_timeout();
        let backoff_min = self.cfg.backoff_min();
        let backoff_max = self.cfg.backoff_max();
        let session = tokio::spawn(async move {
            run_session_loop(
                connector,
                routes_rx,
                ws_timeout,
                backoff_min,
                backoff_max,
                session_shutdown,
            )
            .await;
        });

        // tokio::time::interval fires immediately on first tick → first
        // discovery happens right away.
        let mut tick = tokio::time::interval(self.cfg.discovery_poll());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if let Err(e) = self.reconcile_once().await {
                        tracing::warn!(error = %e, "supervisor: discovery cycle failed; will retry");
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
        }

        tracing::info!("supervisor: shutting down");
        let mut handles = self.handles.write().await;
        let mut joins = Vec::with_capacity(handles.len() * 2);
        for (_, handle) in handles.drain() {
            // Dropping the sender closes the mpsc; aggregator task
            // observes None on recv, flushes, and returns.
            drop(handle.trade_tx);
            handle.ch_fanout.abort();
            joins.push(handle.aggregator);
        }
        for j in joins {
            let _ = j.await;
        }
        let _ = session.await;
    }

    /// One discovery cycle. Public so tests can drive reconciliation
    /// deterministically without spinning up `run()`.
    pub async fn reconcile_once(&self) -> Result<()> {
        let specs = self
            .info
            .fetch_symbols()
            .await
            .map_err(|e| anyhow::anyhow!("fetch_symbols: {e}"))?;
        let filtered = filter_symbols(&specs, &self.cfg);
        let ranked = self.rank_specs(filtered).await;
        let wanted: Vec<SymbolSpec> = match self.cfg.top_n {
            Some(n) => ranked.into_iter().take(n).collect(),
            None => ranked,
        };
        let wanted_keys: HashSet<SymbolKey> = wanted
            .iter()
            .map(|s| SymbolKey::new(s.exchange, s.market_type, s.symbol.as_str()))
            .collect();

        let mut h = self.handles.write().await;

        let to_remove: Vec<SymbolKey> = h
            .keys()
            .filter(|k| !wanted_keys.contains(k))
            .cloned()
            .collect();
        for key in &to_remove {
            if let Some(handle) = h.remove(key) {
                drop(handle.trade_tx);
                handle.ch_fanout.abort();
                tracing::info!(symbol = %key.symbol, "supervisor: removed delisted symbol");
            }
        }

        let mut added = 0usize;
        for spec in &wanted {
            let key = SymbolKey::new(spec.exchange, spec.market_type, spec.symbol.as_str());
            if h.contains_key(&key) {
                continue;
            }
            let handle = self.spawn_symbol(spec.clone());
            h.insert(key, handle);
            added += 1;
        }
        if added > 0 {
            tracing::info!(added, total = h.len(), "supervisor: added new symbols");
        }

        // Publish updated routes; session task will drop+reconnect to
        // pick up the new subscription set. send_replace stores the new
        // value unconditionally — using send() would silently drop the
        // update during the brief window between supervisor construction
        // and the session task subscribing.
        let routes: Vec<SymbolRoute> = h
            .values()
            .map(|sh| SymbolRoute {
                spec: sh.spec.clone(),
                sink: sh.trade_tx.clone(),
            })
            .collect();
        self.routes_tx.send_replace(routes);

        Ok(())
    }

    /// Order the filtered symbol set per `cfg.rank_by`. On any failure
    /// of the volume fetch we log and fall through to the alphabetical
    /// fallback rather than dropping a discovery cycle entirely.
    async fn rank_specs(&self, mut specs: Vec<SymbolSpec>) -> Vec<SymbolSpec> {
        match self.cfg.rank_by {
            RankBy::Alphabetical => specs,
            RankBy::Volume24h => {
                let Some(ranker) = self.ranker.as_ref() else {
                    tracing::warn!(
                        "rank_by=volume_24h but no VolumeRanker provided; using alphabetical"
                    );
                    return specs;
                };
                let volumes = match ranker.fetch_24h_quote_volumes().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "fetch_24h_quote_volumes failed; using alphabetical fallback"
                        );
                        return specs;
                    }
                };
                specs.sort_by(|a, b| {
                    let va = volumes.get(&a.symbol).copied().unwrap_or(0.0);
                    let vb = volumes.get(&b.symbol).copied().unwrap_or(0.0);
                    vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
                });
                specs
            }
        }
    }

    fn spawn_symbol(&self, spec: SymbolSpec) -> SymbolHandle {
        let key = SymbolKey::new(spec.exchange, spec.market_type, spec.symbol.as_str());
        let bucket_step = spec.tick_size;
        let (trade_tx, trade_rx) = mpsc::channel(self.ingest.trade_channel_bound);
        let agg = Aggregator::new(
            key.clone(),
            bucket_step,
            self.ingest.window_ms,
            self.ingest.diff_interval_ms,
        );
        let bus_for_agg = Arc::clone(&self.bus);
        let tick = self.ingest.agg_tick_interval();
        let aggregator = tokio::spawn(async move {
            run_aggregator(agg, trade_rx, bus_for_agg, tick).await;
        });
        let ch_fanout = spawn_snapshot_to_ch(
            &self.bus,
            &key,
            spec.clone(),
            self.region.clone(),
            self.ch_tx.clone(),
        );
        SymbolHandle {
            spec,
            trade_tx,
            aggregator,
            ch_fanout,
        }
    }
}

/// Apply allow/deny/quote filters from config, but DO NOT apply
/// `top_n` — truncation runs after ranking in `BinanceSupervisor`.
pub fn filter_symbols(specs: &[SymbolSpec], cfg: &BinancePerpConfig) -> Vec<SymbolSpec> {
    let allow: Option<HashSet<&str>> = if cfg.allow.is_empty() {
        None
    } else {
        Some(cfg.allow.iter().map(String::as_str).collect())
    };
    let deny: HashSet<&str> = cfg.deny.iter().map(String::as_str).collect();
    let quotes: HashSet<Quote> = cfg
        .include_quotes
        .iter()
        .filter_map(|q| match q.as_str() {
            "USDT" => Some(Quote::Usdt),
            "USDC" => Some(Quote::Usdc),
            _ => None,
        })
        .collect();

    specs
        .iter()
        .filter(|s| {
            if s.exchange != Exchange::BinanceF || s.market_type != MarketType::Perp {
                return false;
            }
            if !quotes.contains(&s.quote) {
                return false;
            }
            if deny.contains(s.symbol.as_str()) {
                return false;
            }
            if let Some(a) = &allow {
                if !a.contains(s.symbol.as_str()) {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect()
}

async fn run_session_loop(
    connector: BinanceFuturesWs,
    mut routes_rx: watch::Receiver<Vec<SymbolRoute>>,
    connect_timeout: Duration,
    backoff_min: Duration,
    backoff_max: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut backoff = backoff_min;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let routes_snapshot = routes_rx.borrow().clone();
        if routes_snapshot.is_empty() {
            tokio::select! {
                _ = routes_rx.changed() => continue,
                _ = shutdown.changed() => {
                    if *shutdown.borrow() { break; }
                }
            }
            continue;
        }

        let session = run_session(&connector, &routes_snapshot, connect_timeout);
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            _ = routes_rx.changed() => {
                tracing::info!("session loop: routes changed; cycling connection");
            }
            result = session => {
                match result {
                    Ok(stats) => {
                        tracing::info!(?stats, "session ended; reconnecting");
                        backoff = backoff_min;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, ?backoff, "session error; backing off");
                        // Sleep is interruptible by shutdown.
                        tokio::select! {
                            _ = tokio::time::sleep(backoff) => {}
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() { break; }
                            }
                        }
                        backoff = (backoff * 2).min(backoff_max);
                    }
                }
            }
        }
    }
    tracing::info!("session loop: exiting");
}

fn spawn_snapshot_to_ch(
    bus: &Arc<ClusterBus>,
    key: &SymbolKey,
    spec: SymbolSpec,
    region: String,
    ch_tx: mpsc::Sender<ClusterRow>,
) -> JoinHandle<()> {
    let mut sub = bus.subscribe(key);
    tokio::spawn(async move {
        loop {
            match sub.recv().await {
                Ok(ClusterFrame::Snapshot(s)) => {
                    let rows = rows_from_snapshot(&s, &spec, &region);
                    for row in rows {
                        if ch_tx.send(row).await.is_err() {
                            return;
                        }
                    }
                }
                Ok(ClusterFrame::Diff(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(lagged = n, symbol = %spec.symbol, "ch fanout lagged behind bus");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use exchange_core::Quote;

    use super::*;

    fn spec(symbol: &str, quote: Quote) -> SymbolSpec {
        SymbolSpec {
            exchange: Exchange::BinanceF,
            market_type: MarketType::Perp,
            quote,
            symbol: symbol.into(),
            price_scale: 2,
            qty_scale: 3,
            tick_size: 10,
            step_size: 1,
        }
    }

    fn cfg() -> BinancePerpConfig {
        BinancePerpConfig {
            discovery_poll_secs: 60,
            ..Default::default()
        }
    }

    #[test]
    fn filter_keeps_usdt_perp_drops_busd() {
        let specs = vec![
            spec("BTCUSDT", Quote::Usdt),
            spec("ETHUSDC", Quote::Usdc),
            // Mark a non-perp by hand:
            SymbolSpec {
                market_type: MarketType::Spot,
                ..spec("BTCUSDT_SPOT", Quote::Usdt)
            },
        ];
        let kept = filter_symbols(&specs, &cfg());
        let symbols: Vec<&str> = kept.iter().map(|s| s.symbol.as_str()).collect();
        assert!(symbols.contains(&"BTCUSDT"));
        assert!(symbols.contains(&"ETHUSDC"));
        assert!(!symbols.contains(&"BTCUSDT_SPOT"));
    }

    #[test]
    fn filter_allow_list_is_authoritative() {
        let specs = vec![
            spec("BTCUSDT", Quote::Usdt),
            spec("ETHUSDT", Quote::Usdt),
            spec("DOGEUSDT", Quote::Usdt),
        ];
        let mut c = cfg();
        c.allow = vec!["BTCUSDT".into()];
        let kept = filter_symbols(&specs, &c);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].symbol, "BTCUSDT");
    }

    #[test]
    fn filter_deny_overrides_default() {
        let specs = vec![spec("BTCUSDT", Quote::Usdt), spec("ETHUSDT", Quote::Usdt)];
        let mut c = cfg();
        c.deny = vec!["BTCUSDT".into()];
        let kept = filter_symbols(&specs, &c);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].symbol, "ETHUSDT");
    }

    #[test]
    fn filter_does_not_apply_top_n() {
        // top_n is intentionally a supervisor concern (after ranking),
        // not a filter concern.
        let specs: Vec<_> = (0..10)
            .map(|i| spec(&format!("S{i}USDT"), Quote::Usdt))
            .collect();
        let mut c = cfg();
        c.top_n = Some(3);
        let kept = filter_symbols(&specs, &c);
        assert_eq!(kept.len(), 10);
    }

    /// Mock ExchangeInfo whose return value can be flipped between
    /// reconciliation runs.
    struct MockInfo {
        specs: Mutex<Vec<SymbolSpec>>,
    }
    #[async_trait]
    impl ExchangeInfo for MockInfo {
        async fn fetch_symbols(&self) -> exchange_core::Result<Vec<SymbolSpec>> {
            Ok(self.specs.lock().unwrap().clone())
        }
    }

    /// Mock VolumeRanker with a fixed map.
    struct MockRanker {
        map: std::collections::HashMap<String, f64>,
    }
    #[async_trait]
    impl VolumeRanker for MockRanker {
        async fn fetch_24h_quote_volumes(
            &self,
        ) -> exchange_core::Result<std::collections::HashMap<String, f64>> {
            Ok(self.map.clone())
        }
    }

    #[tokio::test]
    async fn reconcile_adds_new_and_drops_delisted() {
        let info = Arc::new(MockInfo {
            specs: Mutex::new(vec![
                spec("BTCUSDT", Quote::Usdt),
                spec("ETHUSDT", Quote::Usdt),
            ]),
        });
        let bus = Arc::new(ClusterBus::new());
        let (ch_tx, _ch_rx) = mpsc::channel(64);
        let supervisor = BinanceSupervisor::new(
            info.clone(),
            None,
            bus.clone(),
            "test".into(),
            IngestConfig::default(),
            cfg(),
            ch_tx,
        );

        // First cycle: 2 symbols, both spawn.
        supervisor.reconcile_once().await.unwrap();
        let initial: Vec<String> = supervisor
            .handles
            .read()
            .await
            .keys()
            .map(|k| k.symbol.to_string())
            .collect();
        let mut sorted = initial.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);

        // Routes channel got published.
        let routes = supervisor.routes_tx.borrow().clone();
        assert_eq!(routes.len(), 2);

        // Mutate the universe: drop ETHUSDT (delisted), add SOLUSDT (new listing).
        {
            let mut g = info.specs.lock().unwrap();
            g.clear();
            g.push(spec("BTCUSDT", Quote::Usdt));
            g.push(spec("SOLUSDT", Quote::Usdt));
        }
        supervisor.reconcile_once().await.unwrap();

        let final_set: Vec<String> = supervisor
            .handles
            .read()
            .await
            .keys()
            .map(|k| k.symbol.to_string())
            .collect();
        let mut sorted = final_set.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["BTCUSDT".to_string(), "SOLUSDT".to_string()]);
    }

    #[tokio::test]
    async fn rank_by_volume_picks_top_n_by_descending_volume() {
        let info = Arc::new(MockInfo {
            specs: Mutex::new(vec![
                spec("AAAUSDT", Quote::Usdt), // alphabetically first
                spec("BTCUSDT", Quote::Usdt),
                spec("ETHUSDT", Quote::Usdt),
                spec("DOGEUSDT", Quote::Usdt), // tiniest volume
            ]),
        });
        let ranker = Arc::new(MockRanker {
            map: [
                ("BTCUSDT".to_string(), 1_000_000_000.0),
                ("ETHUSDT".to_string(), 500_000_000.0),
                ("AAAUSDT".to_string(), 1_000.0),
                ("DOGEUSDT".to_string(), 100.0),
            ]
            .into_iter()
            .collect(),
        });
        let bus = Arc::new(ClusterBus::new());
        let (ch_tx, _ch_rx) = mpsc::channel(64);
        let mut c = cfg();
        c.rank_by = RankBy::Volume24h;
        c.top_n = Some(2);

        let supervisor = BinanceSupervisor::new(
            info,
            Some(Arc::clone(&ranker) as Arc<dyn VolumeRanker>),
            bus,
            "test".into(),
            IngestConfig::default(),
            c,
            ch_tx,
        );
        supervisor.reconcile_once().await.unwrap();

        let kept: Vec<String> = supervisor
            .handles
            .read()
            .await
            .keys()
            .map(|k| k.symbol.to_string())
            .collect();
        let mut sorted = kept.clone();
        sorted.sort();
        // Should keep the two highest-volume tickers, regardless of
        // alphabetical position in the source list.
        assert_eq!(sorted, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }
}
