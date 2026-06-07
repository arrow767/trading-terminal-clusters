use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clickhouse_sink::{rows_from_snapshot, ClusterRow};
use cluster_engine::{run_aggregator, Aggregator, ClusterBus};
use exchange_core::{
    ClusterFrame, Exchange, ExchangeInfo, MarketType, Quote, StreamKey, SymbolKey, SymbolSpec,
    TradePrint, VolumeRanker, WsConnector,
};
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::JoinHandle;

use crate::aster_session::run_session as run_aster_session;
use crate::binance_session::{run_session as run_binance_session, SymbolRoute};
use crate::bitget_session::run_session as run_bitget_session;
use crate::kucoin_session::run_session as run_kucoin_session;
use crate::mexc_session::run_session as run_mexc_session;
use crate::bybit_session::run_session as run_bybit_session;
use crate::config::{BinancePerpConfig, IngestConfig, RankBy};
use crate::okx_session::run_session as run_okx_session;

/// Какой session-runner использовать для данного supervisor'а.
///
/// Каждый exchange-протокол отличается по подписке/heartbeat'у/envelope'у
/// трейдов (см. screener_exchange_quirks). Один общий `run_session` через
/// трейт-объект сделать в Rust трудно из-за async-fn-in-trait + специфики
/// payload-форматов, поэтому диспатчим через простой enum здесь.
///
/// При добавлении новой биржи: добавь вариант + новый `xxx_session.rs`
/// модуль с `pub async fn run_session(connector: &XxxWs, routes, timeout)`,
/// и match-arm в `run_session_loop`.
#[derive(Clone, Copy)]
pub enum SessionFlavor {
    Binance,
    Bybit,
    Okx,
    Bitget,
    Aster,
    Kucoin,
    Mexc,
}

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
    /// WS-коннектор. Inject'ится снаружи (BinanceFuturesWs / BinanceSpotWs /
    /// BybitWs / ...) — иначе supervisor хардкодился бы под futures, и
    /// spot/новые биржи требовали бы дублирования файла.
    connector: Arc<dyn WsConnector>,
    /// Какую `*_session::run_session` функцию использовать. Per-exchange
    /// payload-форматы достаточно расходятся (Bybit batched + client-ping
    /// vs Binance single-trade + server-ping), чтобы делать общий runner
    /// сейчас — не стоит. Enum-dispatch достаточно.
    session_flavor: SessionFlavor,
    /// Exchange + market_type, под которые этот supervisor фильтрует
    /// universe. Поле — single-source-of-truth: filter_symbols сверяется
    /// именно с этой парой, чтобы один supervisor никогда не подхватил
    /// futures-данные в spot-pipeline (и наоборот).
    exchange: Exchange,
    market_type: MarketType,
    bus: Arc<ClusterBus>,
    region: String,
    ingest: IngestConfig,
    cfg: BinancePerpConfig,
    handles: Arc<RwLock<HashMap<SymbolKey, SymbolHandle>>>,
    routes_tx: watch::Sender<Vec<SymbolRoute>>,
    /// Один writer-channel на каждый TF. Сборка в main.rs; supervisor
    /// раздаёт клоны конкретных tx-каналов своим per-symbol per-TF fanout'ам.
    /// Key = interval_seconds (как в `timeframes_secs`).
    ch_tx_by_tf: Arc<HashMap<u32, mpsc::Sender<ClusterRow>>>,
}

struct SymbolHandle {
    spec: SymbolSpec,
    /// Куда session-loop пишет трейды этого символа.
    trade_tx: mpsc::Sender<TradePrint>,
    /// Per-symbol fanout: один tx сверху → N TF-каналов внутри.
    fanout: JoinHandle<()>,
    /// По одному аггрегатору на каждый таймфрейм.
    aggregators: Vec<JoinHandle<()>>,
    /// По одному CH-fanout'у (bus → ClusterRow writer mpsc) на каждый TF.
    ch_fanouts: Vec<JoinHandle<()>>,
}

impl BinanceSupervisor {
    pub fn new(
        info: Arc<dyn ExchangeInfo>,
        ranker: Option<Arc<dyn VolumeRanker>>,
        connector: Arc<dyn WsConnector>,
        session_flavor: SessionFlavor,
        exchange: Exchange,
        market_type: MarketType,
        bus: Arc<ClusterBus>,
        region: String,
        ingest: IngestConfig,
        cfg: BinancePerpConfig,
        ch_tx_by_tf: HashMap<u32, mpsc::Sender<ClusterRow>>,
    ) -> Self {
        let (routes_tx, _) = watch::channel(Vec::new());
        Self {
            info,
            ranker,
            connector,
            session_flavor,
            exchange,
            market_type,
            bus,
            region,
            ingest,
            cfg,
            handles: Arc::new(RwLock::new(HashMap::new())),
            routes_tx,
            ch_tx_by_tf: Arc::new(ch_tx_by_tf),
        }
    }

    /// Drive discovery + WS session until `shutdown` flips to true.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        let connector = Arc::clone(&self.connector);
        let routes_rx = self.routes_tx.subscribe();
        let session_shutdown = shutdown.clone();
        let ws_timeout = self.cfg.ws_connect_timeout();
        let backoff_min = self.cfg.backoff_min();
        let backoff_max = self.cfg.backoff_max();
        let flavor = self.session_flavor;
        let session = tokio::spawn(async move {
            run_session_loop(
                connector,
                flavor,
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
        let mut joins: Vec<JoinHandle<()>> = Vec::new();
        for (_, handle) in handles.drain() {
            // Dropping session->fanout sender кидает domino:
            // fanout видит None → дропает per-TF senders → каждый
            // aggregator видит None → flush'ит финальный кадр в bus → bus
            // отдаёт его ch_fanout subscriber'у. ch_fanouts мы аборт'им
            // отдельно ниже — без abort'а они зависнут на bus.recv ().
            drop(handle.trade_tx);
            for ch_fan in handle.ch_fanouts {
                ch_fan.abort();
            }
            joins.push(handle.fanout);
            joins.extend(handle.aggregators);
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
        let filtered = filter_symbols(&specs, &self.cfg, self.exchange, self.market_type);
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
                for ch_fan in handle.ch_fanouts {
                    ch_fan.abort();
                }
                // fanout + aggregators самозавершатся по domino-эффекту
                // (см. shutdown-комментарий). Здесь не ждём — это горячий
                // путь reconcile, блокировать его await'ом дорого.
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
        let symbol_key = SymbolKey::new(spec.exchange, spec.market_type, spec.symbol.as_str());
        let bucket_step = spec.tick_size;
        let tick = self.ingest.agg_tick_interval();
        let diff_ms = self.ingest.diff_interval_ms;
        let trade_bound = self.ingest.trade_channel_bound;

        // По одному аггрегатору и одному CH-fanout'у на каждый TF.
        let mut tf_trade_txs: Vec<mpsc::Sender<TradePrint>> = Vec::new();
        let mut aggregators: Vec<JoinHandle<()>> = Vec::new();
        let mut ch_fanouts: Vec<JoinHandle<()>> = Vec::new();

        for &tf_secs in &self.ingest.timeframes_secs {
            let (tf_tx, tf_rx) = mpsc::channel(trade_bound);
            tf_trade_txs.push(tf_tx);

            let window_ms = (tf_secs as i64) * 1000;
            let agg = Aggregator::new(symbol_key.clone(), bucket_step, window_ms, diff_ms);
            let stream_key = StreamKey::new(symbol_key.clone(), tf_secs);
            let bus_for_agg = Arc::clone(&self.bus);
            let stream_key_for_agg = stream_key.clone();
            let agg_handle = tokio::spawn(async move {
                run_aggregator(agg, stream_key_for_agg, tf_rx, bus_for_agg, tick).await;
            });
            aggregators.push(agg_handle);

            // CH-fanout: подписываемся на bus для этой пары (symbol, tf),
            // и пушим строки в writer-канал именно этой TF.
            let ch_tx = self
                .ch_tx_by_tf
                .get(&tf_secs)
                .cloned()
                .expect("invariant: ch_tx_by_tf has all configured timeframes");
            let ch_fan = spawn_snapshot_to_ch(
                &self.bus,
                stream_key,
                spec.clone(),
                self.region.clone(),
                ch_tx,
            );
            ch_fanouts.push(ch_fan);
        }

        // Per-symbol trade fanout: получает трейд от session, раздаёт во
        // все per-TF mpsc-каналы. `try_send` — чтобы ОДИН медленный TF
        // не блокировал быстрые. На `Full` ведём per-TF счётчик: ранее
        // дроп был тихим, что приводило к незаметному расхождению данных
        // между TF (1m видит трейды, 5m их теряет). Теперь логируем
        // нарастающую сумму с rate-limit'ом (раз в 5с per TF), чтобы
        // оператор/диагностика сразу видели если канал недостаточен.
        let (session_tx, mut session_rx) = mpsc::channel(trade_bound);
        let symbol_for_log = spec.symbol.clone();
        let tfs_for_log: Vec<u32> = self.ingest.timeframes_secs.clone();
        let fanout = tokio::spawn(async move {
            // Параллельные счётчики: drops[i] относится к tf_trade_txs[i]
            // → tfs_for_log[i]. Локальные (не Atomic'и) — fanout single-task.
            let mut drops_full: Vec<u64> = vec![0; tf_trade_txs.len()];
            let mut last_warn_ms: Vec<i64> = vec![0; tf_trade_txs.len()];
            while let Some(trade) = session_rx.recv().await {
                for (i, tx) in tf_trade_txs.iter().enumerate() {
                    match tx.try_send(trade) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // TF-aggregator мёртв, дальше тоже мимо пройдёт.
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            drops_full[i] = drops_full[i].saturating_add(1);
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as i64)
                                .unwrap_or(0);
                            // rate-limit: один warn раз в 5с на TF.
                            if now_ms - last_warn_ms[i] >= 5_000 {
                                tracing::warn!(
                                    symbol = %symbol_for_log,
                                    interval_seconds = tfs_for_log[i],
                                    total_drops = drops_full[i],
                                    "fanout: trade DROPPED — per-TF channel full \
                                     (увеличь ingest.trade_channel_bound или \
                                     ускорь aggregator/sink)"
                                );
                                last_warn_ms[i] = now_ms;
                            }
                        }
                    }
                }
            }
            tracing::debug!(symbol = %symbol_for_log, ?drops_full, "trade fanout exiting");
            // tf_trade_txs дропнутся вместе с этим scope → aggregators
            // увидят close → flush + exit.
        });

        SymbolHandle {
            spec,
            trade_tx: session_tx,
            fanout,
            aggregators,
            ch_fanouts,
        }
    }
}

/// Apply allow/deny/quote filters from config, but DO NOT apply
/// `top_n` — truncation runs after ranking in `BinanceSupervisor`.
/// `exchange`/`market_type` — пара, под которую supervisor работает; всё
/// что не из этой пары — отбрасывается (futures-supervisor никогда не
/// возьмёт spot-листинг и наоборот).
pub fn filter_symbols(
    specs: &[SymbolSpec],
    cfg: &BinancePerpConfig,
    exchange: Exchange,
    market_type: MarketType,
) -> Vec<SymbolSpec> {
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
            if s.exchange != exchange || s.market_type != market_type {
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
    connector: Arc<dyn WsConnector>,
    flavor: SessionFlavor,
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

        // Per-exchange session entry. Trade-envelope, ping policy и
        // subscribe-batching различаются между биржами — общим runner'ом
        // делать без потери прозрачности уже не получается. См.
        // `SessionFlavor` для гайда по добавлению новой биржи.
        let session: std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<crate::binance_session::SessionStats>> + Send>> =
            match flavor {
                SessionFlavor::Binance => Box::pin(run_binance_session(
                    connector.as_ref(),
                    &routes_snapshot,
                    connect_timeout,
                )),
                SessionFlavor::Bybit => {
                    // Bybit connector обязан быть BybitWs — этот инвариант
                    // следит SessionFlavor в `new()`. Downcast через Any не
                    // нужен: BybitWs реализует WsConnector, а run_bybit_session
                    // принимает &BybitWs — так что мы downcast'им через as_any.
                    // Чтобы не тащить Any в trait WsConnector, идём проще:
                    // полагаемся на корректность вызова из main.rs
                    // (тип flavor=Bybit ⇒ connector=BybitWs).
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_bybit::BybitWs>()
                        .expect("SessionFlavor::Bybit requires BybitWs connector");
                    Box::pin(run_bybit_session(ws, &routes_snapshot, connect_timeout))
                }
                SessionFlavor::Okx => {
                    // Инвариант как у Bybit: flavor=Okx ⇒ connector=OkxWs
                    // (гарантируется конструированием в main.rs).
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_okx::OkxWs>()
                        .expect("SessionFlavor::Okx requires OkxWs connector");
                    Box::pin(run_okx_session(ws, &routes_snapshot, connect_timeout))
                }
                SessionFlavor::Bitget => {
                    // Инвариант: flavor=Bitget ⇒ connector=BitgetWs.
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_bitget::BitgetWs>()
                        .expect("SessionFlavor::Bitget requires BitgetWs connector");
                    Box::pin(run_bitget_session(ws, &routes_snapshot, connect_timeout))
                }
                SessionFlavor::Aster => {
                    // Инвариант: flavor=Aster ⇒ connector=AsterWs.
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_aster::AsterWs>()
                        .expect("SessionFlavor::Aster requires AsterWs connector");
                    Box::pin(run_aster_session(ws, &routes_snapshot, connect_timeout))
                }
                SessionFlavor::Kucoin => {
                    // Инвариант: flavor=Kucoin ⇒ connector=KucoinWs.
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_kucoin::KucoinWs>()
                        .expect("SessionFlavor::Kucoin requires KucoinWs connector");
                    Box::pin(run_kucoin_session(ws, &routes_snapshot, connect_timeout))
                }
                SessionFlavor::Mexc => {
                    // Инвариант: flavor=Mexc ⇒ connector=MexcWs.
                    let ws = connector
                        .as_any()
                        .downcast_ref::<exchange_mexc::MexcWs>()
                        .expect("SessionFlavor::Mexc requires MexcWs connector");
                    Box::pin(run_mexc_session(ws, &routes_snapshot, connect_timeout))
                }
            };
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
                        tracing::info!(?stats, "session ended; reconnecting after min backoff");
                        // Ждём минимальный backoff даже на «чистом» закрытии.
                        // Binance закрывает сокет каждые 24h и при route-cycle —
                        // без этой паузы мы могли бы войти в reconnect-loop
                        // если REST-уровень (например subscribe_payload) сразу
                        // фейлится после открытия. Шаг маленький (по умолчанию
                        // 500мс), так что user-perceived latency не страдает.
                        tokio::select! {
                            _ = tokio::time::sleep(backoff_min) => {}
                            _ = shutdown.changed() => {
                                if *shutdown.borrow() { break; }
                            }
                        }
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
    stream_key: StreamKey,
    spec: SymbolSpec,
    region: String,
    ch_tx: mpsc::Sender<ClusterRow>,
) -> JoinHandle<()> {
    let mut sub = bus.subscribe(&stream_key);
    let interval = stream_key.interval_seconds;
    tokio::spawn(async move {
        // Tracking: lag-flapping (когда bus стабильно отстаёт — ChWriter
        // не справляется). Логируем нарастающий total, чтобы оператор
        // видел реальный масштаб потерь, а не только последний всплеск.
        let mut total_lagged: u64 = 0;
        loop {
            match sub.recv().await {
                Ok(ClusterFrame::Snapshot(s)) => {
                    let rows = rows_from_snapshot(&s, &spec, &region);
                    for row in rows {
                        // send().await блокирует если ch_tx полный — это
                        // штатный backpressure: лучше подождать, чем
                        // дропать ряды. Если ch_tx стабильно полный,
                        // bus начнёт лагать → сработает ветка `Lagged`
                        // ниже, оператор увидит warn.
                        if ch_tx.send(row).await.is_err() {
                            return;
                        }
                    }
                }
                Ok(ClusterFrame::Diff(_)) => {
                    // Diff (live-cluster gRPC stream) в CH не пишем —
                    // только Snapshot. Diff'ы только для realtime подписчиков.
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    total_lagged = total_lagged.saturating_add(n);
                    tracing::warn!(
                        lagged = n,
                        total_lagged,
                        symbol = %spec.symbol,
                        interval_seconds = interval,
                        "ch fanout LAGGED — frames dropped (увеличь bus capacity \
                         или ускорь ChWriter / увеличь ch_channel_bound)"
                    );
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
    use exchange_binance::BinanceFuturesWs;
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
        let kept = filter_symbols(&specs, &cfg(), Exchange::BinanceF, MarketType::Perp);
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
        let kept = filter_symbols(&specs, &c, Exchange::BinanceF, MarketType::Perp);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].symbol, "BTCUSDT");
    }

    #[test]
    fn filter_deny_overrides_default() {
        let specs = vec![spec("BTCUSDT", Quote::Usdt), spec("ETHUSDT", Quote::Usdt)];
        let mut c = cfg();
        c.deny = vec!["BTCUSDT".into()];
        let kept = filter_symbols(&specs, &c, Exchange::BinanceF, MarketType::Perp);
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
        let kept = filter_symbols(&specs, &c, Exchange::BinanceF, MarketType::Perp);
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

    /// Тест-хелпер: построить ch_tx_by_tf по списку timeframes из конфига.
    /// Возвращает Map + Vec приёмников (чтобы они не дропнулись и каналы
    /// не закрылись, пока тест работает).
    fn build_ch_txs(ingest: &IngestConfig) -> (HashMap<u32, mpsc::Sender<ClusterRow>>, Vec<mpsc::Receiver<ClusterRow>>) {
        let mut txs = HashMap::new();
        let mut rxs = Vec::new();
        for &tf in &ingest.timeframes_secs {
            let (tx, rx) = mpsc::channel(64);
            txs.insert(tf, tx);
            rxs.push(rx);
        }
        (txs, rxs)
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
        let ingest = IngestConfig::default();
        let (ch_txs, _ch_rxs) = build_ch_txs(&ingest);
        let supervisor = BinanceSupervisor::new(
            info.clone(),
            None,
            Arc::new(BinanceFuturesWs::new()) as Arc<dyn WsConnector>,
            SessionFlavor::Binance,
            Exchange::BinanceF,
            MarketType::Perp,
            bus.clone(),
            "test".into(),
            ingest,
            cfg(),
            ch_txs,
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
        let ingest = IngestConfig::default();
        let (ch_txs, _ch_rxs) = build_ch_txs(&ingest);
        let mut c = cfg();
        c.rank_by = RankBy::Volume24h;
        c.top_n = Some(2);

        let supervisor = BinanceSupervisor::new(
            info,
            Some(Arc::clone(&ranker) as Arc<dyn VolumeRanker>),
            Arc::new(BinanceFuturesWs::new()) as Arc<dyn WsConnector>,
            SessionFlavor::Binance,
            Exchange::BinanceF,
            MarketType::Perp,
            bus,
            "test".into(),
            ingest,
            c,
            ch_txs,
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
