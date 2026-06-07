use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clickhouse_sink::{ChWriter, ChWriterConfig};
use cluster_engine::ClusterBus;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

mod binance_session;
mod binance_supervisor;
mod bitget_session;
mod bybit_session;
mod config;
mod okx_session;

use binance_supervisor::{BinanceSupervisor, SessionFlavor};
use config::{table_name_for, Config};

#[tokio::main]
async fn main() -> Result<()> {
    ops::init_tracing("cluster-ingest");

    let cfg = match std::env::var("INGEST_CONFIG").ok().map(PathBuf::from) {
        Some(path) => {
            tracing::info!(path = %path.display(), "loading config");
            Config::load(&path)?
        }
        None => {
            tracing::info!("INGEST_CONFIG not set; using defaults");
            Config::default()
        }
    };

    // Env-var overrides for the most common dev knobs.
    let mut ingest = cfg.ingest;
    if let Ok(region) = std::env::var("INGEST_REGION") {
        ingest.region = region;
    }
    if let Ok(url) = std::env::var("CH_URL") {
        ingest.clickhouse_url = url;
    }
    let mut binance_cfg = ingest.exchanges.binance_perp.clone().unwrap_or_default();
    if let Ok(top_n_str) = std::env::var("INGEST_TOP_N") {
        if let Ok(n) = top_n_str.parse() {
            binance_cfg.top_n = Some(n);
        }
    }
    if let Ok(listen) = std::env::var("INGEST_GRPC_LISTEN") {
        ingest.grpc_listen = listen;
    }
    if let Ok(listen) = std::env::var("INGEST_REST_LISTEN") {
        ingest.rest_listen = listen;
    }

    // Bearer-токены: применяем env-overrides и валидируем — лучше упасть
    // на старте, чем потом обнаружить что сервис открыт всему интернету.
    ingest.auth.apply_env();
    ingest
        .auth
        .validate()
        .context("invalid [ingest.auth] config")?;
    let auth_state =
        cluster_api::AuthState::new(ingest.auth.tokens.clone(), ingest.auth.enabled);
    if auth_state.is_enabled() {
        tracing::info!(
            tokens = ingest.auth.tokens.len(),
            "bearer auth enabled for gRPC + REST"
        );
    } else {
        tracing::warn!("bearer auth DISABLED — сервис открыт; включи [ingest.auth].enabled в prod");
    }

    // Валидируем timeframes и retention заранее — если конфиг сломан,
    // лучше упасть на старте, чем после первого `ALTER`/`INSERT`.
    ingest
        .validate_timeframes()
        .context("invalid [ingest] timeframes_secs")?;
    ingest
        .retention
        .validate()
        .context("invalid [ingest.retention] config")?;

    let bus = Arc::new(ClusterBus::new());

    let grpc_addr: SocketAddr = ingest
        .grpc_listen
        .parse()
        .with_context(|| format!("parse grpc_listen: {}", ingest.grpc_listen))?;
    let rest_addr: SocketAddr = ingest
        .rest_listen
        .parse()
        .with_context(|| format!("parse rest_listen: {}", ingest.rest_listen))?;
    let grpc_bus = Arc::clone(&bus);
    let grpc_auth = auth_state.clone();
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = cluster_api::serve_with_auth(grpc_bus, grpc_addr, grpc_auth).await {
            tracing::error!(error = %e, "gRPC server crashed");
        }
    });

    // REST: /v1/system/metrics + /health + /v1/clusters/range. Один
    // reqwest-клиент шейрится между sysmetrics (table-breakdown) и
    // cluster_history (выдача баров терминалу). Pool keep-alive снизит
    // overhead при частых /range запросах от UI.
    let ch_http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .pool_max_idle_per_host(8)
        .build()
        .context("build reqwest client for REST→CH")?;

    let sysmetrics_state = cluster_api::SysMetricsState {
        ch_client: Some(ch_http.clone()),
        ch_url: ingest.clickhouse_url.clone(),
        ch_database: ingest.clickhouse_database.clone(),
    };
    let history_state = cluster_api::ClusterHistoryState {
        ch_client: ch_http,
        ch_url: ingest.clickhouse_url.clone(),
        ch_database: ingest.clickhouse_database.clone(),
        // Пока пишем как default-пользователь — наш ingest подключается
        // тем же способом (sink также без auth). Если позже поднимем
        // отдельного `cluster`-юзера с паролем — пробросим через env
        // (CH_USER/CH_PASSWORD).
        ch_user: std::env::var("CH_USER").unwrap_or_default(),
        ch_password: std::env::var("CH_PASSWORD").unwrap_or_default(),
    };
    let rest_auth = auth_state.clone();
    let rest_handle = tokio::spawn(async move {
        if let Err(e) =
            cluster_api::serve_rest(rest_addr, sysmetrics_state, history_state, rest_auth).await
        {
            tracing::error!(error = %e, "REST server crashed");
        }
    });

    // По одному ChWriter на каждый TF — пишет в свою таблицу `clusters_<tf>`.
    // Канал по таймфрейму отдельный, чтобы один медленный TF не давил
    // на остальные через общий bound.
    let mut ch_tx_by_tf: HashMap<u32, mpsc::Sender<clickhouse_sink::ClusterRow>> = HashMap::new();
    let mut writer_handles: Vec<JoinHandle<()>> = Vec::new();
    for &tf_secs in &ingest.timeframes_secs {
        let (tx, rx) = mpsc::channel(ingest.ch_channel_bound);
        let table = table_name_for(tf_secs);
        let writer = ChWriter::new(ChWriterConfig {
            url: ingest.clickhouse_url.clone(),
            database: ingest.clickhouse_database.clone(),
            table: table.clone(),
            ..ChWriterConfig::default()
        })
        .with_context(|| format!("build ChWriter for {table}"))?;

        // Apply retention для этой таблицы. Failure non-fatal: ingest
        // продолжит писать, TTL просто останется прежним (видно по
        // /v1/system/metrics → table-breakdown).
        if ingest.retention.apply_on_start {
            if let Some(clause) = ingest.retention.ttl_clause_for(tf_secs) {
                let sql = format!(
                    "ALTER TABLE {}.{} MODIFY TTL {}",
                    ingest.clickhouse_database, table, clause
                );
                tracing::info!(%table, %sql, "applying retention TTL");
                if let Err(e) = writer.execute_ddl(&sql).await {
                    tracing::warn!(
                        error = %e, %table,
                        "MODIFY TTL failed; ingest will continue with existing TTL"
                    );
                } else {
                    tracing::info!(
                        %table,
                        interval_seconds = tf_secs,
                        "retention TTL applied"
                    );
                }
            } else {
                tracing::info!(
                    %table,
                    "retention для этой TF отключена целиком — TTL не трогаю"
                );
            }
        }

        let handle = tokio::spawn(async move {
            match writer.run(rx).await {
                Ok(stats) => tracing::info!(?stats, %table, "ch writer ended"),
                Err(e) => tracing::error!(error = %e, %table, "ch writer crashed"),
            }
        });
        writer_handles.push(handle);
        ch_tx_by_tf.insert(tf_secs, tx);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut supervisor_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // ─── Binance USD-M perp ─────────────────────────────────────────────
    let binance_perp_enabled = ingest.is_exchange_enabled("binance_perp", binance_cfg.enabled);
    if binance_perp_enabled {
        // One Arc, two trait views: ExchangeInfo for symbol discovery,
        // VolumeRanker for top-N ordering. Sharing the underlying
        // BinanceFuturesInfo keeps the reqwest client (with its
        // connection pool) shared between both endpoints.
        let raw = Arc::new(exchange_binance::BinanceFuturesInfo::new());
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> =
            Arc::new(exchange_binance::BinanceFuturesWs::new());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Binance,
            exchange_core::Exchange::BinanceF,
            exchange_core::MarketType::Perp,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            binance_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: binance_perp");
    } else {
        tracing::info!("binance_perp not in enabled_exchanges; skipped");
    }

    // ─── Binance spot ───────────────────────────────────────────────────
    // Тот же supervisor-движок, что и для perp — отличается только
    // ExchangeInfo (api.binance.com) + WsConnector (stream.binance.com:9443).
    // Конфиг секции [ingest.exchanges.binance_spot]: если не задана →
    // используем дефолты BinancePerpConfig (по умолчанию enabled=true).
    let binance_spot_cfg = ingest
        .exchanges
        .binance_spot
        .clone()
        .unwrap_or_default();
    let binance_spot_enabled = ingest.is_exchange_enabled("binance_spot", binance_spot_cfg.enabled);
    if binance_spot_enabled {
        let raw = Arc::new(exchange_binance::BinanceSpotInfo::new());
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> =
            Arc::new(exchange_binance::BinanceSpotWs::new());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Binance,
            exchange_core::Exchange::Binance,
            exchange_core::MarketType::Spot,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            binance_spot_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: binance_spot");
    } else {
        tracing::info!("binance_spot not in enabled_exchanges; skipped");
    }

    // ─── Bybit linear (USDT/USDC perps) ─────────────────────────────────
    // BTCUSDT (USDT-linear) + BTCPERP (USDC-linear) — оба покрываются
    // одним endpoint'ом /v5/public/linear и одним instruments-info запросом.
    let bybit_perp_cfg = ingest
        .exchanges
        .bybit_perp
        .clone()
        .unwrap_or_default();
    if ingest.is_exchange_enabled("bybit_perp", bybit_perp_cfg.enabled) {
        let raw = Arc::new(exchange_bybit::BybitInstrumentsInfo::new(
            exchange_bybit::BybitCategory::Linear,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_bybit::BybitWs::linear());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Bybit,
            exchange_core::Exchange::BybitF,
            exchange_core::MarketType::Perp,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            bybit_perp_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: bybit_perp");
    } else {
        tracing::info!("bybit_perp not in enabled_exchanges; skipped");
    }

    // ─── Bybit spot ─────────────────────────────────────────────────────
    let bybit_spot_cfg = ingest
        .exchanges
        .bybit_spot
        .clone()
        .unwrap_or_default();
    if ingest.is_exchange_enabled("bybit_spot", bybit_spot_cfg.enabled) {
        let raw = Arc::new(exchange_bybit::BybitInstrumentsInfo::new(
            exchange_bybit::BybitCategory::Spot,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_bybit::BybitWs::spot());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Bybit,
            exchange_core::Exchange::Bybit,
            exchange_core::MarketType::Spot,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            bybit_spot_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: bybit_spot");
    } else {
        tracing::info!("bybit_spot not in enabled_exchanges; skipped");
    }

    // ─── OKX linear swaps (USDT/USDC perps) ─────────────────────────────
    // Один публичный WS endpoint на spot+swap; instId BTC-USDT-SWAP. Своп-qty
    // в контрактах → база через ctVal внутри exchange-okx (см. scale::set_ct).
    let okx_perp_cfg = ingest.exchanges.okx_perp.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("okx_perp", okx_perp_cfg.enabled) {
        let raw = Arc::new(exchange_okx::OkxInstrumentsInfo::new(
            exchange_okx::OkxCategory::Swap,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_okx::OkxWs::swap());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Okx,
            exchange_core::Exchange::OkxF,
            exchange_core::MarketType::Perp,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            okx_perp_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: okx_perp");
    } else {
        tracing::info!("okx_perp not in enabled_exchanges; skipped");
    }

    // ─── OKX spot ───────────────────────────────────────────────────────
    let okx_spot_cfg = ingest.exchanges.okx_spot.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("okx_spot", okx_spot_cfg.enabled) {
        let raw = Arc::new(exchange_okx::OkxInstrumentsInfo::new(
            exchange_okx::OkxCategory::Spot,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_okx::OkxWs::spot());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Okx,
            exchange_core::Exchange::Okx,
            exchange_core::MarketType::Spot,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            okx_spot_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: okx_spot");
    } else {
        tracing::info!("okx_spot not in enabled_exchanges; skipped");
    }

    // ─── Bitget USDT-margined linear perps ──────────────────────────────
    // Один публичный WS endpoint на spot+perp; instId канон BTCUSDT. Линейные
    // фьючи — qty уже в базе, контракт-множитель не нужен (в отличие от OKX).
    let bitget_perp_cfg = ingest.exchanges.bitget_perp.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("bitget_perp", bitget_perp_cfg.enabled) {
        let raw = Arc::new(exchange_bitget::BitgetInstrumentsInfo::new(
            exchange_bitget::BitgetCategory::Perp,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_bitget::BitgetWs::perp());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Bitget,
            exchange_core::Exchange::BitgetF,
            exchange_core::MarketType::Perp,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            bitget_perp_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: bitget_perp");
    } else {
        tracing::info!("bitget_perp not in enabled_exchanges; skipped");
    }

    // ─── Bitget spot ────────────────────────────────────────────────────
    let bitget_spot_cfg = ingest.exchanges.bitget_spot.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("bitget_spot", bitget_spot_cfg.enabled) {
        let raw = Arc::new(exchange_bitget::BitgetInstrumentsInfo::new(
            exchange_bitget::BitgetCategory::Spot,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_bitget::BitgetWs::spot());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Bitget,
            exchange_core::Exchange::Bitget,
            exchange_core::MarketType::Spot,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            bitget_spot_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: bitget_spot");
    } else {
        tracing::info!("bitget_spot not in enabled_exchanges; skipped");
    }

    // ─── Aster linear perps (asterdex.com) ──────────────────────────────
    // Клон combined-stream Binance → переиспользуем SessionFlavor::Binance
    // (binance_session + BinanceFuturesTradeParser парсят Aster as-is).
    let aster_perp_cfg = ingest.exchanges.aster_perp.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("aster_perp", aster_perp_cfg.enabled) {
        let raw = Arc::new(exchange_aster::AsterInstrumentsInfo::new(
            exchange_aster::AsterCategory::Perp,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_aster::AsterWs::futures());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Binance,
            exchange_core::Exchange::AsterF,
            exchange_core::MarketType::Perp,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            aster_perp_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: aster_perp");
    } else {
        tracing::info!("aster_perp not in enabled_exchanges; skipped");
    }

    // ─── Aster spot ─────────────────────────────────────────────────────
    let aster_spot_cfg = ingest.exchanges.aster_spot.clone().unwrap_or_default();
    if ingest.is_exchange_enabled("aster_spot", aster_spot_cfg.enabled) {
        let raw = Arc::new(exchange_aster::AsterInstrumentsInfo::new(
            exchange_aster::AsterCategory::Spot,
        ));
        let info: Arc<dyn exchange_core::ExchangeInfo> = Arc::clone(&raw) as _;
        let ranker: Option<Arc<dyn exchange_core::VolumeRanker>> = Some(Arc::clone(&raw) as _);
        let connector: Arc<dyn exchange_core::WsConnector> = Arc::new(exchange_aster::AsterWs::spot());
        let supervisor = BinanceSupervisor::new(
            info,
            ranker,
            connector,
            SessionFlavor::Binance,
            exchange_core::Exchange::Aster,
            exchange_core::MarketType::Spot,
            Arc::clone(&bus),
            ingest.region.clone(),
            ingest.clone(),
            aster_spot_cfg,
            ch_tx_by_tf.clone(),
        );
        let shutdown_for_sup = shutdown_rx.clone();
        supervisor_tasks.push(tokio::spawn(async move {
            supervisor.run(shutdown_for_sup).await;
        }));
        tracing::info!("supervisor started: aster_spot");
    } else {
        tracing::info!("aster_spot not in enabled_exchanges; skipped");
    }

    // Дропаем локальные клоны TX-каналов, чтобы при shutdown'е supervisor'а
    // (когда его собственные клоны тоже дропнутся) writer'ы увидели close
    // и завершились корректно.
    drop(ch_tx_by_tf);

    // Раньше main селективился на одном writer_handle, чтобы заметить
    // его падение. С N writer'ами select_all усложняет код без выгоды:
    // упавший writer и так логируется внутри своей spawn-блока, а exit-
    // сигнал у нас есть (ctrl_c).
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("ctrl-c received; signalling shutdown");

    let _ = shutdown_tx.send(true);
    for t in supervisor_tasks {
        let _ = t.await;
    }
    grpc_handle.abort();
    rest_handle.abort();
    for h in writer_handles {
        h.abort();
    }

    Ok(())
}
