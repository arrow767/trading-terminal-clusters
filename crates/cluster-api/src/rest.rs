//! Тонкий axum-роутер: health-пробы + /v1/system/metrics. Auth ставится
//! middleware'ом на этом же уровне, чтобы /v1/system/metrics не утекал
//! публично, а пробы (которым нечего отдавать кроме «жив») оставались
//! доступными для k8s/uptime-мониторинга.
//!
//! Запуск отдельным портом (не на gRPC-сокете), чтобы reverse-proxy
//! мог терминировать TLS и резать публичный доступ обычными path-rules.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{middleware, routing::get, Router};
use tower_http::compression::CompressionLayer;

use crate::auth::{rest_auth_middleware, AuthState};
use crate::cluster_history::{cluster_range, ClusterHistoryState};
use crate::sysmetrics::{system_metrics, SysMetricsState};

/// Собрать REST-роутер.
/// - `sysmetrics_state` + `history_state` имеют свои `with_state(...)` —
///   axum поддерживает разные state-типы на разных под-роутерах через
///   `Router::with_state`.
/// - Bearer middleware — общий, прикладывается СНАРУЖИ над merge.
/// - Gzip-compression — внутренний слой, чтобы респонзы крупных
///   /clusters/range запросов сжимались (5–10× на columnar JSON).
///   /health тоже сжмётся, но это копейки — не оптимизируем отдельно.
pub fn router(
    sysmetrics_state: SysMetricsState,
    history_state: ClusterHistoryState,
    auth_state: AuthState,
) -> Router {
    let sysmetrics = Router::new()
        .route("/v1/system/metrics", get(system_metrics))
        .with_state(sysmetrics_state);

    let history = Router::new()
        .route("/v1/clusters/range", get(cluster_range))
        .with_state(history_state);

    let public = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/health/live", get(|| async { "live" }))
        .route("/health/ready", get(|| async { "ready" }));

    Router::new()
        .merge(public)
        .merge(sysmetrics)
        .merge(history)
        .layer(CompressionLayer::new().gzip(true))
        .layer(middleware::from_fn_with_state(
            auth_state,
            rest_auth_middleware,
        ))
}

/// Запустить axum-сервер на `addr`. Возвращает, когда сервер закрывается.
pub async fn serve_rest(
    addr: SocketAddr,
    sysmetrics_state: SysMetricsState,
    history_state: ClusterHistoryState,
    auth_state: AuthState,
) -> Result<()> {
    let app = router(sysmetrics_state, history_state, auth_state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind REST listener on {addr}"))?;
    tracing::info!(%addr, "cluster-api: REST server listening (+gzip, +/v1/clusters/range)");
    axum::serve(listener, app).await.context("axum serve")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn empty_state() -> SysMetricsState {
        SysMetricsState {
            ch_client: None,
            ch_url: String::new(),
            ch_database: String::new(),
        }
    }

    fn empty_history_state() -> ClusterHistoryState {
        ClusterHistoryState {
            ch_client: reqwest::Client::new(),
            ch_url: String::new(),
            ch_database: String::new(),
            ch_user: String::new(),
            ch_password: String::new(),
        }
    }

    fn app_with(auth: AuthState) -> Router {
        router(empty_state(), empty_history_state(), auth)
    }

    #[tokio::test]
    async fn health_is_public_even_when_auth_enabled() {
        let app = app_with(AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sysmetrics_rejects_missing_bearer() {
        let app = app_with(AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/system/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sysmetrics_rejects_wrong_bearer() {
        let app = app_with(AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/system/metrics")
                    .header("authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn sysmetrics_accepts_correct_bearer() {
        let app = app_with(AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/system/metrics")
                    .header("authorization", "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sysmetrics_open_when_auth_disabled() {
        let app = app_with(AuthState::disabled());
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/system/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn clusters_range_validates_params() {
        // Без bearer — отказ.
        let app = app_with(AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/clusters/range?exchange=BINANCEF&market_type=perp&symbol=BTCUSDT&interval_seconds=60&from_ms=0&to_ms=60000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // С bearer но без CH доступа — 502 (CH connection refused), не 5xx-крэш.
        let app = app_with(AuthState::disabled());
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/clusters/range?exchange=BINANCEF&market_type=perp&symbol=BTCUSDT&interval_seconds=60&from_ms=0&to_ms=60000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // empty ch_url → request будет невалидным URL'ом → BAD_GATEWAY
        assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn clusters_range_rejects_bad_interval() {
        let app = app_with(AuthState::disabled());
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/clusters/range?exchange=BINANCEF&market_type=perp&symbol=BTCUSDT&interval_seconds=45&from_ms=0&to_ms=60000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }
}
