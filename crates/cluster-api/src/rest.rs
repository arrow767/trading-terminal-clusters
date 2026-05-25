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

use crate::auth::{rest_auth_middleware, AuthState};
use crate::sysmetrics::{system_metrics, SysMetricsState};

/// Собрать REST-роутер. `auth_state` контролирует и сам middleware,
/// и доступ /v1/system/metrics — выключенный auth превращает эндпоинт
/// в открытый (только для dev/local!).
pub fn router(sysmetrics_state: SysMetricsState, auth_state: AuthState) -> Router {
    let protected = Router::new()
        .route("/v1/system/metrics", get(system_metrics))
        .with_state(sysmetrics_state);

    let public = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/health/live", get(|| async { "live" }))
        .route("/health/ready", get(|| async { "ready" }));

    Router::new()
        .merge(public)
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            auth_state,
            rest_auth_middleware,
        ))
}

/// Запустить axum-сервер на `addr`. Возвращает, когда сервер закрывается.
pub async fn serve_rest(
    addr: SocketAddr,
    sysmetrics_state: SysMetricsState,
    auth_state: AuthState,
) -> Result<()> {
    let app = router(sysmetrics_state, auth_state);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind REST listener on {addr}"))?;
    tracing::info!(%addr, "cluster-api: REST server listening");
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

    #[tokio::test]
    async fn health_is_public_even_when_auth_enabled() {
        let app = router(empty_state(), AuthState::new(vec!["secret".into()], true));
        let res = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sysmetrics_rejects_missing_bearer() {
        let app = router(empty_state(), AuthState::new(vec!["secret".into()], true));
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
        let app = router(empty_state(), AuthState::new(vec!["secret".into()], true));
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
        let app = router(empty_state(), AuthState::new(vec!["secret".into()], true));
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
        let app = router(empty_state(), AuthState::disabled());
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
}
