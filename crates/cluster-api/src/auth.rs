//! Bearer-token authentication.
//!
//! Один и тот же набор токенов проверяется на двух точках входа:
//! - REST middleware (`/v1/system/metrics` и любые будущие admin-роуты);
//! - gRPC interceptor (ClusterStream subscribe).
//!
//! Сравнение константного времени — иначе сравнение `==` коротко-замыкается
//! на первой различающейся байте и через timing leak позволяет
//! угадывать токен. См. оригинал в `oi-api/src/auth.rs` — копируем тот же
//! паттерн целиком, чтобы поведение между сервисами было одинаковым.
//!
//! Дизайн «hardcoded токен в конфиге» сознательный: цель — отрезать
//! публичный трафик и dos-боты, а не управлять пользовательскими
//! доступами. Никаких rotations / DB / JWT — просто статический Bearer
//! загруженный из TOML/env. Менять → перезапуск.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tonic::{metadata::MetadataValue, Status};

/// Множество принимаемых токенов. Дёшевый `clone` — внутри `Arc<Vec>`.
#[derive(Clone, Debug)]
pub struct AuthState {
    tokens: Arc<Vec<String>>,
    /// Если `false`, любой запрос пропускается без проверки. Используется
    /// в dev-режиме и unit-тестах, чтобы не тащить токены в каждый сетап.
    enabled: bool,
}

impl AuthState {
    pub fn new(tokens: Vec<String>, enabled: bool) -> Self {
        Self {
            tokens: Arc::new(tokens),
            enabled,
        }
    }

    /// Отключённый auth — все запросы проходят. Удобно для тестов.
    pub fn disabled() -> Self {
        Self::new(Vec::new(), false)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Constant-time проверка `presented` против списка разрешённых.
    /// Возвращает true на первом совпадении. Различие в длине утечёт
    /// через short-circuit, но длина токена — не секрет.
    pub fn accepts(&self, presented: &str) -> bool {
        if !self.enabled {
            return true;
        }
        self.tokens
            .iter()
            .any(|valid| ct_eq(valid.as_bytes(), presented.as_bytes()))
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Достать `Bearer <token>` из строки заголовка. Регистр-чувствительно
/// (RFC 6750 формально допускает кейсы — мы выбираем строгость, так же
/// как oi-api).
fn extract_bearer(header_value: Option<&str>) -> Option<&str> {
    header_value.and_then(|v| v.strip_prefix("Bearer "))
}

/// REST middleware. `/health`, `/health/*`, `/metrics` пропускаются без
/// auth — k8s/uptime пробам не положено таскать токен.
pub async fn rest_auth_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if is_public_path(path) {
        return next.run(request).await;
    }
    if !state.enabled {
        return next.run(request).await;
    }
    let header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let Some(token) = extract_bearer(header) else {
        return unauthenticated_response();
    };
    if state.accepts(token) {
        next.run(request).await
    } else {
        unauthenticated_response()
    }
}

fn is_public_path(path: &str) -> bool {
    path == "/health"
        || path.starts_with("/health/")
        || path == "/metrics"
}

fn unauthenticated_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
        "missing or invalid bearer token",
    )
        .into_response()
}

/// gRPC interceptor. Регистрируется на сервисе ClusterStream; tonic
/// клонирует его на каждый запрос. `AuthState` уже `Arc`'нут, так что
/// клон стоит копейки.
pub fn grpc_auth_interceptor(
    state: AuthState,
) -> impl tonic::service::Interceptor + Clone {
    move |req: tonic::Request<()>| -> Result<tonic::Request<()>, Status> {
        if !state.enabled {
            return Ok(req);
        }
        let header: Option<&MetadataValue<_>> = req.metadata().get("authorization");
        let token_str = header.and_then(|v| v.to_str().ok());
        let Some(token) = extract_bearer(token_str) else {
            return Err(Status::unauthenticated("missing bearer token"));
        };
        if state.accepts(token) {
            Ok(req)
        } else {
            Err(Status::unauthenticated("invalid bearer token"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_bearer_strict_prefix() {
        assert_eq!(extract_bearer(Some("Bearer s3cr3t")), Some("s3cr3t"));
        assert_eq!(extract_bearer(Some("bearer s3cr3t")), None);
        assert_eq!(extract_bearer(Some("Token s3cr3t")), None);
        assert_eq!(extract_bearer(None), None);
    }

    #[test]
    fn ct_eq_matches_eq() {
        assert!(ct_eq(b"abcd", b"abcd"));
        assert!(!ct_eq(b"abcd", b"abce"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn disabled_auth_accepts_anything() {
        let s = AuthState::disabled();
        assert!(s.accepts(""));
        assert!(s.accepts("anything"));
    }

    #[test]
    fn enabled_auth_strict() {
        let s = AuthState::new(vec!["alpha".into(), "bravo".into()], true);
        assert!(s.accepts("alpha"));
        assert!(s.accepts("bravo"));
        assert!(!s.accepts("charlie"));
        assert!(!s.accepts(""));
    }

    #[test]
    fn public_paths_only_strict_prefix() {
        assert!(is_public_path("/health"));
        assert!(is_public_path("/health/live"));
        assert!(is_public_path("/metrics"));
        assert!(!is_public_path("/healthier"));
        assert!(!is_public_path("/v1/system/metrics"));
        assert!(!is_public_path("/"));
    }
}
