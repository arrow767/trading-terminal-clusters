pub mod proto {
    tonic::include_proto!("clusters");
}

pub mod auth;
pub mod rest;
mod stream_server;
pub mod sysmetrics;

pub use auth::AuthState;
pub use rest::serve_rest;
pub use stream_server::{serve, serve_with_auth, ClusterStreamService};
pub use sysmetrics::SysMetricsState;
