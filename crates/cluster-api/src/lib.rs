pub mod proto {
    tonic::include_proto!("clusters");
}

mod stream_server;

pub use stream_server::{serve, ClusterStreamService};
