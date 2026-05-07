use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use cluster_engine::ClusterBus;
use exchange_core::{
    AnalyticsDiff, AnalyticsSnapshot, ClusterBucket, ClusterFrame, Exchange, MarketType,
    SymbolKey as DomainSymbolKey,
};
use futures_util::Stream;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::proto;
use crate::proto::cluster_stream_server::{ClusterStream, ClusterStreamServer};

pub struct ClusterStreamService {
    bus: Arc<ClusterBus>,
}

impl ClusterStreamService {
    pub fn new(bus: Arc<ClusterBus>) -> Self {
        Self { bus }
    }

    pub fn into_server(self) -> ClusterStreamServer<Self> {
        ClusterStreamServer::new(self)
    }
}

type FrameStream = Pin<Box<dyn Stream<Item = Result<proto::Frame, Status>> + Send>>;

#[tonic::async_trait]
impl ClusterStream for ClusterStreamService {
    type SubscribeStream = FrameStream;

    async fn subscribe(
        &self,
        req: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = req.into_inner();
        if req.symbols.is_empty() {
            return Err(Status::invalid_argument("subscribe with empty symbol list"));
        }
        if req.symbols.len() > 1024 {
            return Err(Status::invalid_argument(
                "too many symbols in one subscribe (max 1024)",
            ));
        }

        let mut per_symbol_streams = Vec::with_capacity(req.symbols.len());
        for sym in req.symbols {
            let key = symbol_key_from_proto(&sym)?;
            let rx = self.bus.subscribe(&key);
            let proto_key = proto_key_from_domain(&key);
            // BroadcastStream reports lag with `Lagged(n)`; we filter it
            // out and continue. A lagged client misses frames but will
            // resync at the next window-roll snapshot — preferable to
            // tearing down the stream and forcing a full reconnect.
            let s = BroadcastStream::new(rx).filter_map(move |r| match r {
                Ok(frame) => Some(Ok(frame_to_proto(&proto_key, &frame))),
                Err(BroadcastStreamRecvError::Lagged(n)) => {
                    tracing::warn!(lagged = n, symbol = %proto_key.symbol, "client lagged");
                    None
                }
            });
            per_symbol_streams.push(s);
        }

        let merged: FrameStream = Box::pin(futures_util::stream::select_all(per_symbol_streams));
        Ok(Response::new(merged))
    }
}

/// Run a tonic gRPC server on `addr` exposing ClusterStream over the
/// provided bus. Returns when the server task finishes (or errors).
pub async fn serve(bus: Arc<ClusterBus>, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    let svc = ClusterStreamService::new(bus).into_server();
    tracing::info!(%addr, "cluster-api: gRPC server listening");
    Server::builder().add_service(svc).serve(addr).await
}

// `tonic::Status` is large (~176 bytes) — boxing it everywhere just to
// silence result_large_err is ergonomic noise without measurable benefit
// on a path that returns once per Subscribe call (not per frame).
#[allow(clippy::result_large_err)]
fn symbol_key_from_proto(p: &proto::SymbolKey) -> Result<DomainSymbolKey, Status> {
    let exchange = parse_exchange(&p.exchange)
        .ok_or_else(|| Status::invalid_argument(format!("unknown exchange: {}", p.exchange)))?;
    let market_type = parse_market_type(&p.market_type).ok_or_else(|| {
        Status::invalid_argument(format!("unknown market_type: {}", p.market_type))
    })?;
    if p.symbol.is_empty() {
        return Err(Status::invalid_argument("empty symbol"));
    }
    Ok(DomainSymbolKey::new(
        exchange,
        market_type,
        p.symbol.as_str(),
    ))
}

fn proto_key_from_domain(k: &DomainSymbolKey) -> proto::SymbolKey {
    proto::SymbolKey {
        exchange: k.exchange.wire_id().to_string(),
        market_type: market_type_wire(k.market_type).to_string(),
        symbol: k.symbol.to_string(),
    }
}

fn parse_exchange(s: &str) -> Option<Exchange> {
    Some(match s {
        "BINANCE" => Exchange::Binance,
        "BINANCEF" => Exchange::BinanceF,
        "BYBIT" => Exchange::Bybit,
        "BYBITF" => Exchange::BybitF,
        "BITGET" => Exchange::Bitget,
        "BITGETF" => Exchange::BitgetF,
        "OKX" => Exchange::Okx,
        "OKXF" => Exchange::OkxF,
        "HYPERLIQUID" => Exchange::Hyperliquid,
        "KUCOIN" => Exchange::Kucoin,
        "KUCOINF" => Exchange::KucoinF,
        "GATE" => Exchange::Gate,
        "GATEF" => Exchange::GateF,
        _ => return None,
    })
}

fn parse_market_type(s: &str) -> Option<MarketType> {
    Some(match s {
        "SPOT" => MarketType::Spot,
        "PERP" => MarketType::Perp,
        _ => return None,
    })
}

fn market_type_wire(m: MarketType) -> &'static str {
    match m {
        MarketType::Spot => "SPOT",
        MarketType::Perp => "PERP",
    }
}

fn frame_to_proto(key: &proto::SymbolKey, frame: &ClusterFrame) -> proto::Frame {
    let body = match frame {
        ClusterFrame::Snapshot(s) => proto::frame::Body::Snapshot(snapshot_to_proto(s)),
        ClusterFrame::Diff(d) => proto::frame::Body::Diff(diff_to_proto(d)),
    };
    proto::Frame {
        key: Some(key.clone()),
        body: Some(body),
    }
}

fn snapshot_to_proto(s: &AnalyticsSnapshot) -> proto::Snapshot {
    proto::Snapshot {
        window_start_ns: s.window_start_ns,
        sequence: s.sequence,
        clusters: s.clusters.iter().map(bucket_to_proto).collect(),
    }
}

fn diff_to_proto(d: &AnalyticsDiff) -> proto::Diff {
    proto::Diff {
        window_start_ns: d.window_start_ns,
        sequence: d.sequence,
        upserts: d.upserts.iter().map(bucket_to_proto).collect(),
        removes: d.removes.clone(),
    }
}

fn bucket_to_proto(b: &ClusterBucket) -> proto::Bucket {
    proto::Bucket {
        price: b.price,
        bid_qty: b.bid_qty,
        ask_qty: b.ask_qty,
        trades: b.trades,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use exchange_core::{AnalyticsSnapshot, ClusterBucket, ClusterFrame};
    use tokio::net::TcpListener;

    use super::*;
    use crate::proto::cluster_stream_client::ClusterStreamClient;

    fn key() -> DomainSymbolKey {
        DomainSymbolKey::new(Exchange::BinanceF, MarketType::Perp, "BTCUSDT")
    }

    fn snapshot_frame(seq: i64) -> ClusterFrame {
        ClusterFrame::Snapshot(Arc::new(AnalyticsSnapshot {
            window_start_ns: 1_700_000_000_000_000_000,
            sequence: seq,
            clusters: vec![ClusterBucket {
                price: 6_723_450,
                bid_qty: 100,
                ask_qty: 50,
                trades: 12,
            }],
        }))
    }

    #[tokio::test]
    async fn subscribe_streams_published_frames() {
        let bus = Arc::new(ClusterBus::new());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let bus_for_server = Arc::clone(&bus);
        let server_handle = tokio::spawn(async move {
            serve(bus_for_server, addr).await.unwrap();
        });

        // Wait briefly for the server to bind.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = ClusterStreamClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

        let req = proto::SubscribeRequest {
            symbols: vec![proto::SymbolKey {
                exchange: "BINANCEF".into(),
                market_type: "PERP".into(),
                symbol: "BTCUSDT".into(),
            }],
        };
        let mut stream = client.subscribe(req).await.unwrap().into_inner();

        // Give the server a tick to register the subscription before we
        // publish, otherwise the publish may happen before the broadcast
        // receiver is allocated.
        tokio::time::sleep(Duration::from_millis(50)).await;
        bus.publish(&key(), snapshot_frame(7));

        let frame = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let proto_key = frame.key.unwrap();
        assert_eq!(proto_key.exchange, "BINANCEF");
        assert_eq!(proto_key.market_type, "PERP");
        assert_eq!(proto_key.symbol, "BTCUSDT");
        let body = frame.body.unwrap();
        match body {
            proto::frame::Body::Snapshot(s) => {
                assert_eq!(s.sequence, 7);
                assert_eq!(s.clusters.len(), 1);
                assert_eq!(s.clusters[0].price, 6_723_450);
                assert_eq!(s.clusters[0].bid_qty, 100);
            }
            _ => panic!("expected snapshot body"),
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn subscribe_rejects_empty_symbol_list() {
        let bus = Arc::new(ClusterBus::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let bus_for_server = Arc::clone(&bus);
        let server_handle = tokio::spawn(async move {
            serve(bus_for_server, addr).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = ClusterStreamClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let req = proto::SubscribeRequest { symbols: vec![] };
        let r = client.subscribe(req).await;
        let err = r.expect_err("should reject empty list");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        server_handle.abort();
    }

    #[tokio::test]
    async fn subscribe_rejects_unknown_exchange() {
        let bus = Arc::new(ClusterBus::new());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let bus_for_server = Arc::clone(&bus);
        let server_handle = tokio::spawn(async move {
            serve(bus_for_server, addr).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut client = ClusterStreamClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let req = proto::SubscribeRequest {
            symbols: vec![proto::SymbolKey {
                exchange: "FAKEX".into(),
                market_type: "PERP".into(),
                symbol: "BTCUSDT".into(),
            }],
        };
        let r = client.subscribe(req).await;
        let err = r.expect_err("should reject unknown exchange");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        server_handle.abort();
    }
}
