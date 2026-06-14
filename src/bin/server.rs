use std::sync::Arc;

use interstellar_db::{Observation, SpatioTemporalStore};
use prost::Message as _;
use prost_types::Any;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use interstellar_db::interstellar::{
    interstellar_db_server::{InterstellarDb, InterstellarDbServer},
    QueryRequest, QueryResponse, QueryWindowRequest, StoreRequest, StoreResponse,
    SubscribeRequest,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(e.to_string())
}

fn obs_to_response(o: Observation) -> QueryResponse {
    let payload = if o.payload.is_empty() {
        None
    } else {
        Any::decode(o.payload.as_slice()).ok()
    };
    QueryResponse {
        key: o.key,
        x: o.x,
        y: o.y,
        z: o.z,
        timestamp_secs: o.timestamp_secs,
        timestamp_nanos: o.timestamp_nanos,
        payload,
    }
}

// ---------------------------------------------------------------------------
// Subscription registry
// ---------------------------------------------------------------------------

struct Subscription {
    center_x: f64,
    center_y: f64,
    center_z: f64,
    /// Pre-computed radius² to avoid a sqrt on every Store call.
    r2: f64,
    tx: mpsc::Sender<Result<QueryResponse, Status>>,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

struct Svc {
    store: Arc<SpatioTemporalStore>,
    subs: Arc<RwLock<Vec<Subscription>>>,
}

#[tonic::async_trait]
impl InterstellarDb for Svc {
    // ── Store ────────────────────────────────────────────────────────────────

    async fn store(
        &self,
        request: Request<StoreRequest>,
    ) -> Result<Response<StoreResponse>, Status> {
        let req = request.into_inner();
        let (x, y, z) = (req.x, req.y, req.z);

        let payload_bytes = req
            .payload
            .ok_or_else(|| Status::invalid_argument("payload is required"))?
            .encode_to_vec();

        let key = self
            .store
            .store(x, y, z, req.timestamp_secs, req.timestamp_nanos, &payload_bytes)
            .await
            .map_err(internal)?;

        // Fan out to matching subscriptions.
        // Decode Any once and clone per matching subscriber rather than
        // re-decoding inside the loop.
        let payload_any = Any::decode(payload_bytes.as_slice()).ok();
        let notification = QueryResponse {
            key: key.to_vec(),
            x,
            y,
            z,
            timestamp_secs: req.timestamp_secs,
            timestamp_nanos: req.timestamp_nanos,
            payload: payload_any,
        };

        // Fan out under a read lock so concurrent writes don't serialize.
        let any_closed = {
            let subs = self.subs.read().await;
            let mut any_closed = false;
            for sub in subs.iter() {
                if sub.tx.is_closed() {
                    any_closed = true;
                    continue;
                }
                let dx = x - sub.center_x;
                let dy = y - sub.center_y;
                let dz = z - sub.center_z;
                if dx * dx + dy * dy + dz * dz <= sub.r2 {
                    let _ = sub.tx.try_send(Ok(notification.clone()));
                }
            }
            any_closed
        };

        // Re-check under write lock; stale indices from the read pass are
        // unsafe because another concurrent write may have already reshuffled.
        if any_closed {
            self.subs.write().await.retain(|sub| !sub.tx.is_closed());
        }

        Ok(Response::new(StoreResponse { key: key.to_vec() }))
    }

    // ── Query (single epoch) ─────────────────────────────────────────────────

    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let store = Arc::clone(&self.store);
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                store.query_epoch(req.center_x, req.center_y, req.center_z, req.radius, req.timestamp_secs)
            }).await;
            match result {
                Ok(Ok(items)) => {
                    for item in items {
                        if tx.send(Ok(obs_to_response(item))).await.is_err() { break; }
                    }
                }
                Ok(Err(e)) => { let _ = tx.send(Err(internal(e))).await; }
                Err(e) => { let _ = tx.send(Err(Status::internal(e.to_string()))).await; }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ── QueryWindow (time range) ─────────────────────────────────────────────

    type QueryWindowStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query_window(
        &self,
        request: Request<QueryWindowRequest>,
    ) -> Result<Response<Self::QueryWindowStream>, Status> {
        let req = request.into_inner();
        if req.start_secs > req.end_secs {
            return Err(Status::invalid_argument("start_secs must be <= end_secs"));
        }
        let store = Arc::clone(&self.store);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                store.query_window(
                    req.center_x,
                    req.center_y,
                    req.center_z,
                    req.radius,
                    req.start_secs,
                    req.end_secs,
                )
            }).await;
            match result {
                Ok(Ok(items)) => {
                    for item in items {
                        if tx.send(Ok(obs_to_response(item))).await.is_err() { break; }
                    }
                }
                Ok(Err(e)) => { let _ = tx.send(Err(internal(e))).await; }
                Err(e) => { let _ = tx.send(Err(Status::internal(e.to_string()))).await; }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    // ── Subscribe (watch a region for new stores) ────────────────────────────

    type SubscribeStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(256);

        self.subs.write().await.push(Subscription {
            center_x: req.center_x,
            center_y: req.center_y,
            center_z: req.center_z,
            r2: req.radius * req.radius,
            tx,
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "./interstellar.db".to_string());
    let port = std::env::var("PORT").unwrap_or_else(|_| "50051".to_string());
    let addr = format!("[::1]:{port}").parse()?;

    let store = SpatioTemporalStore::open(&db_path, 1000.0, 3600)?;

    let svc = Svc {
        store: Arc::new(store),
        subs: Arc::new(RwLock::new(Vec::new())),
    };

    eprintln!("InterstellarDB gRPC server listening on {addr}");
    Server::builder()
        .add_service(InterstellarDbServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
