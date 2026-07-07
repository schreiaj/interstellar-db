use std::sync::Arc;

use interstellar_db::{
    sync_once, Observation, ObservationEvent, SpatioTemporalStore, SubscriptionHub, SyncService,
};
use prost::Message as _;
use prost_types::Any;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use interstellar_db::interstellar::{
    interstellar_db_server::{InterstellarDb, InterstellarDbServer},
    interstellar_sync_client::InterstellarSyncClient,
    interstellar_sync_server::InterstellarSyncServer,
    QueryResponse, QueryWindowRequest, StoreRequest, StoreResponse, SubscribeRequest,
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

fn event_to_response(e: &ObservationEvent) -> QueryResponse {
    let payload = if e.payload.is_empty() {
        None
    } else {
        Any::decode(e.payload.as_slice()).ok()
    };
    QueryResponse {
        key: e.key.to_vec(),
        x: e.x,
        y: e.y,
        z: e.z,
        timestamp_secs: e.timestamp_secs,
        timestamp_nanos: e.timestamp_nanos,
        payload,
    }
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

struct Svc {
    store: Arc<SpatioTemporalStore>,
    /// Region subscriptions, fed by the store's change tap — so subscribers
    /// hear about sync-ingested observations too, not just local Stores.
    hub: Arc<SubscriptionHub>,
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

        // Subscription fan-out happens in the SubscriptionHub, fed by the
        // store's change tap — this handler only needs to write.
        Ok(Response::new(StoreResponse { key: key.to_vec() }))
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

    // ── Subscribe (watch a region for new observations) ──────────────────────

    type SubscribeStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let mut events = self.hub.attach(
            req.center_x,
            req.center_y,
            req.center_z,
            req.radius,
            req.max_age_secs,
            256,
        );

        // Forward hub events onto the gRPC stream; both channels are bounded,
        // and dropping either end tears the whole pipeline down.
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if tx.send(Ok(event_to_response(&event))).await.is_err() {
                    break;
                }
            }
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
    // Loopback by default; set BIND_ADDR=[::] (or 0.0.0.0) to serve a mesh.
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "[::1]".to_string());
    let addr = format!("{bind}:{port}").parse()?;

    let store = Arc::new(SpatioTemporalStore::open(&db_path, 1000.0, 3600)?);

    // Background epoch sealer: periodically persists Merkle seal records for
    // settled epochs so restarts rebuild the forest without scanning history.
    // Grace of one epoch duration = assumed worst-case delivery staleness.
    let sealer_store = Arc::clone(&store);
    tokio::spawn(async move {
        let grace = sealer_store.epoch_duration() as u64;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(600));
        loop {
            interval.tick().await;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before unix epoch")
                .as_secs();
            match sealer_store.seal_settled_epochs(now, grace).await {
                Ok(sealed) if !sealed.is_empty() => {
                    eprintln!("sealed epochs: {sealed:?}");
                }
                Ok(_) => {}
                Err(e) => eprintln!("epoch sealing failed: {e}"),
            }
        }
    });

    // Mesh peers: comma-separated gRPC URIs (e.g. "http://[::1]:50052").
    // Each is pulled from periodically; pull-only sync means a pair of nodes
    // listing each other converges from both sides.
    let peers: Vec<String> = std::env::var("PEERS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    let sync_interval: u64 = std::env::var("SYNC_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    if !peers.is_empty() {
        let sync_store = Arc::clone(&store);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(sync_interval));
            loop {
                interval.tick().await;
                for peer in &peers {
                    let mut client = match InterstellarSyncClient::connect(peer.clone()).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("peer {peer} unreachable: {e}");
                            continue;
                        }
                    };
                    match sync_once(&sync_store, &mut client).await {
                        Ok(stats) if stats.rows_applied > 0 => eprintln!(
                            "synced from {peer}: {} rows applied ({} received, {} leaves)",
                            stats.rows_applied, stats.rows_received, stats.leaves_fetched
                        ),
                        Ok(_) => {}
                        Err(e) => eprintln!("sync with {peer} failed: {e}"),
                    }
                }
            }
        });
    }

    // Subscription hub, fed by the store's change tap so both local writes
    // and sync-ingested rows reach subscribers.
    let hub = Arc::new(SubscriptionHub::new());
    tokio::spawn(Arc::clone(&hub).run(store.watch()));

    let svc = Svc {
        store: Arc::clone(&store),
        hub,
    };

    eprintln!("InterstellarDB gRPC server listening on {addr}");
    Server::builder()
        .add_service(InterstellarDbServer::new(svc))
        .add_service(InterstellarSyncServer::new(SyncService::new(store)))
        .serve(addr)
        .await?;

    Ok(())
}
