//! Anti-entropy sync between mesh peers.
//!
//! [`SyncService`] serves this node's Merkle forest over gRPC (read-only);
//! [`sync_once`] drives one pull round against a peer: compare epoch roots,
//! descend only into divergent octants, fetch the rows in divergent leaf
//! cells, and merge them through the store's idempotent [`ingest`].
//!
//! Sync is pull-only. A node never writes to a peer; each node is responsible
//! for its own convergence, so a pair of nodes polling each other converges
//! from both sides, and one-way reachability still makes one-way progress.
//! Over-shipping is harmless: merge is set-union, duplicates no-op.
//!
//! [`ingest`]: crate::SpatioTemporalStore::ingest

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::interstellar::{
    interstellar_sync_client::InterstellarSyncClient,
    interstellar_sync_server::InterstellarSync,
    ChildHash, EpochRoot, SyncChildrenRequest, SyncChildrenResponse, SyncFetchRequest,
    SyncRootsRequest, SyncRootsResponse, SyncRow,
};
use crate::store::MERKLE_LEAF_DEPTH_BITS;
use crate::{merkle, SpatioTemporalStore};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn hash_bytes(h: u128) -> Vec<u8> {
    h.to_be_bytes().to_vec()
}

fn hash_from_bytes(b: &[u8]) -> Result<u128, Status> {
    let arr: [u8; 16] = b
        .try_into()
        .map_err(|_| Status::invalid_argument("hash must be 16 bytes"))?;
    Ok(u128::from_be_bytes(arr))
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Read-only gRPC facade over one store's Merkle forest and key ranges.
pub struct SyncService {
    store: Arc<SpatioTemporalStore>,
}

impl SyncService {
    pub fn new(store: Arc<SpatioTemporalStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl InterstellarSync for SyncService {
    async fn roots(
        &self,
        _request: Request<SyncRootsRequest>,
    ) -> Result<Response<SyncRootsResponse>, Status> {
        let roots = self
            .store
            .epoch_roots()
            .into_iter()
            .map(|(epoch, hash)| EpochRoot { epoch, hash: hash_bytes(hash) })
            .collect();
        Ok(Response::new(SyncRootsResponse {
            roots,
            leaf_depth_bits: MERKLE_LEAF_DEPTH_BITS as u32,
        }))
    }

    async fn children(
        &self,
        request: Request<SyncChildrenRequest>,
    ) -> Result<Response<SyncChildrenResponse>, Status> {
        let req = request.into_inner();
        let depth = req.depth_bits as usize;
        if depth % 3 != 0 || depth + 3 > MERKLE_LEAF_DEPTH_BITS {
            return Err(Status::invalid_argument(format!(
                "depth_bits must be a multiple of 3 with depth_bits + 3 ≤ {MERKLE_LEAF_DEPTH_BITS}"
            )));
        }
        let children = self
            .store
            .child_hashes(req.epoch, depth, req.prefix)
            .into_iter()
            .map(|(prefix, hash)| ChildHash { prefix, hash: hash_bytes(hash) })
            .collect();
        Ok(Response::new(SyncChildrenResponse { children }))
    }

    type FetchStream = ReceiverStream<Result<SyncRow, Status>>;

    async fn fetch(
        &self,
        request: Request<SyncFetchRequest>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let req = request.into_inner();
        let start: [u8; 20] = req
            .start_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("start_key must be 20 bytes"))?;
        let end: [u8; 20] = req
            .end_key
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("end_key must be 20 bytes"))?;
        if start > end {
            return Err(Status::invalid_argument("start_key must be ≤ end_key"));
        }

        let store = Arc::clone(&self.store);
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let rows = tokio::task::spawn_blocking(move || store.scan_range(&start, &end)).await;
            match rows {
                Ok(Ok(rows)) => {
                    for (key, payload) in rows {
                        let row = SyncRow { key: key.to_vec(), payload };
                        if tx.send(Ok(row)).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Err(e)) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// ---------------------------------------------------------------------------
// Client side (the puller)
// ---------------------------------------------------------------------------

/// Outcome of one pull round against one peer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncStats {
    /// Divergent leaf cells fetched.
    pub leaves_fetched: usize,
    /// Rows the peer shipped (including ones we already had).
    pub rows_received: u64,
    /// Rows that actually changed this store.
    pub rows_applied: u64,
}

/// One pull round: fetch everything the peer has that this store lacks.
///
/// Cost is proportional to the divergence, not the data: matching subtrees
/// are skipped at the highest level where their hashes agree. Descends to the
/// shallower of the two nodes' leaf depths (hashes are structure-independent,
/// so mixed depths compare correctly).
pub async fn sync_once(
    store: &SpatioTemporalStore,
    client: &mut InterstellarSyncClient<tonic::transport::Channel>,
) -> Result<SyncStats, BoxError> {
    let remote = client.roots(SyncRootsRequest {}).await?.into_inner();
    let remote_leaf = remote.leaf_depth_bits as usize;
    if remote_leaf < 3 || remote_leaf % 3 != 0 {
        return Err(format!("peer reported invalid leaf depth {remote_leaf}").into());
    }
    let descend_to = remote_leaf.min(MERKLE_LEAF_DEPTH_BITS);

    let mut stats = SyncStats::default();

    // Work through divergent nodes breadth-first. A node goes on the list
    // only if the peer's hash differs from ours AND the peer's is non-zero —
    // cells the peer doesn't have contain nothing to pull.
    let mut work: VecDeque<(u32, usize, u64)> = VecDeque::new();
    for root in remote.roots {
        let theirs = hash_from_bytes(&root.hash)?;
        if theirs != 0 && theirs != store.node_hash(root.epoch, 0, 0) {
            work.push_back((root.epoch, 0, 0));
        }
    }

    while let Some((epoch, depth, prefix)) = work.pop_front() {
        if depth == descend_to {
            let (start, end) = merkle::leaf_key_range(epoch, prefix, depth);
            let mut rows = client
                .fetch(SyncFetchRequest { start_key: start.to_vec(), end_key: end.to_vec() })
                .await?
                .into_inner();
            stats.leaves_fetched += 1;
            while let Some(row) = rows.message().await? {
                let key: [u8; 20] = row
                    .key
                    .as_slice()
                    .try_into()
                    .map_err(|_| "peer sent a row with a malformed key")?;
                stats.rows_received += 1;
                if store.ingest(key, &row.payload).await? {
                    stats.rows_applied += 1;
                }
            }
            continue;
        }

        let children = client
            .children(SyncChildrenRequest { epoch, depth_bits: depth as u32, prefix })
            .await?
            .into_inner();
        for child in children.children {
            let theirs = hash_from_bytes(&child.hash)?;
            if theirs != 0 && theirs != store.node_hash(epoch, depth + 3, child.prefix) {
                work.push_back((epoch, depth + 3, child.prefix));
            }
        }
    }

    Ok(stats)
}
