pub mod hub;
pub mod store;
pub mod sync;
pub use hub::SubscriptionHub;
pub use store::{Observation, ObservationEvent, SpatioTemporalStore};
pub use sync::{sync_once, SyncService, SyncStats};

// The pure spatio-temporal indexer now lives in the storage-agnostic
// `interstellar-index` crate (which also compiles to wasm). Re-export it so
// existing call sites (`interstellar_db::HybridSpatioTemporalIndexer`,
// `crate::HybridSpatioTemporalIndexer`) keep working unchanged.
pub use interstellar_index::HybridSpatioTemporalIndexer;
pub use interstellar_index::merkle::{self, DivergentLeaf, MerkleForest, NodeHash, diff_forests};

pub mod interstellar {
    tonic::include_proto!("interstellar");
}
