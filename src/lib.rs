pub mod store;
pub use store::{Observation, SpatioTemporalStore};

// The pure spatio-temporal indexer now lives in the storage-agnostic
// `interstellar-index` crate (which also compiles to wasm). Re-export it so
// existing call sites (`interstellar_db::HybridSpatioTemporalIndexer`,
// `crate::HybridSpatioTemporalIndexer`) keep working unchanged.
pub use interstellar_index::HybridSpatioTemporalIndexer;

pub mod interstellar {
    tonic::include_proto!("interstellar");
}
