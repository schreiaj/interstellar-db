//! Browser bindings for the pure spatio-temporal indexer.
//!
//! This crate exposes [`interstellar_index::HybridSpatioTemporalIndexer`] to
//! JavaScript via `wasm-bindgen`. It is deliberately storage-agnostic: the WASM
//! side only *computes* keys and KV scan ranges, and decodes/sphere-filters
//! candidates. The actual persistence (IndexedDB) and range scans live in the
//! JS Web Worker — see `web/worker.js`.
//!
//! ## Why timestamps are `f64`
//!
//! JS numbers are IEEE-754 doubles, exact for integers up to 2^53. Unix-second
//! timestamps and sub-second nanosecond offsets both fit comfortably, so we take
//! `f64` at the boundary and cast to `u64` internally rather than forcing
//! callers to deal with `BigInt`.

use interstellar_index::HybridSpatioTemporalIndexer;
use js_sys::{Array, Float64Array, Uint8Array};
use wasm_bindgen::prelude::*;

/// Install a panic hook that forwards Rust panics to `console.error`, so an
/// out-of-range assertion in the indexer shows a readable stack in devtools
/// instead of an opaque `unreachable`.
#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

/// Thin JS handle over the native indexer.
#[wasm_bindgen]
pub struct Indexer {
    inner: HybridSpatioTemporalIndexer,
}

#[wasm_bindgen]
impl Indexer {
    /// `new Indexer(maxRange, epochDuration)`.
    ///
    /// `maxRange` is the half-extent of each axis (coordinates are clamped to
    /// `[-maxRange, maxRange]`); `epochDuration` is the macro-epoch length in
    /// seconds. Mirrors the native `HybridSpatioTemporalIndexer::new`.
    #[wasm_bindgen(constructor)]
    pub fn new(max_range: f64, epoch_duration: u32) -> Indexer {
        Indexer {
            inner: HybridSpatioTemporalIndexer::new(max_range, epoch_duration),
        }
    }

    /// The 20-byte big-endian key for a 4-D point, as a `Uint8Array`.
    /// IndexedDB stores `Uint8Array`/`ArrayBuffer` keys and orders them by byte
    /// comparison — exactly the lexicographic order this key layout assumes.
    #[wasm_bindgen(js_name = generateKey)]
    pub fn generate_key(&self, x: f64, y: f64, z: f64, secs: f64, nanos: f64) -> Uint8Array {
        let key = self.inner.generate_key(x, y, z, secs as u64, nanos as u64);
        Uint8Array::from(&key[..])
    }

    /// Decode a 20-byte key back into its 4-D point, as a 5-element
    /// `Float64Array`: `[x, y, z, secs, nanos]`. Throws if the input is not
    /// exactly 20 bytes.
    ///
    /// Returns a plain typed array rather than a wrapped struct so the worker's
    /// per-candidate decode loop allocates no wasm-side handles to free.
    #[wasm_bindgen(js_name = decodeKey)]
    pub fn decode_key(&self, key: &[u8]) -> Result<Float64Array, JsError> {
        let arr: [u8; 20] = key
            .try_into()
            .map_err(|_| JsError::new("key must be exactly 20 bytes"))?;
        let (x, y, z, secs, nanos) = self.inner.decode_key(&arr);
        Ok(Float64Array::from(
            &[x, y, z, secs as f64, nanos as f64][..],
        ))
    }

    /// Plan a single-epoch sphere query: returns the `[start, end]` byte-range
    /// pairs (each a 2-element `Array` of `Uint8Array`) to scan in IndexedDB for
    /// the epoch containing `secs`. Depth is chosen automatically to keep
    /// fan-out small, mirroring the native `query_epoch`.
    #[wasm_bindgen(js_name = epochRanges)]
    pub fn epoch_ranges(&self, cx: f64, cy: f64, cz: f64, radius: f64, secs: f64) -> Array {
        let depth = self.inner.calculate_optimal_depth(radius);
        let ranges =
            self.inner
                .get_boundary_protected_ranges(cx, cy, cz, radius, secs as u64, depth);
        ranges_to_js(&ranges)
    }

    /// Plan a time-window sphere query across `[startSecs, endSecs]`. Returns
    /// `[start, end]` byte-range pairs spanning every epoch in the window,
    /// mirroring the native `query_window`.
    #[wasm_bindgen(js_name = windowRanges)]
    pub fn window_ranges(
        &self,
        cx: f64,
        cy: f64,
        cz: f64,
        radius: f64,
        start_secs: f64,
        end_secs: f64,
    ) -> Array {
        let depth = self.inner.calculate_optimal_depth(radius);
        let ranges = self.inner.get_boundary_protected_ranges_time_window(
            cx,
            cy,
            cz,
            radius,
            start_secs as u64,
            end_secs as u64,
            depth,
        );
        ranges_to_js(&ranges)
    }
}

/// Convert native `(start, end)` key-range pairs into a JS `Array` of
/// `[Uint8Array, Uint8Array]` pairs, ready to feed `IDBKeyRange.bound`.
fn ranges_to_js(ranges: &[([u8; 20], [u8; 20])]) -> Array {
    let out = Array::new_with_length(ranges.len() as u32);
    for (i, (start, end)) in ranges.iter().enumerate() {
        let pair = Array::new_with_length(2);
        pair.set(0, Uint8Array::from(&start[..]).into());
        pair.set(1, Uint8Array::from(&end[..]).into());
        out.set(i as u32, pair.into());
    }
    out
}
