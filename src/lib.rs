use std::collections::BTreeSet;

pub mod store;
pub use store::{Observation, SpatioTemporalStore};

pub mod interstellar {
    tonic::include_proto!("interstellar");
}


#[derive(Debug, Clone)]
pub struct HybridSpatioTemporalIndexer {
    max_range: f64,
    epoch_duration: u32,
}

impl HybridSpatioTemporalIndexer {
    pub fn new(max_range: f64, epoch_duration: u32) -> Self {
        assert!(max_range > 0.0, "max_range must be positive, got {max_range}");
        assert!(epoch_duration > 0, "epoch_duration must be non-zero");
        Self { max_range, epoch_duration }
    }

    pub fn max_range(&self) -> f64 { self.max_range }
    pub fn epoch_duration(&self) -> u32 { self.epoch_duration }

    /// Normalizes a float coordinate [-max_range, max_range] into a 21-bit integer.
    fn normalize_coordinate(&self, val: f64) -> u32 {
        let clamped = val.clamp(-self.max_range, self.max_range);
        let normalized = (clamped + self.max_range) / (2.0 * self.max_range);
        let max_21bit = (1u32 << 21) - 1;
        (normalized * max_21bit as f64).round() as u32
    }

    /// Denormalizes a 21-bit integer back into a float coordinate.
    fn denormalize_coordinate(&self, val: u32) -> f64 {
        let max_21bit = (1u32 << 21) - 1;
        let normalized = val as f64 / max_21bit as f64;
        (normalized * 2.0 * self.max_range) - self.max_range
    }

    /// Interleaves 21-bit X, Y, Z into a 63-bit Morton code packed into a u64.
    /// Bit layout per axis-bit i: x→3i+2, y→3i+1, z→3i  (bit 63 always zero).
    fn encode_3d_morton(x: u32, y: u32, z: u32) -> u64 {
        let mut morton: u64 = 0;
        for i in 0..21 {
            morton |= (((x >> i) & 1) as u64) << (3 * i + 2);
            morton |= (((y >> i) & 1) as u64) << (3 * i + 1);
            morton |= (((z >> i) & 1) as u64) << (3 * i);
        }
        morton
    }

    /// Deinterleaves a 63-bit Morton code back into 21-bit X, Y, Z.
    fn decode_3d_morton(morton: u64) -> (u32, u32, u32) {
        let (mut x, mut y, mut z) = (0u32, 0u32, 0u32);
        for i in 0..21 {
            x |= (((morton >> (3 * i + 2)) & 1) as u32) << i;
            y |= (((morton >> (3 * i + 1)) & 1) as u32) << i;
            z |= (((morton >> (3 * i)) & 1) as u32) << i;
        }
        (x, y, z)
    }

    /// Generates a 20-byte big-endian key:
    ///   [4B macro epoch] [8B 3-D Morton] [8B nanosecond offset within epoch]
    ///
    /// # Panics (debug)
    /// Panics in debug builds if `timestamp_nanos >= 1_000_000_000`.
    pub fn generate_key(
        &self,
        x: f64, y: f64, z: f64,
        timestamp_secs: u64,
        timestamp_nanos: u64,
    ) -> [u8; 20] {
        debug_assert!(timestamp_nanos < 1_000_000_000, "timestamp_nanos must be < 1_000_000_000 (sub-second only), got {timestamp_nanos}");
        let epoch_bucket = (timestamp_secs / self.epoch_duration as u64) as u32;
        let nano_offset =
            (timestamp_secs % self.epoch_duration as u64) * 1_000_000_000
            + timestamp_nanos;

        let spatial_code = Self::encode_3d_morton(
            self.normalize_coordinate(x),
            self.normalize_coordinate(y),
            self.normalize_coordinate(z),
        );

        let mut key = [0u8; 20];
        key[0..4].copy_from_slice(&epoch_bucket.to_be_bytes());
        key[4..12].copy_from_slice(&spatial_code.to_be_bytes());
        key[12..20].copy_from_slice(&nano_offset.to_be_bytes());
        key
    }

    /// Reconstructs (x, y, z, secs, sub-second-nanos) from a 20-byte key.
    pub fn decode_key(&self, key: &[u8; 20]) -> (f64, f64, f64, u64, u64) {
        let epoch_bucket = u32::from_be_bytes(key[0..4].try_into().unwrap());
        let spatial_code = u64::from_be_bytes(key[4..12].try_into().unwrap());
        let nano_offset  = u64::from_be_bytes(key[12..20].try_into().unwrap());

        let (nx, ny, nz) = Self::decode_3d_morton(spatial_code);
        let x = self.denormalize_coordinate(nx);
        let y = self.denormalize_coordinate(ny);
        let z = self.denormalize_coordinate(nz);

        let total_nanos =
            epoch_bucket as u64 * self.epoch_duration as u64 * 1_000_000_000
            + nano_offset;
        (x, y, z, total_nanos / 1_000_000_000, total_nanos % 1_000_000_000)
    }

    /// Returns the largest `depth_bits` (multiple of 3, ≤ 63) such that a bounding
    /// box of `2·radius` spans at most ~2 cells per axis (≤ 8 total Morton cells).
    ///
    /// At the returned depth, each spatial cell is at least as wide as the search
    /// diameter, keeping KV fan-out in [1, 8].
    pub fn calculate_optimal_depth(&self, radius: f64) -> usize {
        let fraction = (radius * 2.0) / (self.max_range * 2.0);
        if fraction >= 1.0 {
            return 3;
        }
        // Number of times we can halve the full range before a cell ≤ search diameter.
        let subdivisions = (1.0_f64 / fraction).log2().floor() as usize;
        let axis_bits = subdivisions.clamp(1, 21);
        axis_bits * 3
    }

    /// Walks the 3-D Morton grid at `depth_bits` resolution and returns every
    /// unique cell prefix that intersects the bounding box (center ± radius).
    /// This is the epoch-independent spatial core shared by both range helpers.
    fn compute_spatial_prefixes(
        &self,
        center_x: f64, center_y: f64, center_z: f64,
        radius: f64,
        depth_bits: usize,
    ) -> BTreeSet<u64> {
        let shift = 64 - depth_bits;

        let nx_min = self.normalize_coordinate(center_x - radius);
        let nx_max = self.normalize_coordinate(center_x + radius);
        let ny_min = self.normalize_coordinate(center_y - radius);
        let ny_max = self.normalize_coordinate(center_y + radius);
        let nz_min = self.normalize_coordinate(center_z - radius);
        let nz_max = self.normalize_coordinate(center_z + radius);

        let axis_bits  = depth_bits / 3;
        let axis_shift = 21 - axis_bits;
        let step = 1u32 << axis_shift;

        // Snap to nearest lower cell boundary so no partial cell is skipped.
        let x0 = (nx_min >> axis_shift) << axis_shift;
        let y0 = (ny_min >> axis_shift) << axis_shift;
        let z0 = (nz_min >> axis_shift) << axis_shift;

        let mut prefixes = BTreeSet::new();
        let mut x = x0;
        while x <= nx_max {
            let mut y = y0;
            while y <= ny_max {
                let mut z = z0;
                while z <= nz_max {
                    let m = Self::encode_3d_morton(x, y, z);
                    prefixes.insert((m >> shift) << shift);
                    if z > u32::MAX - step { break; }
                    z += step;
                }
                if y > u32::MAX - step { break; }
                y += step;
            }
            if x > u32::MAX - step { break; }
            x += step;
        }
        prefixes
    }

    /// Generates range pairs covering every Morton cell that intersects the 3-D
    /// bounding box (center ± radius) at the requested `depth_bits` resolution.
    ///
    /// Each pair is a `(start_key, end_key)` 20-byte big-endian inclusive range
    /// ready for a KV prefix scan within the macro epoch that contains
    /// `timestamp_secs`.
    ///
    /// # Panics
    /// Panics if `depth_bits` is not a multiple of 3 in [3, 63].
    pub fn get_boundary_protected_ranges(
        &self,
        center_x: f64, center_y: f64, center_z: f64,
        radius: f64,
        timestamp_secs: u64,
        depth_bits: usize,
    ) -> Vec<([u8; 20], [u8; 20])> {
        assert!(
            depth_bits >= 3 && depth_bits <= 63 && depth_bits % 3 == 0,
            "depth_bits must be a multiple of 3 in [3, 63], got {depth_bits}"
        );
        let epoch = (timestamp_secs / self.epoch_duration as u64) as u32;
        let spatial_mask = (1u64 << (64 - depth_bits)) - 1;
        self.compute_spatial_prefixes(center_x, center_y, center_z, radius, depth_bits)
            .into_iter()
            .map(|prefix| {
                let mut s = [0u8; 20];
                s[0..4].copy_from_slice(&epoch.to_be_bytes());
                s[4..12].copy_from_slice(&prefix.to_be_bytes());
                // s[12..20] stays 0u64

                let mut e = [0u8; 20];
                e[0..4].copy_from_slice(&epoch.to_be_bytes());
                e[4..12].copy_from_slice(&(prefix | spatial_mask).to_be_bytes());
                e[12..20].copy_from_slice(&u64::MAX.to_be_bytes());

                (s, e)
            })
            .collect()
    }

    /// Generates range pairs covering every Morton cell that intersects the 3-D
    /// bounding box across a wall-clock time window `[start_secs, end_secs]`.
    ///
    /// Use this for Kalman prediction queries where you need historical
    /// observations (e.g. "last seen in this region over the past 2 hours")
    /// rather than a single-epoch snapshot.
    ///
    /// ## How epoch boundaries are handled
    ///
    /// Spatial prefixes are computed once and stamped onto every epoch bucket in
    /// `[start_epoch, end_epoch]`.  The nano-offset (nanosecond) field is
    /// clamped on the **first** and **last** epoch to avoid pulling observations
    /// outside `[start_secs, end_secs]`:
    ///
    /// ```text
    /// epoch_n   [start_nano ──────────────────── MAX ]
    /// epoch_n+1 [0 ────────────────────────────── MAX ]
    /// …
    /// epoch_m   [0 ──────────────────── end_nano ]
    /// ```
    ///
    /// Note: for Morton cells whose code falls strictly *between* a prefix and
    /// `prefix | mask`, all nano-offsets within that epoch are included even at
    /// boundary epochs.  Callers should apply a final timestamp filter after
    /// decoding keys if sub-epoch precision is required.
    ///
    /// ## Fan-out
    ///
    /// Total ranges = `spatial_cells × epoch_count`.  Keep `epoch_count` small
    /// (e.g. ≤ 48 for a 30-minute epoch over 24 h) or widen `depth_bits` via
    /// `calculate_optimal_depth` to keep spatial cells ≤ 8.
    ///
    /// # Panics
    /// Panics if `depth_bits` is not a multiple of 3 in [3, 63], or
    /// `start_secs > end_secs`.
    pub fn get_boundary_protected_ranges_time_window(
        &self,
        center_x: f64, center_y: f64, center_z: f64,
        radius: f64,
        start_secs: u64,
        end_secs: u64,
        depth_bits: usize,
    ) -> Vec<([u8; 20], [u8; 20])> {
        assert!(
            depth_bits >= 3 && depth_bits <= 63 && depth_bits % 3 == 0,
            "depth_bits must be a multiple of 3 in [3, 63], got {depth_bits}"
        );
        assert!(start_secs <= end_secs, "start_secs must be ≤ end_secs");

        let d = self.epoch_duration as u64;
        let start_epoch  = (start_secs / d) as u32;
        let end_epoch    = (end_secs   / d) as u32;
        let start_nano   = (start_secs % d) * 1_000_000_000;
        let end_nano     = (end_secs   % d) * 1_000_000_000;
        let spatial_mask = (1u64 << (64 - depth_bits)) - 1;

        let prefixes =
            self.compute_spatial_prefixes(center_x, center_y, center_z, radius, depth_bits);

        let epoch_count = (end_epoch - start_epoch + 1) as usize;
        let mut ranges = Vec::with_capacity(prefixes.len() * epoch_count);

        for epoch in start_epoch..=end_epoch {
            let t_lo = if epoch == start_epoch { start_nano } else { 0 };
            let t_hi = if epoch == end_epoch   { end_nano   } else { u64::MAX };

            for &prefix in &prefixes {
                let mut s = [0u8; 20];
                s[0..4].copy_from_slice(&epoch.to_be_bytes());
                s[4..12].copy_from_slice(&prefix.to_be_bytes());
                s[12..20].copy_from_slice(&t_lo.to_be_bytes());

                let mut e = [0u8; 20];
                e[0..4].copy_from_slice(&epoch.to_be_bytes());
                e[4..12].copy_from_slice(&(prefix | spatial_mask).to_be_bytes());
                e[12..20].copy_from_slice(&t_hi.to_be_bytes());

                ranges.push((s, e));
            }
        }
        ranges
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    fn indexer() -> HybridSpatioTemporalIndexer {
        HybridSpatioTemporalIndexer::new(1000.0, 3600)
    }

    // -----------------------------------------------------------------------
    // Coordinate normalization
    // -----------------------------------------------------------------------

    #[test]
    fn normalize_origin_is_midpoint() {
        let idx = indexer();
        let mid = (1u32 << 21) / 2; // 1_048_576
        // 0.0 should map to the midpoint ±1 due to rounding
        let result = idx.normalize_coordinate(0.0);
        assert!((result as i64 - mid as i64).abs() <= 1);
    }

    #[test]
    fn normalize_negative_max_is_zero() {
        let idx = indexer();
        assert_eq!(idx.normalize_coordinate(-1000.0), 0);
    }

    #[test]
    fn normalize_positive_max_is_full() {
        let idx = indexer();
        assert_eq!(idx.normalize_coordinate(1000.0), (1u32 << 21) - 1);
    }

    #[test]
    fn normalize_clamps_out_of_range() {
        let idx = indexer();
        assert_eq!(idx.normalize_coordinate(-9999.0), 0);
        assert_eq!(idx.normalize_coordinate(9999.0), (1u32 << 21) - 1);
    }

    #[test]
    fn normalize_denormalize_roundtrip_interior() {
        let idx = indexer();
        for &v in &[-999.0, -500.0, -1.0, 0.0, 1.0, 500.0, 999.0] {
            let n = idx.normalize_coordinate(v);
            let back = idx.denormalize_coordinate(n);
            // 21-bit quantisation error ≤ range / 2^21 ≈ 0.001
            assert_relative_eq!(back, v, epsilon = 0.002);
        }
    }

    #[test]
    fn denormalize_zero_is_negative_max() {
        let idx = indexer();
        assert_relative_eq!(idx.denormalize_coordinate(0), -1000.0, epsilon = 1e-9);
    }

    #[test]
    fn denormalize_full_is_positive_max() {
        let idx = indexer();
        let max_21bit = (1u32 << 21) - 1;
        assert_relative_eq!(idx.denormalize_coordinate(max_21bit), 1000.0, epsilon = 1e-9);
    }

    // -----------------------------------------------------------------------
    // Morton encoding / decoding
    // -----------------------------------------------------------------------

    #[test]
    fn morton_zero_roundtrip() {
        use super::HybridSpatioTemporalIndexer as H;
        assert_eq!(H::decode_3d_morton(H::encode_3d_morton(0, 0, 0)), (0, 0, 0));
    }

    #[test]
    fn morton_max_roundtrip() {
        use super::HybridSpatioTemporalIndexer as H;
        let m = (1u32 << 21) - 1;
        assert_eq!(H::decode_3d_morton(H::encode_3d_morton(m, m, m)), (m, m, m));
    }

    #[test]
    fn morton_axis_isolation() {
        use super::HybridSpatioTemporalIndexer as H;
        // Only x=1 set → bit 2 of Morton; only y=1 → bit 1; only z=1 → bit 0
        assert_eq!(H::encode_3d_morton(1, 0, 0), 0b100);
        assert_eq!(H::encode_3d_morton(0, 1, 0), 0b010);
        assert_eq!(H::encode_3d_morton(0, 0, 1), 0b001);
    }

    #[test]
    fn morton_known_value() {
        use super::HybridSpatioTemporalIndexer as H;
        // x=0b11, y=0b11, z=0b11 → each pair interleaves to 0b111111
        assert_eq!(H::encode_3d_morton(3, 3, 3), 0b111111);
    }

    #[test]
    fn morton_roundtrip_arbitrary() {
        use super::HybridSpatioTemporalIndexer as H;
        let cases = [(123456, 654321, 111111), (0, 0, 1), (1, 0, 0), (524287, 0, 524287)];
        for (x, y, z) in cases {
            assert_eq!(H::decode_3d_morton(H::encode_3d_morton(x, y, z)), (x, y, z));
        }
    }

    #[test]
    fn morton_uses_only_63_bits() {
        use super::HybridSpatioTemporalIndexer as H;
        let m = (1u32 << 21) - 1;
        let code = H::encode_3d_morton(m, m, m);
        // Bit 63 must be zero (we pack 3×21=63 bits into bits 0..62)
        assert_eq!(code >> 63, 0);
    }

    // -----------------------------------------------------------------------
    // Key generation and decoding
    // -----------------------------------------------------------------------

    #[test]
    fn key_is_20_bytes() {
        let idx = indexer();
        let key = idx.generate_key(1.0, 2.0, 3.0, 1_000_000, 500_000_000);
        assert_eq!(key.len(), 20);
    }

    #[test]
    fn key_roundtrip_exact_position() {
        let idx = indexer();
        // Use values that map cleanly through 21-bit quantisation
        let key = idx.generate_key(0.0, 0.0, 0.0, 7200, 0);
        let (x, y, z, secs, nanos) = idx.decode_key(&key);
        assert_relative_eq!(x, 0.0, epsilon = 0.002);
        assert_relative_eq!(y, 0.0, epsilon = 0.002);
        assert_relative_eq!(z, 0.0, epsilon = 0.002);
        assert_eq!(secs, 7200);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn key_roundtrip_with_sub_second_nanos() {
        let idx = indexer();
        let key = idx.generate_key(100.0, -200.0, 300.0, 3601, 123_456_789);
        let (_, _, _, secs, nanos) = idx.decode_key(&key);
        assert_eq!(secs, 3601);
        assert_eq!(nanos, 123_456_789);
    }

    #[test]
    fn epoch_bucket_field_is_correct() {
        let idx = indexer(); // epoch_duration = 3600
        let key = idx.generate_key(0.0, 0.0, 0.0, 7200, 0); // 7200s = epoch 2
        let epoch = u32::from_be_bytes(key[0..4].try_into().unwrap());
        assert_eq!(epoch, 2);
    }

    #[test]
    fn key_epoch_boundary_transition() {
        let idx = indexer();
        // One second before and after epoch boundary at 3600s
        let key_before = idx.generate_key(0.0, 0.0, 0.0, 3599, 999_999_999);
        let key_after  = idx.generate_key(0.0, 0.0, 0.0, 3600, 0);

        let epoch_before = u32::from_be_bytes(key_before[0..4].try_into().unwrap());
        let epoch_after  = u32::from_be_bytes(key_after[0..4].try_into().unwrap());
        assert_eq!(epoch_before, 0);
        assert_eq!(epoch_after, 1);
    }

    // -----------------------------------------------------------------------
    // Big-endian lexicographic sort order
    // -----------------------------------------------------------------------

    #[test]
    fn keys_sort_by_epoch_first() {
        let idx = indexer();
        let k0 = idx.generate_key(500.0, 500.0, 500.0, 0, 0);     // epoch 0
        let k1 = idx.generate_key(-500.0, -500.0, -500.0, 3600, 0); // epoch 1
        // Epoch 0 key must sort before epoch 1 key, regardless of spatial code
        assert!(k0 < k1, "epoch 0 key should be less than epoch 1 key");
    }

    #[test]
    fn keys_within_epoch_sort_by_spatial_then_time() {
        let idx = indexer();
        // Two points in the same epoch; the one with lower Morton code should sort first
        let k_origin = idx.generate_key(0.0, 0.0, 0.0, 100, 999_999_999);
        let k_far    = idx.generate_key(999.0, 999.0, 999.0, 100, 0);
        let morton_origin = u64::from_be_bytes(k_origin[4..12].try_into().unwrap());
        let morton_far    = u64::from_be_bytes(k_far[4..12].try_into().unwrap());
        if morton_origin < morton_far {
            assert!(k_origin < k_far);
        } else {
            assert!(k_far < k_origin);
        }
    }

    #[test]
    fn same_spatial_different_time_sorts_chronologically() {
        let idx = indexer();
        let k_early = idx.generate_key(50.0, 50.0, 50.0, 100, 0);
        let k_late  = idx.generate_key(50.0, 50.0, 50.0, 200, 0);
        assert!(k_early < k_late);
    }

    // -----------------------------------------------------------------------
    // Optimal depth calculation
    // -----------------------------------------------------------------------

    #[test]
    fn optimal_depth_full_range_returns_minimum() {
        let idx = indexer();
        assert_eq!(idx.calculate_optimal_depth(1000.0), 3);
    }

    #[test]
    fn optimal_depth_is_multiple_of_3() {
        let idx = indexer();
        for &r in &[1.0, 10.0, 50.0, 100.0, 500.0, 999.0] {
            let d = idx.calculate_optimal_depth(r);
            assert_eq!(d % 3, 0, "depth {d} for radius {r} is not a multiple of 3");
        }
    }

    #[test]
    fn optimal_depth_bounded() {
        let idx = indexer();
        for &r in &[0.001, 0.1, 1.0, 100.0, 1000.0] {
            let d = idx.calculate_optimal_depth(r);
            assert!(d >= 3 && d <= 63, "depth {d} out of [3,63] for radius {r}");
        }
    }

    #[test]
    fn optimal_depth_decreases_with_larger_radius() {
        let idx = indexer();
        let d_small = idx.calculate_optimal_depth(1.0);
        let d_large = idx.calculate_optimal_depth(500.0);
        assert!(d_small >= d_large, "smaller radius should yield >= depth bits");
    }

    // -----------------------------------------------------------------------
    // Boundary-protected range generation
    // -----------------------------------------------------------------------

    #[test]
    fn range_count_is_bounded() {
        let idx = indexer();
        let depth = idx.calculate_optimal_depth(50.0);
        let ranges = idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 50.0, 3600, depth);
        assert!(
            ranges.len() <= 27,
            "expected ≤ 27 ranges (3³), got {}",
            ranges.len()
        );
    }

    #[test]
    fn ranges_are_disjoint_and_sorted() {
        let idx = indexer();
        let depth = idx.calculate_optimal_depth(100.0);
        let ranges = idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 100.0, 0, depth);
        for w in ranges.windows(2) {
            // end of previous range must be strictly less than start of next
            assert!(w[0].1 < w[1].0, "ranges overlap or are out of order");
        }
    }

    #[test]
    fn range_keys_are_20_bytes() {
        let idx = indexer();
        let ranges = idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 10.0, 0, 9);
        for (start, end) in &ranges {
            assert_eq!(start.len(), 20);
            assert_eq!(end.len(), 20);
        }
    }

    #[test]
    fn range_start_le_end() {
        let idx = indexer();
        let ranges = idx.get_boundary_protected_ranges(50.0, -50.0, 0.0, 20.0, 3600, 12);
        for (start, end) in &ranges {
            assert!(start <= end, "start key must be ≤ end key");
        }
    }

    #[test]
    fn generated_key_falls_within_a_range() {
        let idx = indexer();
        let cx = 100.0_f64;
        let cy = -50.0_f64;
        let cz = 25.0_f64;
        let radius = 30.0_f64;
        let ts = 7200u64;
        let depth = idx.calculate_optimal_depth(radius);

        let ranges = idx.get_boundary_protected_ranges(cx, cy, cz, radius, ts, depth);
        let probe_key = idx.generate_key(cx, cy, cz, ts + 10, 0);

        let covered = ranges.iter().any(|(s, e)| probe_key >= *s && probe_key <= *e);
        assert!(covered, "key for center point must fall within one of the query ranges");
    }

    #[test]
    fn near_max_range_corner_key_is_covered() {
        let idx = indexer();
        let radius = 50.0;
        let depth = idx.calculate_optimal_depth(radius);
        // Corner near max_range boundary
        let ranges = idx.get_boundary_protected_ranges(990.0, 990.0, 990.0, radius, 0, depth);
        let probe = idx.generate_key(995.0, 995.0, 995.0, 0, 0);
        let covered = ranges.iter().any(|(s, e)| probe >= *s && probe <= *e);
        assert!(covered, "corner point near max_range must be covered");
    }

    #[test]
    fn depth_3_gives_single_range() {
        // At depth_bits=3 (1 bit per axis, 2 cells per axis), a small central
        // search may fall entirely within one octant = 1 range.
        let idx = indexer();
        let ranges = idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 1.0, 0, 3);
        // Must be at least 1 and at most 8
        assert!(!ranges.is_empty());
        assert!(ranges.len() <= 8);
    }

    // -----------------------------------------------------------------------
    // Time-window range generation
    // -----------------------------------------------------------------------

    #[test]
    fn time_window_single_epoch_matches_point_query() {
        // A window entirely within one epoch must produce the same spatial
        // prefixes as get_boundary_protected_ranges for that epoch.
        let idx = indexer();
        let depth = idx.calculate_optimal_depth(30.0);

        let single = idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 30.0, 7200, depth);
        let window = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 30.0, 7200, 7299, depth,
        );
        // Same number of ranges (one epoch, same spatial cells).
        assert_eq!(single.len(), window.len());
        // The spatial bytes (bytes 4..12) of each pair must match.
        for (s, w) in single.iter().zip(window.iter()) {
            assert_eq!(s.0[4..12], w.0[4..12], "start spatial mismatch");
            assert_eq!(s.1[4..12], w.1[4..12], "end spatial mismatch");
        }
    }

    #[test]
    fn time_window_spans_correct_epoch_count() {
        let idx = indexer(); // epoch_duration = 3600
        let depth = idx.calculate_optimal_depth(50.0);
        // 0 → 10799s = epochs 0, 1, 2  → 3 epochs (10800 would be the start of epoch 3)
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 50.0, 0, 10799, depth,
        );
        let n_spatial = idx
            .get_boundary_protected_ranges(0.0, 0.0, 0.0, 50.0, 0, depth)
            .len();
        assert_eq!(ranges.len(), n_spatial * 3);
    }

    #[test]
    fn time_window_first_epoch_start_nano_is_clamped() {
        let idx = indexer();
        // start_secs = 3700 → epoch 1, nano_offset = 100 * 1e9 = 100_000_000_000
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 10.0, 3700, 7200, 9,
        );
        let first_start = &ranges[0].0;
        let epoch_in_key = u32::from_be_bytes(first_start[0..4].try_into().unwrap());
        let t_lo = u64::from_be_bytes(first_start[12..20].try_into().unwrap());
        assert_eq!(epoch_in_key, 1);
        assert_eq!(t_lo, 100 * 1_000_000_000u64); // 100s into the epoch
    }

    #[test]
    fn time_window_last_epoch_end_nano_is_clamped() {
        let idx = indexer();
        // end_secs = 7500 → epoch 2, nano_offset = 300 * 1e9
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 10.0, 0, 7500, 9,
        );
        let last_end = &ranges[ranges.len() - 1].1;
        let epoch_in_key = u32::from_be_bytes(last_end[0..4].try_into().unwrap());
        let t_hi = u64::from_be_bytes(last_end[12..20].try_into().unwrap());
        assert_eq!(epoch_in_key, 2);
        assert_eq!(t_hi, 300 * 1_000_000_000u64); // 300s into the epoch
    }

    #[test]
    fn time_window_middle_epochs_use_full_time_range() {
        let idx = indexer();
        // Window over epochs 0, 1, 2 — epoch 1 is the middle.
        let depth = 9usize;
        let n_spatial = idx
            .get_boundary_protected_ranges(0.0, 0.0, 0.0, 10.0, 0, depth)
            .len();
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 10.0, 1800, 9000, depth,
        );
        // Middle epoch = epoch 1; its ranges are at index n_spatial..2*n_spatial
        for i in n_spatial..2 * n_spatial {
            let t_lo = u64::from_be_bytes(ranges[i].0[12..20].try_into().unwrap());
            let t_hi = u64::from_be_bytes(ranges[i].1[12..20].try_into().unwrap());
            assert_eq!(t_lo, 0, "middle epoch t_lo must be 0");
            assert_eq!(t_hi, u64::MAX, "middle epoch t_hi must be MAX");
        }
    }

    #[test]
    fn time_window_generated_key_covered_across_epochs() {
        let idx = indexer();
        let radius = 20.0_f64;
        let depth = idx.calculate_optimal_depth(radius);

        // Observations scattered over three epochs
        let probes = [
            idx.generate_key(5.0, 5.0, 5.0, 500,   0),  // epoch 0
            idx.generate_key(5.0, 5.0, 5.0, 4000,  0),  // epoch 1
            idx.generate_key(5.0, 5.0, 5.0, 7700,  0),  // epoch 2
        ];

        let ranges = idx.get_boundary_protected_ranges_time_window(
            5.0, 5.0, 5.0, radius, 0, 10800, depth,
        );

        for probe in &probes {
            let covered = ranges.iter().any(|(s, e)| probe >= s && probe <= e);
            assert!(covered, "probe key at secs≈{} not covered", {
                let (_, _, _, secs, _) = idx.decode_key(probe);
                secs
            });
        }
    }

    #[test]
    fn time_window_key_before_start_excluded() {
        let idx = indexer();
        // start_secs=3700 → epoch 1, t_lo = 100s * 1e9.
        // A key at (epoch=1, same spatial, t=50s) must NOT be covered.
        let depth = 9usize;
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 10.0, 3700, 7200, depth,
        );
        // Key for origin at epoch=1, nano=50s (before the 100s start_nano)
        let early_key = idx.generate_key(0.0, 0.0, 0.0, 3600 + 50, 0);
        // The epoch is correct but the nano_offset is before start_nano.
        // Because the origin maps to the first spatial prefix in the range,
        // the start_key nano clamp must exclude it.
        let epoch_of_key = u32::from_be_bytes(early_key[0..4].try_into().unwrap());
        assert_eq!(epoch_of_key, 1);

        // Find the range that covers epoch 1 and contains origin's Morton prefix.
        let morton = u64::from_be_bytes(early_key[4..12].try_into().unwrap());
        if let Some((s, _)) = ranges.iter().find(|(s, e)| {
            u32::from_be_bytes(s[0..4].try_into().unwrap()) == 1
                && u64::from_be_bytes(s[4..12].try_into().unwrap())
                    <= morton
                && morton
                    <= u64::from_be_bytes(e[4..12].try_into().unwrap())
        }) {
            let range_t_lo = u64::from_be_bytes(s[12..20].try_into().unwrap());
            let key_t      = u64::from_be_bytes(early_key[12..20].try_into().unwrap());
            // The clamp means range_t_lo > key_t, so early_key < start_key.
            assert!(
                range_t_lo > key_t,
                "start_nano clamp must exceed the early key's nano-offset"
            );
        }
        // (If no range covers that Morton prefix at all that's also acceptable.)
    }

    #[test]
    fn time_window_ranges_are_20_bytes() {
        let idx = indexer();
        let ranges = idx.get_boundary_protected_ranges_time_window(
            0.0, 0.0, 0.0, 50.0, 0, 10800, 9,
        );
        for (s, e) in &ranges {
            assert_eq!(s.len(), 20);
            assert_eq!(e.len(), 20);
        }
    }

    #[test]
    fn time_window_start_le_end_all_pairs() {
        let idx = indexer();
        let ranges = idx.get_boundary_protected_ranges_time_window(
            100.0, -50.0, 25.0, 40.0, 1800, 14400, 12,
        );
        for (s, e) in &ranges {
            assert!(s <= e, "start key must be ≤ end key in every range pair");
        }
    }

    #[test]
    #[should_panic]
    fn time_window_start_after_end_panics() {
        let idx = indexer();
        idx.get_boundary_protected_ranges_time_window(0.0, 0.0, 0.0, 10.0, 7200, 3600, 9);
    }

    #[test]
    #[should_panic]
    fn invalid_depth_bits_not_multiple_of_3_panics() {
        let idx = indexer();
        idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 10.0, 0, 4);
    }

    #[test]
    #[should_panic]
    fn invalid_depth_bits_exceeds_63_panics() {
        let idx = indexer();
        idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 10.0, 0, 66);
    }

    #[test]
    #[should_panic]
    fn invalid_depth_bits_zero_panics() {
        let idx = indexer();
        idx.get_boundary_protected_ranges(0.0, 0.0, 0.0, 10.0, 0, 0);
    }
}
