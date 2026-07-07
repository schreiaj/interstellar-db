//! Merkle octree over Morton-coded keys for anti-entropy sync between peers.
//!
//! One tree per macro epoch (a *forest*), mirroring the epoch-major key layout:
//! frozen epochs stabilize to a single root hash, so agreeing on history is one
//! compare per epoch, and churn concentrates in the live epoch.
//!
//! ## Hashing scheme
//!
//! Every node hash is the XOR of the 128-bit digests of all items in its
//! subtree — a commutative, order-independent set fold. Consequences:
//!
//! - Gossip delivery order doesn't matter: identical sets hash identically.
//! - Insert/remove touch one `u128` per level (XOR is self-inverse), never
//!   re-reading bucket contents.
//! - A node's hash is *structure-independent*: `node_hash(epoch, depth, prefix)`
//!   is well-defined for any depth, so peers whose trees use different leaf
//!   depths can still compare — diffing descends to the shallower of the two.
//! - An absent node hashes to 0, which is also the hash of an empty set, so
//!   "missing" and "empty" compare equal, as they should.
//!
//! Item digests are SHA-256 over `key ‖ payload` truncated to 128 bits. The
//! payload is included so two peers holding different payloads under the same
//! key diverge visibly; resolution policy lives with the storage layer.
//!
//! ## Prefix convention
//!
//! Depths are `depth_bits` (multiple of 3 in [3, 63]) and a cell prefix at
//! depth `d` is `(morton >> (64 - d)) << (64 - d)` — identical to the range
//! planners in the indexer, so a divergent leaf maps directly onto a KV key
//! range via [`leaf_key_range`].

use std::collections::{BTreeMap, HashMap};

use sha2::{Digest as _, Sha256};

/// XOR-foldable 128-bit node/item hash.
pub type NodeHash = u128;

/// Digest of a single stored item, foldable into any node that contains it.
pub fn item_digest(key: &[u8; 20], payload: &[u8]) -> NodeHash {
    let mut h = Sha256::new();
    h.update(key);
    h.update(payload);
    let out = h.finalize();
    u128::from_be_bytes(out[..16].try_into().unwrap())
}

fn prefix_at(morton: u64, depth_bits: usize) -> u64 {
    let shift = 64 - depth_bits;
    (morton >> shift) << shift
}

fn split_key(key: &[u8; 20]) -> (u32, u64) {
    let epoch = u32::from_be_bytes(key[0..4].try_into().unwrap());
    let morton = u64::from_be_bytes(key[4..12].try_into().unwrap());
    (epoch, morton)
}

/// Inclusive 20-byte key range covering one Morton cell within one epoch.
///
/// Matches the layout produced by `get_boundary_protected_ranges`: full
/// nano-offset span, spatial bits from `prefix` through `prefix | mask`.
pub fn leaf_key_range(epoch: u32, prefix: u64, depth_bits: usize) -> ([u8; 20], [u8; 20]) {
    let spatial_mask = (1u64 << (64 - depth_bits)) - 1;

    let mut s = [0u8; 20];
    s[0..4].copy_from_slice(&epoch.to_be_bytes());
    s[4..12].copy_from_slice(&prefix.to_be_bytes());

    let mut e = [0u8; 20];
    e[0..4].copy_from_slice(&epoch.to_be_bytes());
    e[4..12].copy_from_slice(&(prefix | spatial_mask).to_be_bytes());
    e[12..20].copy_from_slice(&u64::MAX.to_be_bytes());

    (s, e)
}

// ---------------------------------------------------------------------------
// Per-epoch tree
// ---------------------------------------------------------------------------

/// Sparse Merkle octree for a single macro epoch.
///
/// `levels[i]` holds the XOR accumulators for cells at `depth_bits = (i+1)*3`;
/// the root (depth 0) is kept separately. Cells never present hash to 0.
#[derive(Debug, Clone)]
struct EpochTree {
    root: NodeHash,
    count: u64,
    levels: Vec<HashMap<u64, NodeHash>>,
}

impl EpochTree {
    fn new(leaf_depth_bits: usize) -> Self {
        Self {
            root: 0,
            count: 0,
            levels: vec![HashMap::new(); leaf_depth_bits / 3],
        }
    }

    /// XOR `digest` into every cell on the root-to-leaf path. Self-inverse:
    /// folding the same digest twice removes it.
    fn toggle(&mut self, morton: u64, digest: NodeHash) {
        self.root ^= digest;
        for (i, level) in self.levels.iter_mut().enumerate() {
            let depth_bits = (i + 1) * 3;
            *level.entry(prefix_at(morton, depth_bits)).or_insert(0) ^= digest;
        }
    }

    fn node_hash(&self, depth_bits: usize, prefix: u64) -> NodeHash {
        if depth_bits == 0 {
            return self.root;
        }
        self.levels[depth_bits / 3 - 1]
            .get(&prefix)
            .copied()
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Forest
// ---------------------------------------------------------------------------

/// A Merkle octree per macro epoch, keyed by the epoch bucket in the key.
#[derive(Debug, Clone)]
pub struct MerkleForest {
    leaf_depth_bits: usize,
    trees: BTreeMap<u32, EpochTree>,
}

impl MerkleForest {
    /// # Panics
    /// Panics if `leaf_depth_bits` is not a multiple of 3 in [3, 63].
    pub fn new(leaf_depth_bits: usize) -> Self {
        assert!(
            leaf_depth_bits >= 3 && leaf_depth_bits <= 63 && leaf_depth_bits % 3 == 0,
            "leaf_depth_bits must be a multiple of 3 in [3, 63], got {leaf_depth_bits}"
        );
        Self { leaf_depth_bits, trees: BTreeMap::new() }
    }

    pub fn leaf_depth_bits(&self) -> usize {
        self.leaf_depth_bits
    }

    /// Total items folded in across all epochs.
    pub fn len(&self) -> u64 {
        self.trees.values().map(|t| t.count).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fold an item in. O(levels) hashmap XORs; no reads of prior contents.
    pub fn insert(&mut self, key: &[u8; 20], payload: &[u8]) {
        let (epoch, morton) = split_key(key);
        let digest = item_digest(key, payload);
        let tree = self
            .trees
            .entry(epoch)
            .or_insert_with(|| EpochTree::new(self.leaf_depth_bits));
        tree.toggle(morton, digest);
        tree.count += 1;
    }

    /// Fold an item back out (e.g. superseded payload, epoch expiry).
    /// Must be called with exactly the key/payload that was inserted.
    pub fn remove(&mut self, key: &[u8; 20], payload: &[u8]) {
        let (epoch, morton) = split_key(key);
        let digest = item_digest(key, payload);
        if let Some(tree) = self.trees.get_mut(&epoch) {
            tree.toggle(morton, digest);
            tree.count = tree.count.saturating_sub(1);
        }
    }

    /// All epochs with a tree present (including ones folded back to empty).
    pub fn epochs(&self) -> Vec<u32> {
        self.trees.keys().copied().collect()
    }

    /// Serialize one epoch's tree to a compact seal record: its leaf hashes
    /// plus item count. Upper levels are omitted — they rebuild from the
    /// leaves by XOR-fold in [`Self::load_sealed_epoch`] without touching the
    /// underlying data. Returns `None` for an absent or empty epoch.
    ///
    /// Format (all big-endian):
    /// `[version u8 = 1][leaf_depth_bits u8][count u64][n u32][(prefix u64, hash u128) × n]`
    pub fn serialize_epoch(&self, epoch: u32) -> Option<Vec<u8>> {
        let tree = self.trees.get(&epoch)?;
        let leaves = &tree.levels[self.leaf_depth_bits / 3 - 1];
        let mut entries: Vec<(u64, NodeHash)> = leaves
            .iter()
            .filter(|&(_, &h)| h != 0)
            .map(|(&p, &h)| (p, h))
            .collect();
        if entries.is_empty() {
            return None;
        }
        entries.sort_unstable_by_key(|&(p, _)| p);

        let mut out = Vec::with_capacity(2 + 8 + 4 + entries.len() * 24);
        out.push(1u8);
        out.push(self.leaf_depth_bits as u8);
        out.extend_from_slice(&tree.count.to_be_bytes());
        out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
        for (prefix, hash) in entries {
            out.extend_from_slice(&prefix.to_be_bytes());
            out.extend_from_slice(&hash.to_be_bytes());
        }
        Some(out)
    }

    /// Reconstruct a sealed epoch's tree from a [`Self::serialize_epoch`]
    /// record, folding each leaf hash up through every level.
    ///
    /// Fails (leaving the forest untouched) if the record is malformed, was
    /// sealed at a different leaf depth, or the epoch is already populated —
    /// callers should then fall back to rescanning that epoch's data.
    pub fn load_sealed_epoch(&mut self, epoch: u32, bytes: &[u8]) -> Result<(), String> {
        if self.trees.contains_key(&epoch) {
            return Err(format!("epoch {epoch} already populated"));
        }
        if bytes.len() < 14 {
            return Err("seal record too short".to_string());
        }
        if bytes[0] != 1 {
            return Err(format!("unsupported seal record version {}", bytes[0]));
        }
        if bytes[1] as usize != self.leaf_depth_bits {
            return Err(format!(
                "seal record leaf depth {} does not match forest leaf depth {}",
                bytes[1], self.leaf_depth_bits
            ));
        }
        let count = u64::from_be_bytes(bytes[2..10].try_into().unwrap());
        let n = u32::from_be_bytes(bytes[10..14].try_into().unwrap()) as usize;
        if bytes.len() != 14 + n * 24 {
            return Err(format!(
                "seal record length {} does not match {n} entries",
                bytes.len()
            ));
        }

        let mut tree = EpochTree::new(self.leaf_depth_bits);
        for i in 0..n {
            let off = 14 + i * 24;
            let prefix = u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap());
            let hash = u128::from_be_bytes(bytes[off + 8..off + 24].try_into().unwrap());
            // Leaf prefixes have zeroed low bits, so toggling by the prefix
            // itself lands each hash in the right cell at every level.
            tree.toggle(prefix, hash);
        }
        tree.count = count;
        self.trees.insert(epoch, tree);
        Ok(())
    }

    /// Root hash per epoch — the summary two peers exchange first.
    pub fn epoch_roots(&self) -> Vec<(u32, NodeHash)> {
        self.trees
            .iter()
            .filter(|(_, t)| t.root != 0)
            .map(|(&e, t)| (e, t.root))
            .collect()
    }

    /// Hash of the cell `(depth_bits, prefix)` in `epoch`; 0 if empty/absent.
    /// `depth_bits == 0` returns the epoch root.
    ///
    /// # Panics
    /// Panics if `depth_bits` exceeds `leaf_depth_bits` or is not a multiple of 3.
    pub fn node_hash(&self, epoch: u32, depth_bits: usize, prefix: u64) -> NodeHash {
        assert!(
            depth_bits <= self.leaf_depth_bits && depth_bits % 3 == 0,
            "depth_bits must be a multiple of 3 ≤ {}, got {depth_bits}",
            self.leaf_depth_bits
        );
        self.trees
            .get(&epoch)
            .map(|t| t.node_hash(depth_bits, prefix))
            .unwrap_or(0)
    }

    /// The ≤ 8 non-empty children of `(depth_bits, prefix)`, one level deeper.
    /// This is the unit a sync wire protocol exchanges per descent step.
    pub fn child_hashes(
        &self,
        epoch: u32,
        depth_bits: usize,
        prefix: u64,
    ) -> Vec<(u64, NodeHash)> {
        let child_depth = depth_bits + 3;
        (0..8u64)
            .filter_map(|c| {
                let child_prefix = prefix | (c << (64 - child_depth));
                let h = self.node_hash(epoch, child_depth, child_prefix);
                (h != 0).then_some((child_prefix, h))
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Diffing
// ---------------------------------------------------------------------------

/// A leaf cell where two forests disagree, with its ready-to-scan key range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivergentLeaf {
    pub epoch: u32,
    pub depth_bits: usize,
    pub prefix: u64,
    pub start_key: [u8; 20],
    pub end_key: [u8; 20],
}

/// Compare two forests and return every divergent leaf cell.
///
/// Descends only into subtrees whose hashes differ, to the shallower of the
/// two forests' leaf depths (node hashes are structure-independent, so mixed
/// depths compare correctly). Peers then exchange the rows in each returned
/// key range; because merge is idempotent set-union, over-shipping is safe.
pub fn diff_forests(a: &MerkleForest, b: &MerkleForest) -> Vec<DivergentLeaf> {
    let leaf_depth = a.leaf_depth_bits.min(b.leaf_depth_bits);
    let mut out = Vec::new();

    let mut epochs: Vec<u32> = a.trees.keys().chain(b.trees.keys()).copied().collect();
    epochs.sort_unstable();
    epochs.dedup();

    for epoch in epochs {
        if a.node_hash(epoch, 0, 0) != b.node_hash(epoch, 0, 0) {
            descend(a, b, epoch, 0, 0, leaf_depth, &mut out);
        }
    }
    out
}

fn descend(
    a: &MerkleForest,
    b: &MerkleForest,
    epoch: u32,
    depth_bits: usize,
    prefix: u64,
    leaf_depth: usize,
    out: &mut Vec<DivergentLeaf>,
) {
    if depth_bits == leaf_depth {
        let (start_key, end_key) = leaf_key_range(epoch, prefix, depth_bits);
        out.push(DivergentLeaf { epoch, depth_bits, prefix, start_key, end_key });
        return;
    }
    let child_depth = depth_bits + 3;
    for c in 0..8u64 {
        let child_prefix = prefix | (c << (64 - child_depth));
        if a.node_hash(epoch, child_depth, child_prefix)
            != b.node_hash(epoch, child_depth, child_prefix)
        {
            descend(a, b, epoch, child_depth, child_prefix, leaf_depth, out);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HybridSpatioTemporalIndexer;

    fn indexer() -> HybridSpatioTemporalIndexer {
        HybridSpatioTemporalIndexer::new(1000.0, 3600)
    }

    fn key(x: f64, y: f64, z: f64, secs: u64) -> [u8; 20] {
        indexer().generate_key(x, y, z, secs, 0)
    }

    #[test]
    fn empty_forest_has_no_roots() {
        let f = MerkleForest::new(15);
        assert!(f.epoch_roots().is_empty());
        assert!(f.is_empty());
        assert_eq!(f.node_hash(0, 0, 0), 0);
    }

    #[test]
    fn insert_order_does_not_matter() {
        let items: Vec<([u8; 20], &[u8])> = vec![
            (key(1.0, 2.0, 3.0, 100), b"a".as_slice()),
            (key(-500.0, 10.0, 900.0, 200), b"b"),
            (key(0.0, 0.0, 0.0, 300), b"c"),
            (key(999.0, -999.0, 0.5, 3700), b"d"), // second epoch
        ];
        let mut f1 = MerkleForest::new(15);
        let mut f2 = MerkleForest::new(15);
        for (k, p) in &items {
            f1.insert(k, p);
        }
        for (k, p) in items.iter().rev() {
            f2.insert(k, p);
        }
        assert_eq!(f1.epoch_roots(), f2.epoch_roots());
        assert!(diff_forests(&f1, &f2).is_empty());
    }

    #[test]
    fn remove_is_self_inverse() {
        let mut f = MerkleForest::new(15);
        let empty_roots = f.epoch_roots();
        let k = key(5.0, 5.0, 5.0, 42);
        f.insert(&k, b"payload");
        assert_ne!(f.epoch_roots(), empty_roots);
        f.remove(&k, b"payload");
        assert_eq!(f.epoch_roots(), empty_roots);
        assert!(f.is_empty());
    }

    #[test]
    fn parent_hash_is_xor_of_children() {
        let mut f = MerkleForest::new(15);
        for i in 0..50 {
            let k = key(
                (i as f64) * 37.0 - 900.0,
                (i as f64) * 13.0 - 300.0,
                (i as f64) * 7.0,
                100 + i,
            );
            f.insert(&k, format!("p{i}").as_bytes());
        }
        for depth in (0..15).step_by(3) {
            // Every non-empty node at `depth` must equal XOR of its children.
            // Check the root and one full level down from each occupied node.
            let parents: Vec<u64> = if depth == 0 {
                vec![0]
            } else {
                (0..8u64 * 8 * 8)
                    .map(|i| i << (64 - depth.max(3)))
                    .filter(|&p| f.node_hash(0, depth, p) != 0)
                    .take(16)
                    .collect()
            };
            for p in parents {
                let parent = f.node_hash(0, depth, p);
                let folded = f
                    .child_hashes(0, depth, p)
                    .iter()
                    .fold(0u128, |acc, (_, h)| acc ^ h);
                assert_eq!(parent, folded, "depth {depth} prefix {p:#x}");
            }
        }
    }

    #[test]
    fn diff_localizes_single_extra_item() {
        let mut a = MerkleForest::new(15);
        let mut b = MerkleForest::new(15);
        for i in 0..100 {
            let k = key((i as f64) * 19.0 - 950.0, (i as f64) * -11.0 + 550.0, 3.0, 500);
            a.insert(&k, b"shared");
            b.insert(&k, b"shared");
        }
        let extra = key(123.0, -456.0, 789.0, 600);
        a.insert(&extra, b"only-in-a");

        let diffs = diff_forests(&a, &b);
        assert_eq!(diffs.len(), 1, "one extra item must yield one divergent leaf");
        let d = &diffs[0];
        assert!(
            extra >= d.start_key && extra <= d.end_key,
            "divergent range must cover the extra item's key"
        );
        // Symmetric.
        assert_eq!(diff_forests(&b, &a), diffs);
    }

    #[test]
    fn diff_flags_epoch_missing_entirely() {
        let mut a = MerkleForest::new(15);
        let b = MerkleForest::new(15);
        let k = key(1.0, 1.0, 1.0, 10 * 3600); // epoch 10
        a.insert(&k, b"x");
        let diffs = diff_forests(&a, &b);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].epoch, 10);
        assert!(k >= diffs[0].start_key && k <= diffs[0].end_key);
    }

    #[test]
    fn same_key_different_payload_diverges() {
        let mut a = MerkleForest::new(15);
        let mut b = MerkleForest::new(15);
        let k = key(0.0, 0.0, 0.0, 100);
        a.insert(&k, b"payload-1");
        b.insert(&k, b"payload-2");
        let diffs = diff_forests(&a, &b);
        assert_eq!(diffs.len(), 1);
        assert!(k >= diffs[0].start_key && k <= diffs[0].end_key);
    }

    #[test]
    fn mixed_leaf_depths_compare_at_shallower_depth() {
        let mut a = MerkleForest::new(9);
        let mut b = MerkleForest::new(21);
        let shared = key(10.0, 20.0, 30.0, 100);
        a.insert(&shared, b"s");
        b.insert(&shared, b"s");
        assert!(diff_forests(&a, &b).is_empty());

        let extra = key(-800.0, 700.0, -600.0, 100);
        b.insert(&extra, b"e");
        let diffs = diff_forests(&a, &b);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].depth_bits, 9, "descends to the shallower leaf depth");
        assert!(extra >= diffs[0].start_key && extra <= diffs[0].end_key);
    }

    #[test]
    fn seal_roundtrip_reproduces_every_node_hash() {
        let mut f = MerkleForest::new(15);
        for i in 0..80u64 {
            let k = key(
                (i as f64) * 29.0 - 950.0,
                (i as f64) * -17.0 + 700.0,
                (i as f64) * 3.0 - 100.0,
                100 + i,
            );
            f.insert(&k, format!("p{i}").as_bytes());
        }
        let bytes = f.serialize_epoch(0).expect("epoch 0 must serialize");

        let mut g = MerkleForest::new(15);
        g.load_sealed_epoch(0, &bytes).unwrap();

        assert_eq!(f.epoch_roots(), g.epoch_roots());
        assert_eq!(f.len(), g.len());
        assert!(diff_forests(&f, &g).is_empty());
        // Sealed trees must remain fully descendable.
        for (p, h) in f.child_hashes(0, 0, 0) {
            assert_eq!(g.node_hash(0, 3, p), h);
        }
    }

    #[test]
    fn sealed_epoch_accepts_later_inserts() {
        let mut f = MerkleForest::new(15);
        let k1 = key(10.0, 10.0, 10.0, 100);
        f.insert(&k1, b"one");
        let bytes = f.serialize_epoch(0).unwrap();

        let mut g = MerkleForest::new(15);
        g.load_sealed_epoch(0, &bytes).unwrap();
        let k2 = key(-300.0, 400.0, 50.0, 200);
        f.insert(&k2, b"two");
        g.insert(&k2, b"two");
        assert_eq!(f.epoch_roots(), g.epoch_roots(), "late insert folds into a loaded tree");
    }

    #[test]
    fn seal_load_rejects_bad_records() {
        let mut f = MerkleForest::new(15);
        f.insert(&key(1.0, 1.0, 1.0, 100), b"x");
        let bytes = f.serialize_epoch(0).unwrap();

        // Depth mismatch.
        let mut g = MerkleForest::new(9);
        assert!(g.load_sealed_epoch(0, &bytes).is_err());
        // Already-populated epoch.
        let mut h = MerkleForest::new(15);
        h.insert(&key(2.0, 2.0, 2.0, 100), b"y");
        assert!(h.load_sealed_epoch(0, &bytes).is_err());
        // Truncated record.
        let mut i = MerkleForest::new(15);
        assert!(i.load_sealed_epoch(0, &bytes[..bytes.len() - 1]).is_err());
        // Empty epoch serializes to None.
        assert!(f.serialize_epoch(99).is_none());
    }

    #[test]
    fn leaf_key_range_matches_indexer_ranges() {
        // The merkle leaf range for the cell containing a point must cover the
        // generated key for that point, using the same prefix math as the
        // indexer's range planner.
        let idx = indexer();
        let k = idx.generate_key(250.0, -125.0, 60.0, 7200, 123);
        let morton = u64::from_be_bytes(k[4..12].try_into().unwrap());
        let epoch = u32::from_be_bytes(k[0..4].try_into().unwrap());
        for depth in [3usize, 9, 15, 21] {
            let prefix = (morton >> (64 - depth)) << (64 - depth);
            let (s, e) = leaf_key_range(epoch, prefix, depth);
            assert!(k >= s && k <= e, "depth {depth}");
        }
    }
}
