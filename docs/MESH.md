# Running InterstellarDB in a mesh

Multiple InterstellarDB nodes can operate as a **leaderless, partition-tolerant
mesh**: every node accepts writes locally and continuously reconciles with the
peers it can reach. There is no coordinator, no region ownership, and no
consensus round on the write path — convergence comes from anti-entropy sync
over a Merkle octree built on the same Morton-coded keys the store already
uses.

## Quick start: a two-node mesh

Each node needs its own database path and port, and a list of peers to pull
from:

```sh
# Terminal 1
DB_PATH=./node-a.db PORT=50051 PEERS=http://[::1]:50052 cargo run --bin server

# Terminal 2
DB_PATH=./node-b.db PORT=50052 PEERS=http://[::1]:50051 cargo run --bin server
```

Write observations to either node; within one sync interval (default 30 s)
both nodes hold the union and answer queries identically. Nodes tolerate peers
being down — an unreachable peer is skipped and retried on the next round, and
a node that was partitioned catches up automatically when contact resumes.

For nodes on different machines, bind beyond loopback:

```sh
BIND_ADDR=[::] PORT=50051 PEERS=http://robot-2.local:50051,http://robot-3.local:50051 \
    cargo run --bin server
```

Peer lists don't need to be symmetric or complete. Sync is **pull-only**: a
node converges by pulling from the peers it lists, and one-way reachability
still makes one-way progress. Any connected topology (ring, star, partial
mesh) eventually converges everywhere, at the cost of more hops of gossip
latency than a full mesh.

## Configuration

| Variable             | Default             | Meaning                                             |
|----------------------|---------------------|-----------------------------------------------------|
| `DB_PATH`            | `./interstellar.db` | On-disk location of this node's store.              |
| `PORT`               | `50051`             | gRPC listen port.                                   |
| `BIND_ADDR`          | `[::1]`             | Listen address; use `[::]` or `0.0.0.0` for a mesh. |
| `PEERS`              | *(empty)*           | Comma-separated gRPC URIs to pull from.             |
| `SYNC_INTERVAL_SECS` | `30`                | Seconds between pull rounds against each peer.      |

## How reconciliation works

### The data model is a CRDT

Observations are immutable 4-D points; the 20-byte key
(`[epoch][3-D Morton][nano offset]`) is derived from the observation itself
and the payload is never mutated in place. Two nodes' stores therefore merge
by **set union** — order-free, conflict-free, idempotent. Receiving the same
row twice is a no-op, so over-shipping during sync is safe by construction.

The one conflict case — the *same* key carrying *different* payloads on two
nodes — resolves deterministically: the payload with the larger item digest
wins on every node, so peers settle on the same winner regardless of exchange
order.

### The Merkle octree finds the differences

Every node maintains a Merkle forest over its contents: **one octree per macro
epoch**, whose cells are Morton prefixes — the same cells the query planner
scans. Node hashes are XOR-folds of per-item digests (SHA-256 of
`key ‖ payload`, truncated to 128 bits), which gives three properties the mesh
depends on:

- **Order independence** — identical sets hash identically no matter what
  order gossip delivered them in.
- **Structure independence** — a node's hash at `(epoch, depth, prefix)` is
  well-defined at any depth, so peers with different tree granularities still
  compare correctly.
- **O(1) updates** — inserting an observation XORs its digest into one cell
  per level; no scans, no rehash of siblings.

A sync round costs traffic proportional to the **divergence**, not the data:

1. `Roots` — compare per-epoch root hashes. Epochs that match (in steady state
   almost all of history) are skipped with a single compare each.
2. `Children` — for each divergent node, fetch its ≤ 8 child hashes and
   recurse only into octants that differ.
3. `Fetch` — at a divergent leaf, stream the raw rows in that cell's key range
   and merge them through the store's idempotent ingest.

### Epoch sealing keeps restarts cheap

The forest lives in memory and is rebuilt on startup. Settled epochs — those
that ended more than a grace period ago — are **sealed**: a compact record of
the epoch's leaf hashes is persisted, so a restart reconstructs that epoch's
tree from the record instead of rescanning its data. Startup cost tracks the
live epoch, not total history.

Sealing is an optimization, never an invariant. A late-arriving observation
for a sealed epoch (routine under store-and-forward delivery) deletes the seal
record *before* the data commit, so a crash between the two just means one
epoch gets rescanned on the next open. The epoch is resealed on a later pass.
The server seals automatically every 10 minutes with a grace of one epoch
duration.

## Semantics and guarantees

- **Eventual consistency.** A write is immediately visible on its own node and
  propagates on sync rounds. Any observation reaching one node reaches every
  node transitively connected to it.
- **Reads are honest but local.** A query answers from the node's current
  contents; under partition that can lag. The temporal axis is the staleness
  mechanism — clients that care should query time ranges wide enough to cover
  their tolerance (see below).
- **`Subscribe` fires for synced data too.** Subscriptions are fed by the
  node's change tap, so an observation reaches matching subscribers whether
  it was written locally or pulled in from a peer. Because a reconnecting
  peer can replay hours of history in one sync round, every subscription
  takes a `max_age_secs` guard: only observations whose own timestamp is
  within that bound at arrival are delivered (`0` = unfiltered). Duplicate
  and losing-conflict deliveries never fire — only rows that actually
  changed the store do.
- **Deletes don't exist.** The store is grow-only; retention (dropping old
  epochs) must be deterministic and mesh-wide to keep forests comparable, and
  is not yet implemented.

## Tuning for your mesh

**Sync interval** (`SYNC_INTERVAL_SECS`) bounds gossip latency per hop. In
steady state a round against an in-sync peer is one `Roots` RPC — a few
hundred bytes — so short intervals are cheap. Budget:
`worst-case propagation ≈ interval × network diameter`.

**Grace period** (currently one epoch duration) is the assumed worst-case
delivery staleness — how late an observation can arrive for an old epoch
without costing a seal. Longer partitions than the grace just mean some
resealing churn, never lost data.

**Clock sync matters for interpretation, not correctness.** Reconciliation
never compares clocks; two nodes converge to the identical set regardless of
skew. But timestamps are producer-claimed, so consumers fusing tracks should
widen query windows by their clock-uncertainty bound (`δ ≈ ε_position / v_max`
for tracked speed `v` and tolerable error `ε`) and reproject with their own
motion model.

**Epoch duration** (fixed at open: 3600 s in the server) trades seal
granularity against per-epoch overhead. It must be identical across the mesh —
it's baked into the keys.

## Protocol reference

The sync service is defined alongside the data service in
[`proto/interstellar.proto`](../proto/interstellar.proto):

| RPC        | Direction | Purpose                                                |
|------------|-----------|--------------------------------------------------------|
| `Roots`    | pull      | Per-epoch root hashes + the peer's Merkle leaf depth.  |
| `Children` | pull      | Non-empty child hashes of one tree node.               |
| `Fetch`    | pull      | Stream raw rows in one divergent leaf's key range.     |

All three are read-only on the serving side; the puller merges via its own
store's ingest. Embedders can drive the same protocol programmatically with
`interstellar_db::sync_once(&store, &mut client)`.

## Current limitations

- **Static peer list** — no discovery or gossip of membership; `PEERS` is
  fixed at startup.
- **No transport security or auth** — run on a trusted network layer
  (WireGuard, mTLS proxy) until TLS is wired into tonic.
- **Full-epoch key coverage** — a divergent leaf ships all its rows even if
  one differs; leaf granularity (`MERKLE_LEAF_DEPTH_BITS`, 32³ cells per
  epoch) bounds the waste.
