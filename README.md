# interstellar_db

A persistent **4-D spatio-temporal store** for point observations in continuous
3-D space and time, served over gRPC. It is built for workloads like object
tracking and sensor fusion, where you repeatedly ask *"what was observed inside
this sphere, around this time?"* and want the answers back in chronological
order.

## How it works

Every observation is `(x, y, z, timestamp)` plus an arbitrary protobuf payload.
The store maps that 4-D point to a single 20-byte key and writes it to
[SurrealKV](https://crates.io/crates/surrealkv), an embedded LSM key-value
engine. Spatial locality in the key means a bounding-sphere query becomes a
small set of contiguous key-range scans.

### Key layout (20 bytes, big-endian)

```
[ 4B macro epoch ][ 8B 3-D Morton code ][ 8B nanosecond offset in epoch ]
```

- **Macro epoch** — `timestamp_secs / epoch_duration`. Groups observations into
  coarse time buckets so a query can restrict itself to the epochs it cares
  about.
- **3-D Morton code** — each coordinate is clamped to `[-max_range, max_range]`,
  normalized to a 21-bit integer, and the three axes are bit-interleaved into a
  63-bit Z-order curve. Nearby points in space stay nearby in key space.
- **Nanosecond offset** — sub-epoch time, so records sort chronologically within
  a cell.

### Querying

Spatial queries pick an *optimal Morton depth* for the search radius (keeping
KV fan-out between 1 and 8 cells), expand to the cell prefixes that intersect the
bounding box, and scan those ranges. Because Morton ranges cover the bounding
*box* and only boundary cells are time-clamped, every candidate is re-checked
with an exact sphere test and an exact time filter before being returned.
Results are sorted by timestamp, ready for replay or filter integration.

## gRPC API

Defined in [`proto/interstellar.proto`](proto/interstellar.proto):

| RPC           | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `Store`       | Store a payload at a 4-D point; returns the generated 20-byte key.           |
| `QueryWindow` | Stream all payloads within a sphere across every epoch overlapping a window. |
| `Subscribe`   | Stream every subsequent `Store` that lands inside a sphere, until cancelled. |

Payloads are `google.protobuf.Any`, so the store stays codec-agnostic — it never
interprets the bytes you hand it.

## Project layout

| Path                             | What it is                                                          |
|----------------------------------|--------------------------------------------------------------------|
| `crates/interstellar-index/`     | `HybridSpatioTemporalIndexer` — pure key encoding, Morton math, ranges. No I/O; compiles to wasm. |
| `crates/interstellar-wasm/`      | `wasm-bindgen` browser bindings over the indexer.                   |
| `src/lib.rs`                     | Native crate root: re-exports the indexer, wires up the store + proto. |
| `src/store.rs`                   | `SpatioTemporalStore` — SurrealKV-backed persistence and queries.   |
| `src/bin/server.rs`              | gRPC server exposing the `InterstellarDb` service.                  |
| `src/bin/pipeline_demo.rs`       | Kalman-style pipeline: single-epoch vs. time-window queries.        |
| `src/bin/parabola_demo.rs`       | End-to-end demo: stores a trajectory and subscribes to pushes.      |
| `src/bin/bench.rs`               | Insert / read / subscription-fanout benchmarks at 10k–10M records.  |
| `web/`                           | Browser demo: WASM indexer in a Web Worker over IndexedDB.          |
| `proto/interstellar.proto`       | gRPC service and message definitions.                               |

## Building and running

The toolchain is provided through a Nix flake — enter the dev shell first:

```sh
nix develop
```

The build script uses a vendored `protoc`, so no system Protocol Buffers
install is required.

```sh
# Run the gRPC server (DB_PATH and PORT are configurable via env vars)
cargo run --bin server                  # listens on [::1]:50051, ./interstellar.db

# End-to-end demo (builds and starts the server automatically)
cargo run --bin parabola_demo

# Query-mode pipeline demo
cargo run --bin pipeline_demo

# Benchmarks
cargo run --release --bin bench
```

### Server configuration

| Variable  | Default             | Meaning                          |
|-----------|---------------------|----------------------------------|
| `DB_PATH` | `./interstellar.db` | On-disk location of the store.   |
| `PORT`    | `50051`             | gRPC listen port on `[::1]`.     |

The server opens the store with `max_range = 1000.0` and `epoch_duration = 3600`
(one-hour epochs).

## Running in the browser (WASM + IndexedDB)

The indexer is pure computation, so it compiles to `wasm32-unknown-unknown` and
runs client-side with **no server**. The architecture mirrors the native one:

| Native                          | Browser                                            |
|---------------------------------|----------------------------------------------------|
| `HybridSpatioTemporalIndexer`   | same crate, compiled to WASM                        |
| SurrealKV (LSM byte-keyed KV)   | IndexedDB (binary keys ordered by byte comparison)  |
| `tokio` worker threads          | a Web Worker (keeps the main thread responsive)     |

The WASM module only *computes* keys and Morton scan ranges and decodes
candidates; the Web Worker does the IndexedDB I/O via `IDBKeyRange.bound` and
applies the same exact sphere + time filters as `SpatioTemporalStore::collect`.

```sh
nix develop            # toolchain now includes the wasm32 target + wasm-bindgen
./web/build.sh         # cargo build -> wasm-bindgen, outputs web/pkg/
miniserve --index index.html web   # serve at http://localhost:8080
```

The visualization uses [deck.gl](https://deck.gl) with an `OrbitView` — drag to
rotate the 3-D point cloud, scroll to zoom. The query sphere is drawn as three
great-circle wireframes (one per axis plane).

| Button | What it does |
|---|---|
| **Seed** | Generate random observations and store them in IndexedDB. |
| **Query epoch / window** | Run a sphere query; matched points highlight in green. |
| **Orbit demo** | Animates the query center on a Lissajous path, running queries continuously to show live throughput (ms/query in the stat bar). Click again to stop. |
| **Self-test** | Deterministic store→query round-trip; logs PASS/FAIL. |

> A static file server is required (module workers and WASM won't load over
> `file://`). Any server works; `miniserve` ships in the dev shell.
