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
| `Query`       | Stream all payloads within a sphere in the epoch containing a timestamp.     |
| `QueryWindow` | Stream all payloads within a sphere across every epoch overlapping a window. |
| `Subscribe`   | Stream every subsequent `Store` that lands inside a sphere, until cancelled. |

Payloads are `google.protobuf.Any`, so the store stays codec-agnostic — it never
interprets the bytes you hand it.

## Project layout

| Path                        | What it is                                                          |
|-----------------------------|--------------------------------------------------------------------|
| `src/lib.rs`                | `HybridSpatioTemporalIndexer` — key encoding, Morton math, ranges.  |
| `src/store.rs`              | `SpatioTemporalStore` — SurrealKV-backed persistence and queries.   |
| `src/bin/server.rs`         | gRPC server exposing the `InterstellarDb` service.                  |
| `src/bin/pipeline_demo.rs`  | Kalman-style pipeline: single-epoch vs. time-window queries.        |
| `src/bin/parabola_demo.rs`  | End-to-end demo: stores a trajectory and subscribes to pushes.      |
| `src/bin/bench.rs`          | Insert / read / subscription-fanout benchmarks at 10k–10M records.  |
| `proto/interstellar.proto`  | gRPC service and message definitions.                               |

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
