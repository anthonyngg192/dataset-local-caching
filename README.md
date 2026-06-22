# dataset-local

An in-memory, sharded key/value store written in Rust + Tokio. It blends the
**single-threaded execution model of Redis** with the **shared-nothing,
per-core sharding of Dragonfly**: every key is routed to the CPU core that owns
it, and that core processes its commands without any locks.

For the design details and wire protocol, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Requirements

- Rust (edition 2024) — install via [rustup](https://rustup.rs).
- A POSIX shell + `python3` (only for the quick manual test below).

## Build

```bash
cargo build            # debug
cargo build --release  # optimized (use this for benchmarking)
```

This produces two binaries:

- `dataset-local` — the server.
- `bench` — the load generator.

## Run the server

```bash
cargo run --release --bin dataset-local
```

It binds `0.0.0.0:8383` and spawns `next_power_of_two(num_cpus)` workers.

### Configuration

Config comes from environment variables, auto-loaded from a `.env` file in the
working directory if present (real env vars override the file). Copy the template:

```bash
cp .env.example .env   # then edit
cargo run --release    # picks up .env automatically
```

| Var | Default | Meaning |
|-----|---------|---------|
| `WORKERS` | CPU count | Shard/worker count (rounded up to a power of two). |
| `DATASET_USERNAME` | — | Enable auth: required username (set with `DATASET_PASSWORD`). |
| `DATASET_PASSWORD` | — | Required password. Auth is **on** only when both are set. |
| `RUST_LOG` | `info` | Log level, e.g. `RUST_LOG=debug`. |

`.env` is git-ignored (keep secrets out of the repo); `.env.example` is the
tracked template. Overriding still works inline:

```bash
WORKERS=4 DATASET_USERNAME=admin DATASET_PASSWORD=secret cargo run --release
```

When auth is on, the client must send the username/password in its handshake (see
[PROTOCOL.md](PROTOCOL.md#handshake)); the client libraries take them as options.

### Docker

```bash
docker build -t dataset-local .
docker run --rm -p 8383:8383 \
  -e DATASET_USERNAME=admin -e DATASET_PASSWORD=secret \
  -e WORKERS=4 \
  dataset-local
```

Multi-stage build → a small `debian:bookworm-slim` image running as a non-root
user. Auth/worker config is passed via `-e` env vars.

## Quick test

Use a client library (see [Clients](#clients)) — they speak the multiplexed v2
protocol and run a built-in smoke test against a live server:

```bash
node clients/node/dataset-client.mjs           # Node
cd clients/rust && cargo run --example smoke    # Rust
```

For the raw wire format (frames, handshake, op codes) see [PROTOCOL.md](PROTOCOL.md).

## Benchmark

The `bench` binary opens many concurrent connections, each running a
**closed-loop** SET/GET workload (one outstanding request at a time), then
reports throughput and latency percentiles.

```bash
# 1. start the server (release build!)
cargo run --release --bin dataset-local

# 2. in another terminal, run the load generator
cargo run --release --bin bench -- \
    --addr 127.0.0.1:8383 \
    --connections 64 \
    --requests 500000 \
    --value-size 64 \
    --keyspace 100000 \
    --read-ratio 0.9 \
    --pipeline 1
```

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--addr` | `127.0.0.1:8383` | Server address. |
| `--connections`, `-c` | `64` | Number of concurrent connections. |
| `--requests`, `-n` | `100000` | Total operations (split evenly across connections). |
| `--value-size` | `64` | Value size in bytes for SET. |
| `--keyspace` | `100000` | Number of distinct keys (controls hit rate + shard spread). |
| `--read-ratio` | `0.9` | Fraction of operations that are GET (rest are SET). |
| `--pipeline`, `-p` | `1` | Requests sent before reading their responses. `1` = closed-loop; higher amortizes the round-trip. |

### Closed-loop vs pipelining

With `--pipeline 1` each connection sends one request and waits for its
response before sending the next. Throughput is then bounded by the network
round-trip (`throughput ≈ connections / latency`), **not** by the server. Raising
the pipeline depth lets multiple requests be in flight per connection and
exposes the server's real ceiling.

Sweep at 64 connections, 2,000,000 ops, 64B values, 90% reads. Within one reader
pass, requests bound for the same shard are batched into a single worker
message, so one channel send + one task wake-up covers many ops:

Numbers below are the **v2 (multiplexed) protocol**, default 8 workers, one
session:

| `--pipeline` | throughput | vs closed-loop |
|---:|---:|---:|
| 1 (closed-loop) | 170,245 ops/s | 1.0× |
| 8 | 777,183 ops/s | 4.6× |
| 32 | 1,531,190 ops/s | 9.0× |
| 128 | 1,893,212 ops/s | 11.1× |

Throughput tops out around 1.5–1.9M ops/s (run dependent); past that, deeper
pipelining mostly inflates latency, which means the bottleneck has shifted from
the network to the per-op machinery (2 syscalls + a few task wake-ups per op,
amortized by batching). v2 vs the older in-order v1 is within noise on uniform
keys — v2's win is structural (no head-of-line blocking, simpler server), which
only shows under skewed/hot-shard load.

> Numbers vary ±10–15% run to run on a laptop (thermal + scheduling), so treat
> them as ballpark, not exact. The load generator reads responses through a
> `BufReader`; without it, the client itself caps throughput at deep pipelines
> (≈2× lower) — benchmark the harness, not just the server.

### Two axes: pipeline depth vs connection count

Throughput scales along two independent axes:

- **Pipeline depth** (requests in flight per connection) — amortizes syscalls;
  this is what takes a single connection from ~170k to ~1.9M ops/s.
- **Connection count** — spreads load across all cores. A request-response
  workload (one outstanding request per connection) leans on this axis, and it
  keeps climbing as connections grow rather than plateauing on one thread.

Adding *worker shards* does **not** help today: a single worker sits ~8% busy, so
the store is never the bottleneck — that only changes once per-op work gets heavy
(LRU, TTL), which is exactly what the sharded design is built for.

> **Tip:** always benchmark the `--release` build of *both* binaries. Debug
> builds are several times slower and will give misleading numbers. Start with
> `--pipeline 1` to see per-op latency, then raise it (8 → 32 → 128) to find the
> throughput ceiling.

## Clients

The protocol is multiplexed (see [PROTOCOL.md](PROTOCOL.md)): each request carries
a `req_id`, responses come back tagged and possibly out of order, and the client
matches them. That logic lives in the client libraries so apps get a simple async
API:

- **Node** — [`clients/node/dataset-client.mjs`](clients/node/dataset-client.mjs)
  ```js
  import { DatasetClient } from './clients/node/dataset-client.mjs';
  const c = new DatasetClient({ port: 8383, username: 'admin', password: 'secret' });
  await c.connect();              // rejects if auth fails
  await c.set('hello', 'world');
  await c.get('hello');           // Buffer 'world' | null
  ```
- **Rust** — [`clients/rust`](clients/rust) (standalone crate)
  ```rust
  // connect(addr, max_inflight, username, password) — pass "" "" when auth is off
  let c = dataset_client::Client::connect("127.0.0.1:8383", 256, "admin", "secret").await?;
  c.set(b"hello", b"world").await;
  c.get(b"hello").await;          // Some(Bytes) | None
  ```

Run their smoke tests against a live server: `node clients/node/dataset-client.mjs`
and `cd clients/rust && cargo run --example smoke`.

### CLI (REPL)

A redis-cli-style interactive shell, built on the Rust client:

```bash
cd clients/rust
cargo run --bin dataset-cli -- --addr 127.0.0.1:8383 --user admin --pass secret
# dataset> set hello world
# OK
# dataset> get hello
# world
# dataset> del hello
# (integer) 1
```

`--user`/`--pass` are optional (default to `DATASET_USERNAME`/`DATASET_PASSWORD`
env, or empty when the server has auth off). Commands: `get`, `set`, `del`,
`help`, `quit`.

## Project layout

```
src/
├── main.rs          # bootstrap: worker pool + listener
├── server.rs        # TCP listener, reader/writer split, shard routing
├── common.rs        # frame codec (wire protocol, with req_id)
├── hashing.rs       # key -> shard routing
├── utils.rs         # ClientCommand / WorkerOp / WorkerCommand + parsing
├── shards/
│   ├── state.rs     # per-worker event loop
│   └── worker.rs    # the HashMap store
└── bin/
    └── bench.rs     # load generator
clients/
├── node/            # Node.js client library
└── rust/            # Rust client crate + `dataset-cli` REPL binary
Dockerfile           # multi-stage build → slim runtime image
PROTOCOL.md          # wire protocol spec (the shared contract)
ARCHITECTURE.md      # design + threading model
```

---

> _This documentation was written by AI, but all benchmark data comes from our
> own system: Apple M2, 16 GB RAM, macOS._
