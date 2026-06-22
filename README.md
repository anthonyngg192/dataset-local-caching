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

It binds `0.0.0.0:8383` and spawns `next_power_of_two(num_cpus)` workers. You
should see:

```
INFO dataset_local: spawned 8 workers
INFO dataset_local::server: listening on 0.0.0.0:8383 with 8 workers
```

Logging is controlled by `RUST_LOG`, e.g. `RUST_LOG=debug cargo run`.

## Quick manual test

A client must send a handshake frame first, then request frames. This Python
snippet exercises SET / GET / DEL / heartbeat against a running server:

```python
import socket, struct

def frame(header, payload=b""):
    return bytes([header]) + struct.pack(">H", len(payload)) + payload

def request(op, key, value=b""):                       # op: 1=GET 2=SET 3=DEL
    return frame(3, bytes([op]) + struct.pack(">H", len(key)) + key + value)

def read(s):
    head = s.recv(3)
    length = struct.unpack(">H", head[1:3])[0]
    return head[0], (s.recv(length) if length else b"")

s = socket.create_connection(("127.0.0.1", 8383))
s.sendall(frame(1))                                    # handshake (no reply)
s.sendall(request(2, b"hello", b"world")); print(read(s))   # -> (4, b'OK')
s.sendall(request(1, b"hello"));            print(read(s))   # -> (4, b'world')
s.sendall(request(3, b"hello"));            print(read(s))   # -> (4, b'1')
s.sendall(request(1, b"hello"));            print(read(s))   # -> (4, b'(nil)')
s.close()
```

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

### vs Redis 7.2.6 (same machine, single-threaded)

Same box, same session, `redis-benchmark -t get -c 64 -d 64 -r 100000 -P <depth>`
against our 90%-read mixed workload. Redis is single-threaded; we shard across
all cores:

| `--pipeline` | dataset-local | Redis | notes |
|---:|---:|---:|:--|
| 1 | ~170k ops/s | ~150k ops/s | we lead — no event-loop overhead at depth 1 |
| 8 | ~780k ops/s | ~1.19M ops/s | Redis ~1.5× |
| 32 | ~1.53M ops/s | ~2.24M ops/s | Redis ~1.5× |
| 128 | ~1.89M ops/s | ~2.81M ops/s | Redis ~1.5× |

Two axes pull in opposite directions: **scaling pipeline depth favors Redis**
(one thread swallows a whole pipeline with zero coordination), while **scaling
connection count favors us** (we spread across all cores; a single-threaded
server plateaus and even degrades past ~128 connections). The realistic
request-response workload (many connections, no deep pipelining) is the
connection axis — our home turf, where we beat Redis and the lead widens as
connections grow. Adding worker shards does *not* help yet: a single worker is
~8% busy, so the store is never the bottleneck until per-op work gets heavy
(LRU, TTL).

```
=== results === (--pipeline 32)
throughput  : 1531190 ops/sec
```

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
  const c = new DatasetClient({ port: 8383 });
  await c.connect();
  await c.set('hello', 'world');
  await c.get('hello'); // Buffer 'world' | null
  ```
- **Rust** — [`clients/rust`](clients/rust) (standalone crate)
  ```rust
  let c = dataset_client::Client::connect("127.0.0.1:8383", 256).await?;
  c.set(b"hello", b"world").await;
  c.get(b"hello").await; // Some(Bytes) | None
  ```

Run their smoke tests against a live server: `node clients/node/dataset-client.mjs`
and `cd clients/rust && cargo run --example smoke`.

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
└── rust/            # Rust client crate
PROTOCOL.md          # wire protocol spec (the shared contract)
ARCHITECTURE.md      # design + threading model
```

---

> _This documentation was written by AI, but all benchmark data comes from our
> own system: Apple M2, 16 GB RAM, macOS._
