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

Sweep at 64 connections, 2,000,000 ops, 64B values, 90% reads:

| `--pipeline` | throughput | vs closed-loop | p50 latency* | p99 latency* |
|---:|---:|---:|---:|---:|
| 1 (closed-loop) | 139,753 ops/s | 1.0× | 0.40 ms | 1.45 ms |
| 8 | 484,218 ops/s | 3.5× | 0.95 ms | 3.05 ms |
| 32 | 597,667 ops/s | 4.3× | 3.32 ms | 7.48 ms |
| 128 | 639,275 ops/s | 4.6× | 10.41 ms | 44.93 ms |

\*From `--pipeline 8` upward each latency sample is a whole **batch** round-trip,
not a single op, so the numbers are expected to grow with depth. Throughput
plateaus around ~640k ops/s — past that, deeper pipelining only inflates latency,
which means the bottleneck has shifted from the network to the server's
message-passing layer.

```
=== results === (--pipeline 8)
throughput  : 484218 ops/sec
latency (per-batch of 8):
latency p50 : 0.952 ms
latency p99 : 3.050 ms
```

> **Tip:** always benchmark the `--release` build of *both* binaries. Debug
> builds are several times slower and will give misleading numbers. Start with
> `--pipeline 1` to see per-op latency, then raise it (8 → 32 → 128) to find the
> throughput ceiling.

## Project layout

```
src/
├── main.rs          # bootstrap: worker pool + listener
├── server.rs        # TCP listener, connection handling, dispatch
├── common.rs        # frame codec (wire protocol)
├── hashing.rs       # key -> shard routing
├── utils.rs         # ClientCommand / WorkerCommand + request parsing
├── shards/
│   ├── state.rs     # per-worker event loop
│   └── worker.rs    # the HashMap store
└── bin/
    └── bench.rs     # load generator
```

---

> _This documentation was written by AI, but all benchmark data comes from our
> own system: Apple M2, 16 GB RAM, macOS._
