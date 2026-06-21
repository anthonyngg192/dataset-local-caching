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
    --read-ratio 0.9
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

### Example output

```
target 127.0.0.1:8383 | 64 connections | 7812 ops/conn | 499968 total | value 64B | read-ratio 0.90 | keyspace 100000

=== results ===
elapsed     : 2.487s
operations  : 499968
throughput  : 201019 ops/sec
latency mean: 0.317 ms
latency p50 : 0.289 ms
latency p90 : 0.484 ms
latency p99 : 0.792 ms
latency p999: 1.453 ms
latency max : 10.180 ms
```

> **Tip:** always benchmark the `--release` build of *both* binaries. Debug
> builds are several times slower and will give misleading numbers. Throughput
> here is closed-loop, so it scales with `--connections` up to the point where
> the dispatch layer saturates.

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
