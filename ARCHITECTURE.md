# Architecture

`dataset-local` is an in-memory key/value store designed around one idea:
combine the **single-threaded speed of Redis** with the **shared-nothing
multithreading of Dragonfly**. Each CPU core owns a private slice of the
keyspace, so reads and writes never contend on a shared lock — keys are routed
to the owning core, and that core processes its commands serially.

```
                          ┌───────────────────────────────────────────┐
                          │                 Process                     │
                          │                                             │
   TCP client ─────►  ┌───┴────────┐   hash(key) & (N-1)                │
   (handshake,        │  Listener  │  ───────────────┐                  │
    request frames)   │  + per-conn│                 │                  │
                  ◄── │  tasks     │                 ▼                  │
                      └───┬────────┘        ┌─────────────────┐         │
                          │  WorkerCommand   │  Worker 0       │ shard 0 │
                          │  over mpsc       │  HashMap (owned)│         │
                          ├─────────────────►├─────────────────┤         │
                          │                  │  Worker 1       │ shard 1 │
                          │                  │  HashMap (owned)│         │
                          │                  ├─────────────────┤         │
                          │   oneshot reply  │  ...            │         │
                          │  ◄───────────────│  Worker N-1     │ shard N │
                          │                  └─────────────────┘         │
                          └─────────────────────────────────────────────┘
```

## Components

| File | Responsibility |
|------|----------------|
| [`src/main.rs`](src/main.rs) | Bootstrap: spawn the worker pool, build the listener, run. |
| [`src/server.rs`](src/server.rs) | TCP listener, per-connection handling, request routing/dispatch. |
| [`src/common.rs`](src/common.rs) | Wire framing: `Frame`, `FrameKind`, and the `SimpleCodec` encoder/decoder. |
| [`src/hashing.rs`](src/hashing.rs) | `route_hash` — maps a key to a shard id. |
| [`src/utils.rs`](src/utils.rs) | `ClientCommand` (parsed request) + `WorkerCommand` (internal message). |
| [`src/shards/state.rs`](src/shards/state.rs) | The per-worker event loop reading from its mpsc channel. |
| [`src/shards/worker.rs`](src/shards/worker.rs) | The actual storage: a `HashMap<Vec<u8>, Bytes>`. |
| [`src/bin/bench.rs`](src/bin/bench.rs) | Standalone load generator (see [README](README.md)). |

## Threading model

- **One Tokio multi-threaded runtime** drives everything.
- **N workers**, where `N = next_power_of_two(num_cpus)`. Each worker is a Tokio
  task running an infinite `recv().await` loop over an **unbounded mpsc**
  channel. Because each worker owns its `HashMap` exclusively, there is **no
  lock** on the data path.
- **One task per connection.** The listener accepts a socket and spawns a task
  that performs the handshake, then loops decoding frames and dispatching them.
- Connection tasks talk to workers through cloned `UnboundedSender`s
  (`WorkerTx`), shared via an `Arc<Vec<WorkerTx>>`.

### Why power-of-two workers?

Routing uses a bitmask instead of a modulo:

```rust
shard_id = hash & (worker_count - 1)
```

This is only correct when `worker_count` is a power of two, which is why
`main.rs` rounds the CPU count up with `next_power_of_two()`. The mask is a
single AND instruction — cheaper than `%` on the hot path.

## Request lifecycle

1. **Accept** — `ServerListener::start` accepts a TCP connection and spawns
   `handle_conn`.
2. **Handshake** — the first frame must be `FrameKind::HandShake`. Anything else
   gets an error response and the connection is closed. The handshake itself is
   not acknowledged (the client should not block waiting for a reply to it).
3. **Decode** — `SimpleCodec` reads length-delimited frames off the socket.
4. **Parse** — for a `Request` frame, `ClientCommand::parse` turns the payload
   into a `Get`/`Set`/`Del`.
5. **Route** — `route_hash(key, N)` selects the owning worker.
6. **Dispatch** — the connection task sends a `WorkerCommand` carrying a
   `oneshot::Sender` and `await`s the reply. The worker processes the command
   against its `HashMap` and sends the result back over the oneshot.
7. **Respond** — the connection task writes a `Response` frame back to the
   client.

This keeps the design **shared-nothing**: the only cross-thread traffic is
message passing (mpsc in, oneshot out), never shared mutable state.

## Wire protocol

### Frame layout

Every message on the socket is a length-delimited frame:

```
 byte 0      bytes 1-2          bytes 3..3+len
┌────────┬──────────────────┬──────────────────┐
│ header │ payload len (u16 │   payload bytes   │
│  (u8)  │   big-endian)    │                   │
└────────┴──────────────────┴──────────────────┘
```

- Max payload size: 64 KiB (`MAX_FRAME_SIZE`).
- `header` values (`FrameKind`):

  | Value | Kind        | Direction        |
  |-------|-------------|------------------|
  | `1`   | `HandShake` | client → server  |
  | `3`   | `Request`   | client → server  |
  | `4`   | `Response`  | server → client  |
  | `5`   | `Heartbeat` | client → server  |

### Request payload

The payload of a `Request` frame (`header = 3`) encodes one command:

```
 byte 0    bytes 1-2         klen bytes     remaining bytes
┌───────┬──────────────────┬─────────────┬──────────────────┐
│  op   │ key len (u16 be) │     key     │  value (SET only) │
└───────┴──────────────────┴─────────────┴──────────────────┘
```

- `op`: `1 = GET`, `2 = SET`, `3 = DEL`.
- `GET` / `DEL` ignore everything after the key.
- `SET` treats all remaining bytes as the value.

### Response payload

Responses are `Response` frames (`header = 4`) whose payload is a plain byte
string:

| Command | Response payload |
|---------|------------------|
| `GET` hit | the stored value bytes |
| `GET` miss | `(nil)` |
| `SET` | `OK` |
| `DEL` hit | `1` |
| `DEL` miss | `0` |
| `Heartbeat` | `1` |
| parse / routing error | `ERR <message>` |

## Design rationale

- **Shared-nothing sharding (Dragonfly-style).** Partitioning the keyspace by
  hash and pinning each partition to one worker removes lock contention. Adding
  cores scales throughput roughly linearly until the network/dispatch layer
  becomes the bottleneck.
- **Serial per-shard execution (Redis-style).** Within a shard, commands run one
  at a time, so each worker's data structures need no synchronization and
  operations are trivially atomic.
- **Message passing over shared memory.** mpsc-in / oneshot-out keeps ownership
  clear and lets the borrow checker guarantee there is no data race.

## Known trade-offs / TODO

These are intentional simplifications worth revisiting:

- **Unbounded channels** give no backpressure; a slow/stuck worker could grow its
  queue without bound. A bounded channel would trade latency spikes for memory
  safety.
- **`Worker::set` return value is inverted** (`true` when the key already
  existed, `false` on a fresh insert). `dispatch` currently maps both to `OK`,
  so it is not observable yet, but the semantics should be unified.
- **No TTL / expiration**, no `EXISTS` / `INCR` / `MGET`, no persistence — these
  are the natural next features for a Redis-like cache.
- **Heartbeat is an echo** (`1`); real liveness/metrics could be layered on top.
- **No request pipelining** — each connection is closed-loop (one outstanding
  request at a time).
