# Architecture

`dataset-local` is an in-memory key/value store designed around one idea:
combine the **single-threaded speed of Redis** with the **shared-nothing
multithreading of Dragonfly**. Each CPU core owns a private slice of the
keyspace, so reads and writes never contend on a shared lock — keys are routed
to the owning core, and that core processes its commands serially.

Connections are **multiplexed**: every request carries a client-chosen `req_id`,
and the server echoes it on the response. The server processes requests in
parallel across shards and writes each response back **as soon as it is ready,
in any order** — the client matches responses to requests by `req_id`. There is
no response ordering on the server, so there is no head-of-line blocking and no
reorder buffer. Requests bound for the same shard in one reader pass are
**batched** into a single message, so one channel send and one task wake-up cover
many operations.

```
            per connection                          shared worker pool
  ┌───────────────────────────────────┐        ┌───────────────────────────┐
  │  reader  ── batch (req_id, op) ───────────► │ Worker 0  HashMap (owned) │ s0
  │   │  (hash(key) & (N-1))           │        ├───────────────────────────┤
  │   │                                │        │ Worker 1  HashMap (owned) │ s1
  │   │                                │        ├───────────────────────────┤
  │   │   reply (req_id, payload)      │        │ ...                       │
  │   ▼   ◄───────────────────────────────────  │ Worker N-1 HashMap (owned)│ sN
  │  writer ── responses, any order ──► client   └───────────────────────────┘
  └───────────────────────────────────┘          (client reorders by req_id)
```

## Components

| File | Responsibility |
|------|----------------|
| [`src/main.rs`](src/main.rs) | Bootstrap: spawn the worker pool, build the listener, run. |
| [`src/server.rs`](src/server.rs) | TCP listener, per-connection reader/writer split, shard batching, routing. |
| [`src/common.rs`](src/common.rs) | Wire framing: `Frame` (with `req_id`), `FrameKind`, and the `SimpleCodec` codec. |
| [`src/hashing.rs`](src/hashing.rs) | `route_hash` — maps a key to a shard id. |
| [`src/utils.rs`](src/utils.rs) | `ClientCommand` (parsed request), `WorkerOp`, `WorkerCommand::Batch` (internal message). |
| [`src/shards/state.rs`](src/shards/state.rs) | The per-worker event loop; applies each op and replies tagged with its `req_id`. |
| [`src/shards/worker.rs`](src/shards/worker.rs) | The actual storage: a `HashMap<Bytes, Bytes>`. |
| [`src/bin/bench.rs`](src/bin/bench.rs) | Standalone load generator (see [README](README.md)). |

## Threading model

- **One Tokio multi-threaded runtime** drives everything.
- **N workers**, where `N = next_power_of_two(num_cpus)` (override with the
  `WORKERS` env var). Each worker is a Tokio task running a `recv().await` loop
  over an **unbounded mpsc** channel of `WorkerCommand::Batch`. Because each
  worker owns its `HashMap` exclusively, there is **no lock** on the data path.
- **Two tasks per connection.** The listener accepts a socket and spawns a task
  that, after the handshake, splits the framed socket into a **reader half** (the
  connection task) and a **writer half** (its own spawned task). They are joined
  by a per-connection `mpsc::unbounded_channel::<(u32, Bytes)>` — `(req_id,
  response)` pairs.
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

## Connection lifecycle

After the handshake, `handle_conn` splits the framed socket and runs two halves
concurrently, joined by a per-connection reply channel.

- **Writer half** (a spawned task) drains the reply channel and writes each
  `(req_id, payload)` straight to the socket — in whatever order they arrive. No
  sorting, no buffering, no awaiting individual replies.
- **Reader half** (the connection task) decodes a pass of frames
  (`ready_chunks(MAX_BATCH)` hands it every frame already buffered), then:
  1. **Immediate frames** (heartbeat, errors, handshake-after-handshake) → push
     `(req_id, payload)` straight onto the reply channel.
  2. **Request frames** → parse, route by `hash(key) & (N-1)`, and bucket
     `(req_id, WorkerOp)` by shard.
  3. **Dispatch** one `WorkerCommand::Batch` per non-empty shard, each carrying a
     clone of the connection's reply channel sender.

A worker applies each op in a batch and pushes `(req_id, response)` back onto the
connection's reply channel as it finishes — in any order. The writer emits them
as they come; the client reorders by `req_id`.

### Why this is simpler than ordered pipelining

An earlier design kept responses in arrival order on the server, which required a
FIFO completion queue, a per-op reply channel, and a reorder/await step — plus it
suffered head-of-line blocking (a slow shard stalled responses queued behind it).
Moving the `req_id` into the protocol pushes reordering to the client and lets
the server delete all of that: no `Pending`, no FIFO queue, no per-op `oneshot`,
no reorder. The server just routes and streams.

### Sharding and response order are independent

Ops for different shards complete at different times. With multiplexing that is a
non-issue: each response carries its `req_id`, so out-of-order completion is the
expected, correct behavior — the client's outstanding map sorts it out.

## Wire protocol

The full, byte-level spec is in [PROTOCOL.md](PROTOCOL.md). Summary:

```
 byte 0      bytes 1-4           bytes 5-6          bytes 7 .. 7+len
┌────────┬──────────────────┬──────────────────┬────────────────────┐
│ header │  req_id (u32 BE) │   len (u16 BE)   │   payload (len B)   │
└────────┴──────────────────┴──────────────────┴────────────────────┘
```

- Max payload size: 64 KiB (`MAX_FRAME_SIZE`).
- `req_id` is opaque to the server: copied from a Request onto its Response.
- `header` (`FrameKind`): `1` HandShake, `3` Request, `4` Response, `5` Heartbeat.
- Request payload: `[op:u8][klen:u16 BE][key][value (SET only)]`, op `1=GET 2=SET
  3=DEL`.
- Response payload: GET hit = value, GET miss = `(nil)`, SET = `OK`, DEL = `1`/`0`,
  error = `ERR <message>`.

## Design rationale

- **Shared-nothing sharding (Dragonfly-style).** Partitioning the keyspace by
  hash and pinning each partition to one worker removes lock contention. Adding
  cores scales the *store* throughput — though for light GET/SET the store is
  rarely the bottleneck (see trade-offs).
- **Serial per-shard execution (Redis-style).** Within a shard, commands run one
  at a time, so each worker's data structures need no synchronization.
- **Message passing over shared memory.** mpsc in, mpsc out — ownership is clear
  and the borrow checker guarantees no data race.
- **Multiplexing via `req_id`.** Out-of-order responses remove head-of-line
  blocking and let the server drop all ordering machinery. The cost moves to the
  client (it must be async and match by id).
- **Shard batching.** Grouping a reader pass's ops per shard turns N channel
  sends + N worker wake-ups into one per shard. The cost being amortized is the
  task wake-up, not the (cheap) hashmap op.

## Known trade-offs / TODO

- **No server-side backpressure (regression from the ordered design).** The reply
  channel and worker channels are unbounded; the server relies on the client's
  slot pool to bound in-flight requests. A misbehaving client could grow server
  memory. Production should add a per-connection semaphore (bounded in-flight)
  that also drives TCP backpressure.
- **Store is rarely the bottleneck for plain GET/SET.** A single worker sits
  ~8% busy at ~1.5M ops/s; adding workers does not help until per-op store work
  gets heavy (LRU, TTL). Sharding is provisioned for that future, not today's
  empty store.
- **`Worker::set` return value is inverted** (`true` when the key already
  existed). Mapped to `OK` either way, so not observable yet, but should be fixed.
- **No TTL / expiration**, no `EXISTS` / `INCR` / `MGET`, no LRU eviction, no
  persistence — the natural next features for a Redis-like cache.
- **Heartbeat is an echo** (`1`); real liveness/metrics could be layered on top.

---

> _This documentation was written by AI, but all benchmark data comes from our
> own system: Apple M2, 16 GB RAM, macOS._
