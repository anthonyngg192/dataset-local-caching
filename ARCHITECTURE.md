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
  │  reader  ── batch (req_id, op) ───────────► │ Worker 0  cache+index+disk │ s0
  │   │  (hash(key) & (N-1))           │        ├───────────────────────────┤
  │   │                                │        │ Worker 1  cache+index+disk │ s1
  │   │                                │        ├───────────────────────────┤
  │   │   reply (req_id, payload)      │        │ ...                       │
  │   ▼   ◄───────────────────────────────────  │ Worker N-1 cache+index+disk│ sN
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
| [`src/utils.rs`](src/utils.rs) | `ClientCommand` (GET/SET/SETEX/DEL), `WorkerOp`, `WorkerCommand::Batch`. |
| [`src/shards/state.rs`](src/shards/state.rs) | Per-worker event loop: applies each op, replies tagged with `req_id`, runs the periodic TTL sweep + fsync. |
| [`src/shards/worker.rs`](src/shards/worker.rs) | Per-shard store: LRU value cache + key→location index + TTL heap + byte budget. |
| [`src/shards/disk.rs`](src/shards/disk.rs) | Per-shard persistence: `cool.log` (values) + `backup.log` (index journal); write-through + replay. |
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

## Storage engine (per shard)

Each worker owns a small tiered store — RAM is a cache over a durable disk log,
so the dataset is bounded by **disk**, not RAM.

- **Index** — `HashMap<key → {val_off, vlen, exp_ms}>` for *every* key. Always in
  RAM. This bounds the key *count* (~10M short keys per GB of index), not the
  data size.
- **Cache** — an LRU of hot values (`lru` crate), bounded by a byte budget
  (`MAX_MEMORY_MB` split per shard). A GET miss reads the value from disk and
  re-caches it.
- **TTL** — `exp_ms` stored per entry as **absolute epoch ms** (survives
  restart). Lazy on read (an expired key reads as a miss and is dropped) plus a
  bounded active sweep on a timer, driven by a min-heap.

### Persistence (write-through) + tiering

Two append-only files per shard:

- **`cool.log`** — the value store and source of truth: every write appends
  `[op][klen][exp_ms][vlen][key][value]`.
- **`backup.log`** — the index journal: `[op][klen][val_off][vlen][exp_ms][key]`.

Write path: append to both → update index → put in cache → ack. `fsync` is
batched on the timer tick; flush-to-OS happens after each batch. On restart, the
small `backup.log` is replayed to rebuild the index (values stay on disk, loaded
lazily).

**Eviction is lossless.** When the cache passes its 90% high-water mark it drops
LRU values down to 80% — but only the *cache copy*; the value is already in
`cool.log` and the index still points at it, so a later GET reads it back. That
is what lets a 2 GB cache front a much larger on-disk dataset without losing
cold data.

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
  hash and pinning each partition to one worker removes lock contention. In
  principle this scales the *store* across cores — but measured, it does *not*
  help for in-memory work: a single worker is only ~16% busy at 1.6M ops/s, so
  the per-op coordination dominates, not the store (see trade-offs). The payoff
  needs genuinely heavy per-op work (real disk I/O at a scale beyond RAM).
- **RAM as a cache, not the whole store (the real value).** With tiering, the
  dataset is bounded by disk, not memory: a fixed RAM budget fronts a much larger
  durable dataset on disk. This is the point — capacity per GB of RAM — for small,
  memory-constrained deployments. Throughput vs a single-threaded in-memory store
  is *not* the selling axis.
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

- **No compaction yet.** `cool.log` and `backup.log` are append-only and never
  reclaimed, so overwrites, deletes (tombstones), and expired records pile up as
  garbage — the files grow forever and replay slows over time. Compaction
  (rewrite a clean log, swap it in) is the main missing piece.
- **Sharding doesn't measurably pay off here.** Across in-RAM, LRU, and
  page-cached disk tiering, 1 worker ≈ 8 — the per-op coordination dominates. A
  clean win needs data ≫ RAM hitting real disk (parallel I/O across workers),
  which we couldn't reproduce on a laptop (the dataset fit the OS page cache).
- **No server-side backpressure.** Reply and worker channels are unbounded; the
  server trusts the client's slot pool to bound in-flight requests. A per-
  connection semaphore (and TCP backpressure) is the fix.
- **Blocking `pread` in async workers.** A cold read blocks the worker's runtime
  thread (a simplification). Real async file I/O (or `spawn_blocking`) would be
  cleaner under heavy cold-read load.
- **Index bounds key count.** Every key's index entry lives in RAM (~80 B for
  short keys → ~10M keys/GB). Data size is bounded by disk; key *count* by RAM.
- **Auth is weak.** Credentials cross the wire in plaintext and the comparison is
  not constant-time — run behind TLS / a trusted network.
- **Missing commands & metrics.** No `EXISTS` / `INCR` / `MGET`; heartbeat is a
  bare echo (`1`).

---

> _This documentation was written by AI, but all benchmark data comes from our
> own system: Apple M2, 16 GB RAM, macOS._
