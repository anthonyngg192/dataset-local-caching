# Wire protocol

The contract shared by the server and every client library. Get one byte wrong
and the three implementations (server, Node client, Rust client) drift apart, so
this document is the single source of truth.

- Transport: TCP, binary, length-delimited frames.
- **v2 (current code):** *multiplexed*. Each request carries a client-chosen
  `req_id`; the server echoes it on the response. Responses may come back in any
  order, and the client matches them by `req_id`. No head-of-line blocking, and
  the server keeps no response-ordering machinery.
- **v1 (historical):** an earlier design emitted responses in arrival order per
  connection (no `req_id`). It suffered head-of-line blocking and needed a FIFO
  reorder step on the server. v2 replaced it.

The only wire difference between v1 and v2 is the 4-byte `req_id` field in the
frame envelope.

## Frame envelope (v2)

Every frame on the socket:

```
 byte 0      bytes 1-4           bytes 5-6          bytes 7 .. 7+len
┌────────┬──────────────────┬──────────────────┬────────────────────┐
│ header │  req_id (u32 BE) │   len (u16 BE)   │   payload (len B)   │
│  (u8)  │                  │                  │                    │
└────────┴──────────────────┴──────────────────┴────────────────────┘
```

- Fixed 7-byte header, then `len` bytes of payload.
- `len` ≤ 65535 and ≤ `MAX_FRAME_SIZE` (64 KiB).
- `req_id`: client-chosen correlation id (see [Multiplexing](#multiplexing)).
  Opaque to the server — it is copied verbatim from a Request onto its Response
  and never interpreted. Use `0` where it is unused (handshake).

For reference, the v1 envelope is `[header:u8][len:u16 BE][payload]` — no
`req_id`.

## Frame kinds (`header`)

| Value | Kind        | Direction          | `req_id` |
|------:|-------------|--------------------|----------|
| `1`   | `HandShake` | client → server    | unused (`0`) |
| `3`   | `Request`   | client → server    | client-chosen |
| `4`   | `Response`  | server → client    | echoes the Request's |
| `5`   | `Heartbeat` | client → server (server replies) | optional — echo it to measure RTT |

## Handshake

The first frame on a connection must be `HandShake` (`header=1`, `req_id=0`,
`len=0`). The server does **not** reply to it. After that, requests may flow.
Sending anything else first gets an error Response and the connection is closed.

## Request payload (`header=3`)

```
 byte 0    bytes 1-2          klen bytes      remaining bytes
┌───────┬──────────────────┬─────────────┬────────────────────┐
│  op   │ key len (u16 BE) │     key     │  value (SET only)  │
└───────┴──────────────────┴─────────────┴────────────────────┘
```

- `op`: `1 = GET`, `2 = SET`, `3 = DEL`.
- `GET` / `DEL` ignore everything after the key.
- `SET` treats all remaining bytes as the value.

## Response payload (`header=4`)

- The envelope's `req_id` equals the `req_id` of the Request it answers.
- The payload is a raw byte string:

  | Command   | Response payload        |
  |-----------|-------------------------|
  | `GET` hit | the stored value bytes  |
  | `GET` miss| `(nil)`                 |
  | `SET`     | `OK`                    |
  | `DEL` hit | `1`                     |
  | `DEL` miss| `0`                     |
  | `Heartbeat` | `1`                   |
  | error     | `ERR <message>`         |

## Multiplexing

This is the heart of v2 and lives **entirely on the client**.

- **The client assigns `req_id`.** The server is stateless about ids: it copies
  the id from a Request onto its Response, nothing more.
- **Uniqueness scope:** a `req_id` only has to be unique among the requests
  *currently outstanding on that connection*. Once the response arrives, the id
  is free to reuse. Different connections share no id namespace — connection A
  and connection B may both use `req_id = 1` at the same time.
- **Ordering:** the server may write responses in any order (a fast shard's
  result can overtake a slow one). The client resolves each response against its
  outstanding map keyed by `req_id`.

### Recommended id allocation: a slot pool

Use a fixed pool of `N` slots per connection where `req_id = slot index`:

- Sending a request = take a free slot; its index is the `req_id`.
- Receiving the response = return the slot.

This one mechanism does three jobs at once:

1. **No collisions** — an in-use slot is never handed out again.
2. **O(1) lookup** — the outstanding map is a plain array indexed by `req_id`, no
   hash map needed.
3. **Backpressure** — `N` is the max in-flight per connection. When the pool is
   empty the client blocks new requests until a slot frees, which stops it
   reading from the app and ultimately throttles the caller.

### Pool size (`N`)

`N` is the max outstanding requests per connection. **Default: `256`,
configurable.** Tune by benchmark: on commodity hardware throughput plateaus well
before this depth (in our tests, around 32–128), so `256` is comfortable
headroom. Larger `N` adds memory and tail latency for little throughput gain.

## Client library responsibilities

The same checklist for every language (Node, Rust, …):

1. Connection management — open, handshake, reconnect.
2. Slot pool — allocate/free `req_id`s; block when exhausted (backpressure).
3. Outstanding map — `req_id → pending promise/future` (an array indexed by the
   slot works best).
4. Background read loop — continuously read Response frames, extract `req_id`,
   resolve the matching promise/future.
5. Write path — encode a Request with a fresh `req_id` and send.
6. Public API — `get(key)`, `set(key, value)`, `del(key)`, each returning a
   promise/future.

Concurrency note: in **Node** the event loop serializes access to the
outstanding map, so no lock is needed. In **Rust** multiple tasks touch it, so
guard it — a fixed array of `oneshot` slots (id = index) keeps the lock minimal
and lookups O(1).

## Server design (v2, implemented)

The server is *simpler* than the v1 ordered design:

- Echoes `req_id` from each Request onto its Response.
- **No response ordering.** No in-order completion queue, no FIFO reorder, no
  per-op `oneshot`. Each worker writes its result back as soon as it is done,
  tagged with `req_id`, in any order.
- Each worker batch carries the originating connection's reply channel; results
  flow back as `(req_id, payload)` and the connection's writer emits them
  immediately, unordered.

Because ordering correctness moved to the client, the server fully parallelizes
across shards with no gather/reorder step.

> **Backpressure debt:** the current server uses *unbounded* reply and worker
> channels, so it relies entirely on the client's slot pool to bound in-flight
> requests. A misbehaving client could grow server memory. Production should add
> a per-connection in-flight semaphore that also drives TCP backpressure.

---

> _This documentation was written by AI. v2 reflects the current code._
