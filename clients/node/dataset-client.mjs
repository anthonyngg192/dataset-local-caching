// dataset-local client for Node.js — implements the v2 multiplexed protocol.
// See ../../PROTOCOL.md for the wire contract.
//
//   import { DatasetClient } from './dataset-client.mjs';
//   const c = new DatasetClient({ port: 8383 });
//   await c.connect();
//   await c.set('hello', 'world');
//   const v = await c.get('hello');   // Buffer 'world', or null on miss
//   await c.del('hello');             // true / false
//   c.close();

import net from 'node:net';

const HANDSHAKE = 1;
const REQUEST = 3;

const OP_GET = 1;
const OP_SET = 2;
const OP_DEL = 3;
const OP_SETEX = 4;

const NIL = Buffer.from('(nil)');

export class DatasetClient {
  constructor({ host = '127.0.0.1', port = 8383, maxInflight = 256, username = '', password = '' } = {}) {
    this.host = host;
    this.port = port;
    this.max = maxInflight;
    this.username = username;
    this.password = password;

    // Slot pool: req_id = slot index. A free slot guarantees a unique id and
    // doubles as backpressure (no free slot → the call waits).
    this.free = Array.from({ length: maxInflight }, (_, i) => i);
    this.pending = new Array(maxInflight).fill(null); // slot -> { resolve, reject }
    this.waiters = []; // resolvers waiting for a slot to free

    this.socket = null;
    this.buf = Buffer.alloc(0);
    this.closed = false;
    this.handshake = null; // { resolve, reject } until the OK/ERR reply arrives
  }

  // Resolves once the server replies OK to the handshake; rejects on auth failure
  // or connection error.
  connect() {
    return new Promise((resolve, reject) => {
      this.handshake = { resolve, reject };
      this.socket = net.connect(this.port, this.host, () => {
        this.socket.setNoDelay(true);
        this._writeFrame(HANDSHAKE, 0, this._credsPayload());
      });
      this.socket.on('data', (chunk) => this._onData(chunk));
      this.socket.on('error', (e) => this._fail(e));
      this.socket.on('close', () => this._fail(new Error('connection closed')));
    });
  }

  _credsPayload() {
    const u = Buffer.from(this.username);
    const p = Buffer.from(this.password);
    const buf = Buffer.allocUnsafe(2 + u.length + p.length);
    buf.writeUInt16BE(u.length, 0);
    u.copy(buf, 2);
    p.copy(buf, 2 + u.length);
    return buf;
  }

  close() {
    this.closed = true;
    if (this.socket) this.socket.end();
  }

  async get(key) {
    const k = Buffer.from(key);
    const payload = Buffer.allocUnsafe(3 + k.length);
    payload[0] = OP_GET;
    payload.writeUInt16BE(k.length, 1);
    k.copy(payload, 3);
    const res = await this._send(payload);
    return res.equals(NIL) ? null : res;
  }

  async set(key, value) {
    const k = Buffer.from(key);
    const v = Buffer.from(value);
    const payload = Buffer.allocUnsafe(3 + k.length + v.length);
    payload[0] = OP_SET;
    payload.writeUInt16BE(k.length, 1);
    k.copy(payload, 3);
    v.copy(payload, 3 + k.length);
    const res = await this._send(payload);
    return res.toString() === 'OK';
  }

  async setex(key, value, ttlMs) {
    const k = Buffer.from(key);
    const v = Buffer.from(value);
    const payload = Buffer.allocUnsafe(3 + k.length + 4 + v.length);
    payload[0] = OP_SETEX;
    payload.writeUInt16BE(k.length, 1);
    k.copy(payload, 3);
    payload.writeUInt32BE(ttlMs >>> 0, 3 + k.length);
    v.copy(payload, 3 + k.length + 4);
    const res = await this._send(payload);
    return res.toString() === 'OK';
  }

  async del(key) {
    const k = Buffer.from(key);
    const payload = Buffer.allocUnsafe(3 + k.length);
    payload[0] = OP_DEL;
    payload.writeUInt16BE(k.length, 1);
    k.copy(payload, 3);
    const res = await this._send(payload);
    return res.toString() === '1';
  }

  // --- internals ---

  async _send(payload) {
    if (this.closed) throw new Error('client closed');
    const id = await this._acquire();
    const result = new Promise((resolve, reject) => {
      this.pending[id] = { resolve, reject };
    });
    this._writeFrame(REQUEST, id, payload);
    return result;
  }

  _acquire() {
    const id = this.free.pop();
    if (id !== undefined) return Promise.resolve(id);
    return new Promise((resolve) => this.waiters.push(resolve)); // backpressure
  }

  _release(id) {
    const waiter = this.waiters.shift();
    if (waiter) waiter(id); // hand the freed slot straight to a waiting caller
    else this.free.push(id);
  }

  _writeFrame(header, reqId, payload) {
    const frame = Buffer.allocUnsafe(7 + payload.length);
    frame[0] = header;
    frame.writeUInt32BE(reqId >>> 0, 1);
    frame.writeUInt16BE(payload.length, 5);
    payload.copy(frame, 7);
    this.socket.write(frame);
  }

  _onData(chunk) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    while (this.buf.length >= 7) {
      const len = this.buf.readUInt16BE(5);
      if (this.buf.length < 7 + len) break;
      const id = this.buf.readUInt32BE(1);
      const payload = this.buf.subarray(7, 7 + len);
      this.buf = this.buf.subarray(7 + len);

      // The very first frame is the handshake reply.
      if (this.handshake) {
        const hs = this.handshake;
        this.handshake = null;
        if (payload.toString() === 'OK') hs.resolve();
        else hs.reject(new Error(payload.toString()));
        continue;
      }

      const slot = this.pending[id];
      if (slot) {
        this.pending[id] = null;
        this._release(id);
        slot.resolve(payload);
      }
    }
  }

  _fail(err) {
    if (this.closed && err.message === 'connection closed') err = null;
    if (this.handshake && err) {
      this.handshake.reject(err);
      this.handshake = null;
    }
    for (let i = 0; i < this.pending.length; i++) {
      const slot = this.pending[i];
      if (slot) {
        this.pending[i] = null;
        if (err) slot.reject(err);
      }
    }
  }
}

// Minimal self-test: node dataset-client.mjs  (needs the server on :8383)
if (import.meta.url === `file://${process.argv[1]}`) {
  const c = new DatasetClient({ port: 8383 });
  await c.connect();

  console.log('set  ->', await c.set('hello', 'world')); // true
  console.log('get  ->', (await c.get('hello'))?.toString()); // world
  console.log('del  ->', await c.del('hello')); // true
  console.log('miss ->', await c.get('hello')); // null

  // Concurrency: 5000 requests in flight at once, matched by req_id out of order.
  const N = 5000;
  await Promise.all(
    Array.from({ length: N }, (_, i) => c.set(`k${i}`, `v${i}`)),
  );
  const vals = await Promise.all(
    Array.from({ length: N }, (_, i) => c.get(`k${i}`)),
  );
  const ok = vals.every((v, i) => v?.toString() === `v${i}`);
  console.log(`concurrent ${N} get, all matched:`, ok);

  c.close();
}
