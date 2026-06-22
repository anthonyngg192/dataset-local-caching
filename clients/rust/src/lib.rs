//! dataset-local client for Rust — implements the v2 multiplexed protocol.
//! See `../../PROTOCOL.md` for the wire contract.
//!
//! ```no_run
//! # async fn run() {
//! use dataset_client::Client;
//! let c = Client::connect("127.0.0.1:8383", 256, "", "").await.unwrap();
//! c.set(b"hello", b"world").await;
//! let v = c.get(b"hello").await; // Some(Bytes "world"), or None on miss
//! c.del(b"hello").await;
//! # }
//! ```

use std::sync::Arc;

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Mutex, Semaphore, oneshot},
};

const HANDSHAKE: u8 = 1;
const REQUEST: u8 = 3;
const OP_GET: u8 = 1;
const OP_SET: u8 = 2;
const OP_DEL: u8 = 3;
const OP_SETEX: u8 = 4;

/// Outstanding requests, indexed by `req_id` (= slot). A fixed array of
/// `oneshot` senders plus a free list — O(1) allocate/lookup, minimal locking.
struct Slots {
    senders: Vec<Option<oneshot::Sender<Bytes>>>,
    free: Vec<u32>,
}

pub struct Client {
    write: Mutex<OwnedWriteHalf>,
    slots: Arc<Mutex<Slots>>,
    // Backpressure: `max_inflight` permits. Holding a permit guarantees a free
    // slot exists, so allocation never fails.
    sem: Semaphore,
}

impl Client {
    /// Connect and authenticate. Pass empty `username`/`password` when the server
    /// has auth disabled. Errors if the server rejects the handshake.
    pub async fn connect(
        addr: &str,
        max_inflight: usize,
        username: &str,
        password: &str,
    ) -> std::io::Result<Arc<Self>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true).ok();
        let (read_half, mut write_half) = stream.into_split();

        // Handshake: header=1, req_id=0, payload = [ulen:u16][user][pass].
        let mut creds = Vec::with_capacity(2 + username.len() + password.len());
        creds.extend_from_slice(&(username.len() as u16).to_be_bytes());
        creds.extend_from_slice(username.as_bytes());
        creds.extend_from_slice(password.as_bytes());
        let mut frame = Vec::with_capacity(7 + creds.len());
        frame.push(HANDSHAKE);
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&(creds.len() as u16).to_be_bytes());
        frame.extend_from_slice(&creds);
        write_half.write_all(&frame).await?;

        // Read the handshake reply (OK / ERR ...).
        let mut reader = BufReader::new(read_half);
        let mut head = [0u8; 7];
        reader.read_exact(&mut head).await?;
        let len = u16::from_be_bytes([head[5], head[6]]) as usize;
        let mut body = vec![0u8; len];
        reader.read_exact(&mut body).await?;
        if body != b"OK" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                String::from_utf8_lossy(&body).into_owned(),
            ));
        }

        let slots = Arc::new(Mutex::new(Slots {
            senders: (0..max_inflight).map(|_| None).collect(),
            free: (0..max_inflight as u32).collect(),
        }));

        tokio::spawn(read_loop(reader, slots.clone()));

        Ok(Arc::new(Self {
            write: Mutex::new(write_half),
            slots,
            sem: Semaphore::new(max_inflight),
        }))
    }

    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let res = self.call(encode(OP_GET, key, &[])).await;
        if res.as_ref() == b"(nil)" {
            None
        } else {
            Some(res)
        }
    }

    pub async fn set(&self, key: &[u8], value: &[u8]) -> bool {
        self.call(encode(OP_SET, key, value)).await.as_ref() == b"OK"
    }

    /// SET with a time-to-live in milliseconds.
    pub async fn setex(&self, key: &[u8], value: &[u8], ttl_ms: u32) -> bool {
        let mut p = Vec::with_capacity(3 + key.len() + 4 + value.len());
        p.push(OP_SETEX);
        p.extend_from_slice(&(key.len() as u16).to_be_bytes());
        p.extend_from_slice(key);
        p.extend_from_slice(&ttl_ms.to_be_bytes());
        p.extend_from_slice(value);
        self.call(p).await.as_ref() == b"OK"
    }

    pub async fn del(&self, key: &[u8]) -> bool {
        self.call(encode(OP_DEL, key, &[])).await.as_ref() == b"1"
    }

    /// Send one request payload and await its response, matched by `req_id`.
    async fn call(&self, payload: Vec<u8>) -> Bytes {
        // Acquire a permit (waits if max_inflight requests are outstanding).
        let _permit = self.sem.acquire().await.expect("semaphore closed");

        let (tx, rx) = oneshot::channel();
        let id = {
            let mut s = self.slots.lock().await;
            let id = s.free.pop().expect("permit guarantees a free slot");
            s.senders[id as usize] = Some(tx);
            id
        };

        let mut frame = Vec::with_capacity(7 + payload.len());
        frame.push(REQUEST);
        frame.extend_from_slice(&id.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        frame.extend_from_slice(&payload);
        {
            let mut w = self.write.lock().await;
            let _ = w.write_all(&frame).await;
        }

        rx.await
            .unwrap_or_else(|_| Bytes::from_static(b"ERR connection closed"))
        // `_permit` drops here, releasing one unit of backpressure.
    }
}

async fn read_loop(mut reader: BufReader<OwnedReadHalf>, slots: Arc<Mutex<Slots>>) {
    let mut head = [0u8; 7];
    loop {
        if reader.read_exact(&mut head).await.is_err() {
            break;
        }
        let id = u32::from_be_bytes([head[1], head[2], head[3], head[4]]);
        let len = u16::from_be_bytes([head[5], head[6]]) as usize;
        let mut body = vec![0u8; len];
        if len > 0 && reader.read_exact(&mut body).await.is_err() {
            break;
        }

        let sender = {
            let mut s = slots.lock().await;
            let sender = s.senders[id as usize].take();
            s.free.push(id);
            sender
        };
        if let Some(sender) = sender {
            let _ = sender.send(Bytes::from(body));
        }
    }

    // Connection closed: drop every outstanding sender so awaiting callers get a
    // RecvError instead of hanging forever.
    let mut s = slots.lock().await;
    for slot in s.senders.iter_mut() {
        *slot = None;
    }
}

/// Build a request payload: `[op][klen:u16 BE][key][value]`.
fn encode(op: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(3 + key.len() + value.len());
    p.push(op);
    p.extend_from_slice(&(key.len() as u16).to_be_bytes());
    p.extend_from_slice(key);
    p.extend_from_slice(value);
    p
}
