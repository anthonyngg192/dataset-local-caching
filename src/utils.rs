use anyhow::{Result, bail};
use bytes::Bytes;
use tokio::sync::mpsc;

// Request payload wire format:
//   [op:u8][klen:u16 be][key bytes][value bytes (chỉ SET)]
// op: 1 = GET, 2 = SET, 3 = DEL
const OP_GET: u8 = 1;
const OP_SET: u8 = 2;
const OP_DEL: u8 = 3;

// Ready-made response payloads, shared by the worker (which now formats replies)
// and the server (immediate replies). `Bytes::from_static` is a const fn so these
// are zero-cost to clone.
pub const RESP_OK: Bytes = Bytes::from_static(b"OK");
pub const RESP_NIL: Bytes = Bytes::from_static(b"(nil)");
pub const RESP_ONE: Bytes = Bytes::from_static(b"1");
pub const RESP_ZERO: Bytes = Bytes::from_static(b"0");
pub const RESP_WORKER_GONE: Bytes = Bytes::from_static(b"ERR worker dropped response");

/// A single store operation. No reply channel: the worker now formats the whole
/// batch's responses and returns them together, so the per-op `oneshot` is gone.
#[derive(Debug)]
pub enum WorkerOp {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
}

/// Message sent from a connection to a worker: a batch of `(req_id, op)` plus the
/// originating connection's reply channel. The worker applies each op and sends
/// `(req_id, response)` straight back — in any order, no gather, no reorder. The
/// client matches responses to requests by `req_id`.
#[derive(Debug)]
pub enum WorkerCommand {
    Batch {
        ops: Vec<(u32, WorkerOp)>,
        reply: mpsc::UnboundedSender<(u32, Bytes)>,
    },
}

#[derive(Debug)]
pub enum ClientCommand {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
}

impl ClientCommand {
    pub fn parse(buf: &Bytes) -> Result<Self> {
        let Some((&op, rest)) = buf.split_first() else {
            bail!("empty request payload");
        };

        match op {
            OP_GET => {
                let (key, _) = read_field(rest)?;
                Ok(ClientCommand::Get { key })
            }
            OP_SET => {
                let (key, after) = read_field(rest)?;
                Ok(ClientCommand::Set {
                    key,
                    value: Bytes::copy_from_slice(after),
                })
            }
            OP_DEL => {
                let (key, _) = read_field(rest)?;
                Ok(ClientCommand::Del { key })
            }
            other => bail!("unknown op {other}"),
        }
    }
}

/// [len:u16 be][bytes]
fn read_field(buf: &[u8]) -> Result<(Bytes, &[u8])> {
    if buf.len() < 2 {
        bail!("truncated field length");
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let end = 2 + len;
    if buf.len() < end {
        bail!("truncated field body");
    }
    Ok((Bytes::copy_from_slice(&buf[2..end]), &buf[end..]))
}
