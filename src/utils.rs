use anyhow::{Result, bail};
use bytes::Bytes;
use tokio::sync::oneshot;

// Request payload wire format:
//   [op:u8][klen:u16 be][key bytes][value bytes (chỉ SET)]
// op: 1 = GET, 2 = SET, 3 = DEL
const OP_GET: u8 = 1;
const OP_SET: u8 = 2;
const OP_DEL: u8 = 3;

#[derive(Debug)]
pub enum WorkerCommand {
    Get {
        key: Vec<u8>,
        tx: oneshot::Sender<Option<Bytes>>,
    },
    Set {
        key: Vec<u8>,
        value: Bytes,
        tx: oneshot::Sender<bool>,
    },
    Del {
        key: Vec<u8>,
        tx: oneshot::Sender<bool>,
    },
}

#[derive(Debug)]
pub enum ClientCommand {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
}

impl ClientCommand {
    /// Parse một Request frame payload thành ClientCommand.
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
