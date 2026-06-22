use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::codec::Framed;
use tracing::{error, info};

use crate::{
    common::{Frame, FrameKind, SimpleCodec},
    hashing::route_hash,
    utils::{ClientCommand, RESP_ONE, WorkerCommand, WorkerOp},
};

/// Upper bound on how many frames a single reader pass groups into one batch.
const MAX_BATCH: usize = 1024;

#[derive(Clone)]
pub struct WorkerTx {
    tx: mpsc::UnboundedSender<WorkerCommand>,
}

impl WorkerTx {
    pub fn new(tx: mpsc::UnboundedSender<WorkerCommand>) -> Self {
        Self { tx }
    }

    fn send(&self, cmd: WorkerCommand) -> anyhow::Result<()> {
        self.tx
            .send(cmd)
            .map_err(|_| anyhow::anyhow!("worker channel closed"))
    }
}

pub struct ServerListener {
    workers: Arc<Vec<WorkerTx>>,
    /// Optional `(username, password)`. `None` = auth disabled (any handshake ok).
    auth: Arc<Option<(String, String)>>,
}

impl ServerListener {
    pub fn new(workers: Vec<WorkerTx>, auth: Option<(String, String)>) -> Self {
        Self {
            workers: Arc::new(workers),
            auth: Arc::new(auth),
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("0.0.0.0:8383").await?;
        info!(
            "listening on 0.0.0.0:8383 with {} workers (auth {})",
            self.workers.len(),
            if self.auth.is_some() { "on" } else { "off" }
        );

        loop {
            let (socket, peer) = listener.accept().await?;
            let workers = Arc::clone(&self.workers);
            let auth = Arc::clone(&self.auth);

            tokio::spawn(async move {
                if let Err(e) = handle_conn(socket, workers, auth).await {
                    error!("connection {peer} ended: {e}");
                }
            });
        }
    }
}

/// The plan for one decoded frame: an immediate reply, or an op routed to a shard.
enum Plan {
    Immediate(Bytes),
    Op { shard: usize, op: WorkerOp },
}

async fn handle_conn(
    socket: tokio::net::TcpStream,
    workers: Arc<Vec<WorkerTx>>,
    auth: Arc<Option<(String, String)>>,
) -> anyhow::Result<()> {
    let framed = Framed::new(socket, SimpleCodec);
    let (mut writer, mut reader) = framed.split();

    // 1) Handshake must come first, and the server always replies OK/ERR so the
    // client knows whether it is authenticated before sending commands.
    match reader.next().await {
        Some(Ok(frame)) if frame.header == FrameKind::HandShake => {
            let (user, pass) = parse_credentials(&frame.payload);
            let ok = match &*auth {
                None => true,
                Some((u, p)) => user == u.as_bytes() && pass == p.as_bytes(),
            };
            let payload = if ok {
                Bytes::from_static(b"OK")
            } else {
                Bytes::from_static(b"ERR auth failed")
            };
            writer
                .send(Frame {
                    header: FrameKind::Response,
                    req_id: frame.req_id,
                    payload,
                })
                .await?;
            if !ok {
                return Ok(());
            }
            info!("handshake ok");
        }
        Some(Ok(frame)) => {
            writer
                .send(Frame {
                    header: FrameKind::Response,
                    req_id: frame.req_id,
                    payload: Bytes::from_static(
                        b"ERR complete handshake before sending commands",
                    ),
                })
                .await?;
            return Ok(());
        }
        Some(Err(e)) => return Err(e.into()),
        None => anyhow::bail!("connection ended during handshake"),
    }

    // 2) Per-connection reply channel. Workers (and the reader, for immediate
    // replies) push `(req_id, payload)` here; the writer drains and emits them in
    // whatever order they arrive — the client reorders by req_id.
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<(u32, Bytes)>();

    let writer_task = tokio::spawn(async move {
        while let Some((req_id, payload)) = reply_rx.recv().await {
            if writer
                .send(Frame {
                    header: FrameKind::Response,
                    req_id,
                    payload,
                })
                .await
                .is_err()
            {
                break; // socket closed
            }
        }
    });

    // 3) Reader: decode a pass of frames, send immediates straight back, bucket
    // ops by shard, and dispatch one batch per shard. No ordering to maintain —
    // each batch carries this connection's reply channel.
    let mut reader = reader.ready_chunks(MAX_BATCH);
    let mut groups: Vec<Vec<(u32, WorkerOp)>> = (0..workers.len()).map(|_| Vec::new()).collect();

    while let Some(chunk) = reader.next().await {
        for frame in chunk {
            let frame = frame?;
            let req_id = frame.req_id;
            match plan(&workers, frame) {
                Plan::Immediate(payload) => {
                    if reply_tx.send((req_id, payload)).is_err() {
                        return Ok(()); // writer gone
                    }
                }
                Plan::Op { shard, op } => groups[shard].push((req_id, op)),
            }
        }

        for (shard, ops) in groups.iter_mut().enumerate() {
            if ops.is_empty() {
                continue;
            }
            let batch: Vec<(u32, WorkerOp)> = ops.drain(..).collect();
            if workers[shard]
                .send(WorkerCommand::Batch {
                    ops: batch,
                    reply: reply_tx.clone(),
                })
                .is_err()
            {
                return Ok(()); // worker gone
            }
        }
    }

    // Dropping our sender lets the writer finish once all worker clones drain.
    drop(reply_tx);
    let _ = writer_task.await;
    Ok(())
}

/// Parse a handshake payload `[ulen:u16 BE][username][password]`. An empty
/// payload yields empty credentials (used when auth is disabled).
fn parse_credentials(p: &[u8]) -> (&[u8], &[u8]) {
    if p.len() < 2 {
        return (&[], &[]);
    }
    let ulen = u16::from_be_bytes([p[0], p[1]]) as usize;
    if p.len() < 2 + ulen {
        return (&[], &[]);
    }
    (&p[2..2 + ulen], &p[2 + ulen..])
}

/// Turn a decoded frame into a plan. Pure routing — no awaiting, no sending.
fn plan(workers: &[WorkerTx], frame: Frame) -> Plan {
    match frame.header {
        FrameKind::Request => match ClientCommand::parse(&frame.payload) {
            Ok(cmd) => plan_op(workers.len(), cmd),
            Err(e) => Plan::Immediate(Bytes::from(format!("ERR {e}"))),
        },
        FrameKind::Heartbeat => Plan::Immediate(RESP_ONE),
        FrameKind::HandShake => {
            Plan::Immediate(Bytes::from_static(b"Handshake process already completed"))
        }
        FrameKind::Response => Plan::Immediate(Bytes::from_static(b"Invalid command")),
    }
}

/// Route a command to its owning shard.
fn plan_op(worker_count: usize, cmd: ClientCommand) -> Plan {
    let shard = |key: &[u8]| route_hash(key, worker_count).shard_id;
    match cmd {
        ClientCommand::Get { key } => Plan::Op {
            shard: shard(&key),
            op: WorkerOp::Get { key },
        },
        ClientCommand::Set { key, value } => Plan::Op {
            shard: shard(&key),
            op: WorkerOp::Set { key, value },
        },
        ClientCommand::SetEx { key, value, ttl_ms } => Plan::Op {
            shard: shard(&key),
            op: WorkerOp::SetEx {
                key,
                value,
                ttl: std::time::Duration::from_millis(ttl_ms as u64),
            },
        },
        ClientCommand::Del { key } => Plan::Op {
            shard: shard(&key),
            op: WorkerOp::Del { key },
        },
    }
}
