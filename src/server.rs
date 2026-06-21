use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;
use tracing::{error, info};

use crate::{
    common::{Frame, FrameKind, SimpleCodec},
    hashing::route_hash,
    utils::{ClientCommand, WorkerCommand},
};

/// Max number of in-flight requests buffered per connection before the reader
/// stops pulling from the socket. This bound is what turns "don't wait for the
/// response" into safe backpressure: when the queue is full the reader blocks on
/// `send().await`, the kernel TCP window closes, and the client is throttled —
/// no request is lost and memory stays bounded.
const PIPELINE_DEPTH: usize = 1024;

// Pre-built response payloads for the common cases.
const RESP_OK: Bytes = Bytes::from_static(b"OK");
const RESP_NIL: Bytes = Bytes::from_static(b"(nil)");
const RESP_ONE: Bytes = Bytes::from_static(b"1");
const RESP_ZERO: Bytes = Bytes::from_static(b"0");
const RESP_WORKER_GONE: Bytes = Bytes::from_static(b"ERR worker dropped response");

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
}

impl ServerListener {
    pub fn new(workers: Vec<WorkerTx>) -> Self {
        Self {
            workers: Arc::new(workers),
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("0.0.0.0:8383").await?;
        info!(
            "listening on 0.0.0.0:8383 with {} workers",
            self.workers.len()
        );

        loop {
            let (socket, peer) = listener.accept().await?;
            let workers = Arc::clone(&self.workers);

            tokio::spawn(async move {
                if let Err(e) = handle_conn(socket, workers).await {
                    error!("connection {peer} ended: {e}");
                }
            });
        }
    }
}

/// A response that has been routed but not yet produced. The writer pulls these
/// in FIFO order and awaits each one, so responses go out in exactly the order
/// the requests arrived — no correlation id needed.
enum Pending {
    /// Already-known payload (heartbeat, handshake reply, parse/routing errors).
    Immediate(Bytes),
    /// Awaiting a worker reply; the variant remembers how to format it.
    Get(oneshot::Receiver<Option<Bytes>>),
    Set(oneshot::Receiver<bool>),
    Del(oneshot::Receiver<bool>),
}

impl Pending {
    async fn resolve(self) -> Bytes {
        match self {
            Pending::Immediate(payload) => payload,
            Pending::Get(rx) => match rx.await {
                Ok(Some(value)) => value,
                Ok(None) => RESP_NIL,
                Err(_) => RESP_WORKER_GONE,
            },
            Pending::Set(rx) => match rx.await {
                Ok(_) => RESP_OK,
                Err(_) => RESP_WORKER_GONE,
            },
            Pending::Del(rx) => match rx.await {
                Ok(true) => RESP_ONE,
                Ok(false) => RESP_ZERO,
                Err(_) => RESP_WORKER_GONE,
            },
        }
    }
}

async fn handle_conn(
    socket: tokio::net::TcpStream,
    workers: Arc<Vec<WorkerTx>>,
) -> anyhow::Result<()> {
    let framed = Framed::new(socket, SimpleCodec);
    let (mut writer, mut reader) = framed.split();

    // 1) Handshake must come first.
    match reader.next().await {
        Some(Ok(frame)) if frame.header == FrameKind::HandShake => {
            info!("handshake success");
        }
        Some(Ok(_)) => {
            writer
                .send(Frame {
                    header: FrameKind::Response,
                    payload: Bytes::from_static(
                        b"Please complete handshake process before sending commands",
                    ),
                })
                .await?;
            return Ok(());
        }
        Some(Err(e)) => return Err(e.into()),
        None => anyhow::bail!("connection ended during handshake"),
    }

    // 2) Ordered completion queue connecting the reader to the writer.
    let (queue_tx, mut queue_rx) = mpsc::channel::<Pending>(PIPELINE_DEPTH);

    // Writer half: drain pending responses in FIFO order.
    let writer_task = tokio::spawn(async move {
        while let Some(pending) = queue_rx.recv().await {
            let payload = pending.resolve().await;
            if writer
                .send(Frame {
                    header: FrameKind::Response,
                    payload,
                })
                .await
                .is_err()
            {
                break; // socket closed
            }
        }
    });

    // Reader half: decode frames, route them, hand the pending response to the
    // writer — without waiting for the worker. `queue_tx.send().await` applies
    // backpressure once PIPELINE_DEPTH requests are outstanding.
    while let Some(frame) = reader.next().await {
        let frame = frame?;
        let pending = route_frame(&workers, frame);
        if queue_tx.send(pending).await.is_err() {
            break; // writer task gone
        }
    }

    // Closing queue_tx lets the writer drain remaining responses and finish.
    drop(queue_tx);
    let _ = writer_task.await;
    Ok(())
}

/// Turn a decoded frame into a `Pending` response, dispatching to a worker where
/// needed. Does not await the worker.
fn route_frame(workers: &[WorkerTx], frame: Frame) -> Pending {
    match frame.header {
        FrameKind::Request => match ClientCommand::parse(&frame.payload) {
            Ok(cmd) => dispatch(workers, cmd),
            Err(e) => Pending::Immediate(Bytes::from(format!("ERR {e}"))),
        },
        FrameKind::Heartbeat => Pending::Immediate(RESP_ONE),
        FrameKind::HandShake => {
            Pending::Immediate(Bytes::from_static(b"Handshake process already completed"))
        }
        FrameKind::Response => Pending::Immediate(Bytes::from_static(b"Invalid command")),
    }
}

/// Route a command to its owning worker and return the pending reply handle.
fn dispatch(workers: &[WorkerTx], cmd: ClientCommand) -> Pending {
    match cmd {
        ClientCommand::Get { key } => {
            let (tx, rx) = oneshot::channel();
            match worker_for(workers, &key).send(WorkerCommand::Get {
                key: key.to_vec(),
                tx,
            }) {
                Ok(()) => Pending::Get(rx),
                Err(e) => Pending::Immediate(Bytes::from(format!("ERR {e}"))),
            }
        }
        ClientCommand::Set { key, value } => {
            let (tx, rx) = oneshot::channel();
            match worker_for(workers, &key).send(WorkerCommand::Set {
                key: key.to_vec(),
                value,
                tx,
            }) {
                Ok(()) => Pending::Set(rx),
                Err(e) => Pending::Immediate(Bytes::from(format!("ERR {e}"))),
            }
        }
        ClientCommand::Del { key } => {
            let (tx, rx) = oneshot::channel();
            match worker_for(workers, &key).send(WorkerCommand::Del {
                key: key.to_vec(),
                tx,
            }) {
                Ok(()) => Pending::Del(rx),
                Err(e) => Pending::Immediate(Bytes::from(format!("ERR {e}"))),
            }
        }
    }
}

fn worker_for<'a>(workers: &'a [WorkerTx], key: &[u8]) -> &'a WorkerTx {
    let shard = route_hash(key, workers.len()).shard_id;
    &workers[shard]
}
