use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc::UnboundedSender, oneshot},
};
use tokio_util::codec::Framed;
use tracing::{error, info};

use crate::{
    common::{Frame, FrameKind, SimpleCodec},
    hashing::route_hash,
    utils::{ClientCommand, WorkerCommand},
};

#[derive(Clone)]
pub struct WorkerTx {
    tx: UnboundedSender<WorkerCommand>,
}

impl WorkerTx {
    pub fn new(tx: UnboundedSender<WorkerCommand>) -> Self {
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

async fn handle_conn(
    socket: tokio::net::TcpStream,
    workers: Arc<Vec<WorkerTx>>,
) -> anyhow::Result<()> {
    let framed = Framed::new(socket, SimpleCodec);
    let (mut writer, mut reader) = framed.split();

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
        None => {
            anyhow::bail!("connection ended during handshake");
        }
    }

    while let Some(frame) = reader.next().await {
        let frame = frame?;
        let payload = match frame.header {
            FrameKind::Request => match ClientCommand::parse(&frame.payload) {
                Ok(cmd) => dispatch(&workers, cmd).await,
                Err(e) => Bytes::from(format!("ERR {e}")),
            },
            FrameKind::Heartbeat => Bytes::from_static(b"1"),
            FrameKind::HandShake => Bytes::from_static(b"Handshake process already completed"),
            FrameKind::Response => Bytes::from_static(b"Invalid command"),
        };

        writer
            .send(Frame {
                header: FrameKind::Response,
                payload,
            })
            .await?;
    }

    Ok(())
}

async fn dispatch(workers: &[WorkerTx], cmd: ClientCommand) -> Bytes {
    match cmd {
        ClientCommand::Get { key } => {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = worker_for(workers, &key).send(WorkerCommand::Get {
                key: key.to_vec(),
                tx,
            }) {
                return Bytes::from(format!("ERR {e}"));
            }
            match rx.await {
                Ok(Some(value)) => value,
                Ok(None) => Bytes::from_static(b"(nil)"),
                Err(_) => Bytes::from_static(b"ERR worker dropped response"),
            }
        }
        ClientCommand::Set { key, value } => {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = worker_for(workers, &key).send(WorkerCommand::Set {
                key: key.to_vec(),
                value,
                tx,
            }) {
                return Bytes::from(format!("ERR {e}"));
            }
            match rx.await {
                Ok(_) => Bytes::from_static(b"OK"),
                Err(_) => Bytes::from_static(b"ERR worker dropped response"),
            }
        }
        ClientCommand::Del { key } => {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = worker_for(workers, &key).send(WorkerCommand::Del {
                key: key.to_vec(),
                tx,
            }) {
                return Bytes::from(format!("ERR {e}"));
            }
            match rx.await {
                Ok(true) => Bytes::from_static(b"1"),
                Ok(false) => Bytes::from_static(b"0"),
                Err(_) => Bytes::from_static(b"ERR worker dropped response"),
            }
        }
    }
}

fn worker_for<'a>(workers: &'a [WorkerTx], key: &[u8]) -> &'a WorkerTx {
    let shard = route_hash(key, workers.len()).shard_id;
    &workers[shard]
}
