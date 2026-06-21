use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpListener, sync::mpsc::UnboundedSender};
use tokio_util::codec::Framed;
use tracing::info;

use crate::{
    common::{Frame, FrameKind, SimpleCodec},
    utils::WorkerCommand,
};

pub struct WorkerTx {
    tx: UnboundedSender<WorkerCommand>,
}

pub struct ServerListener {
    workers: Vec<WorkerTx>,
}

impl ServerListener {
    pub fn new(workers: Vec<WorkerTx>) -> Self {
        Self { workers }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("0.0.0.0:83831").await?;

        loop {
            let (socket, _) = listener.accept().await?;

            tokio::spawn(async move {
                let framed = Framed::new(socket, SimpleCodec);

                let (mut writer, mut reader) = framed.split();

                if let Some(handshake) = reader.next().await {
                    if let Ok(frame) = handshake {
                        if frame.header == FrameKind::HandShake {
                            info!("handshake success");
                        } else {
                            let _ = writer.send(Frame {
                                header: FrameKind::Response,
                                payload: Bytes::from(
                                    "Please completed handshake process before send connect",
                                ),
                            });
                            return;
                        }
                    } else {
                        return;
                    }
                } else {
                    tracing::error!("Connection ended, during handshake");
                    return;
                }

                if let Some(Ok(event_pt)) = reader.next().await {
                    match event_pt.header {
                        FrameKind::HandShake => {
                            let _ = writer.send(Frame {
                                header: FrameKind::Response,
                                payload: Bytes::from("Handshake process already completed"),
                            });
                        }
                        FrameKind::Response => {
                            let _ = writer.send(Frame {
                                header: FrameKind::Response,
                                payload: Bytes::from("Invalid command"),
                            });
                        }
                        FrameKind::Request => {}
                        FrameKind::Heartbeat => {
                            let _ = writer.send(Frame {
                                header: FrameKind::Response,
                                payload: Bytes::from("1"),
                            });
                        }
                    }
                } else {
                    tracing::error!("Connection ended");
                    return;
                }
            });
        }
    }
}
