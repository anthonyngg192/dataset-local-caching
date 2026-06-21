use bytes::Bytes;
use tokio::sync::oneshot;

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
