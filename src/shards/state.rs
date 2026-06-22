use bytes::Bytes;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    shards::worker::Worker,
    utils::{RESP_NIL, RESP_OK, RESP_ONE, RESP_ZERO, WorkerCommand, WorkerOp},
};

pub struct State {
    worker: Worker,
    rx: UnboundedReceiver<WorkerCommand>,
}

impl State {
    pub fn new(rx: UnboundedReceiver<WorkerCommand>) -> Self {
        Self {
            worker: Worker::start(),
            rx,
        }
    }

    pub async fn start_worker(mut self) {
        while let Some(WorkerCommand::Batch { ops, reply }) = self.rx.recv().await {
            // Apply each op and send its tagged response straight back. Order is
            // irrelevant — the client reorders by req_id.
            for (req_id, op) in ops {
                let payload = self.apply(op);
                let _ = reply.send((req_id, payload));
            }
        }
    }

    fn apply(&mut self, op: WorkerOp) -> Bytes {
        match op {
            WorkerOp::Get { key } => self.worker.get(&key).unwrap_or(RESP_NIL),
            WorkerOp::Set { key, value } => {
                self.worker.set(key, value);
                RESP_OK
            }
            WorkerOp::Del { key } => {
                if self.worker.delete(&key) {
                    RESP_ONE
                } else {
                    RESP_ZERO
                }
            }
        }
    }
}
