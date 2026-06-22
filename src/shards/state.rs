use std::path::Path;

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
    pub fn new(rx: UnboundedReceiver<WorkerCommand>, budget_bytes: usize, dir: &Path) -> Self {
        Self {
            worker: Worker::start(budget_bytes, dir),
            rx,
        }
    }

    pub async fn start_worker(mut self) {
        // The shard self-expires on a timer. Lazy expiration handles correctness
        // on every read; this just reclaims memory from keys nobody touches.
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    Some(WorkerCommand::Batch { ops, reply }) => {
                        // Apply each op and send its tagged response straight back.
                        // Order is irrelevant — the client reorders by req_id.
                        for (req_id, op) in ops {
                            let payload = self.apply(op);
                            let _ = reply.send((req_id, payload));
                        }
                        // After acking: push disk writes to the OS, then evict.
                        // (Both off the client's response latency path.)
                        self.worker.flush_os();
                        self.worker.evict_to_budget();
                    }
                    None => break, // all connections dropped
                },
                _ = tick.tick() => {
                    self.worker.fsync(); // batched durability
                    self.worker.expire_due(256);
                }
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
            WorkerOp::SetEx { key, value, ttl } => {
                self.worker.set_ex(key, value, ttl);
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
