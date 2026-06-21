use tokio::sync::mpsc::UnboundedReceiver;

use crate::{shards::worker::Worker, utils::WorkerCommand};

pub struct State {
    worker_id: usize,
    worker: Worker,
    rx: UnboundedReceiver<WorkerCommand>,
}

impl State {
    pub fn new(worker_id: usize, rx: UnboundedReceiver<WorkerCommand>) -> Self {
        Self {
            worker_id,
            worker: Worker::start(),
            rx,
        }
    }

    pub async fn start_worker(mut self) {
        loop {
            let event_otp = { self.rx.recv().await };

            if let Some(event) = event_otp {
                match event {
                    WorkerCommand::Get { key, tx } => {
                        let res = self.worker.get(&key);
                        let _ = tx.send(res);
                    }
                    WorkerCommand::Set { key, value, tx } => {
                        let res = self.worker.set(key, value);
                        let _ = tx.send(res);
                    }
                    WorkerCommand::Del { key, tx } => {
                        let res = self.worker.delete(&key);
                        let _ = tx.send(res);
                    }
                }
            }
        }
    }
}
