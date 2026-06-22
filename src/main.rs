mod common;
mod hashing;
pub mod server;
mod shards;
pub mod utils;

use std::thread::available_parallelism;

use tokio::sync::mpsc::unbounded_channel;
use tracing::info;

use crate::{
    server::{ServerListener, WorkerTx},
    shards::state::State,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Worker count defaults to the CPU count, but can be pinned via the WORKERS
    // env var (useful for measuring how throughput scales with shard parallelism).
    // Always rounded to a power of two because routing uses a `& (N - 1)` mask.
    let worker_count = std::env::var("WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| available_parallelism().map(|n| n.get()).unwrap_or(4))
        .next_power_of_two();

    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, rx) = unbounded_channel();
        let state = State::new(rx);
        tokio::spawn(state.start_worker());
        workers.push(WorkerTx::new(tx));
    }
    info!("spawned {worker_count} workers");

    let server = ServerListener::new(workers);
    server.start().await
}
