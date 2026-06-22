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
    // Load .env if present. Real environment variables take precedence, so a
    // `docker run -e ...` or an exported var overrides the file.
    dotenvy::dotenv().ok();
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

    // Memory budget: MAX_MEMORY_MB is the total across all shards (0/unset =
    // unlimited). Split evenly per shard.
    let budget_per_shard = std::env::var("MAX_MEMORY_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&mb| mb > 0)
        .map(|mb| mb * 1024 * 1024 / worker_count)
        .unwrap_or(0);

    // Each shard persists to its own directory under DATA_DIR (shared-nothing).
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());

    let mut workers = Vec::with_capacity(worker_count);
    for id in 0..worker_count {
        let (tx, rx) = unbounded_channel();
        let dir = std::path::Path::new(&data_dir).join(format!("shard-{id}"));
        let state = State::new(rx, budget_per_shard, &dir);
        tokio::spawn(state.start_worker());
        workers.push(WorkerTx::new(tx));
    }
    info!(
        "spawned {worker_count} workers (budget/shard: {} MB, data: {data_dir})",
        budget_per_shard / 1024 / 1024
    );

    // Auth is enabled only when both env vars are set.
    let auth = match (std::env::var("DATASET_USERNAME"), std::env::var("DATASET_PASSWORD")) {
        (Ok(user), Ok(pass)) if !user.is_empty() && !pass.is_empty() => Some((user, pass)),
        _ => None,
    };

    let server = ServerListener::new(workers, auth);
    server.start().await
}
