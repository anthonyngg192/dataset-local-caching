//! Closed-loop load generator for the dataset-local server.
//!
//! It opens N concurrent connections; each connection performs the handshake
//! once, then issues a stream of requests (SET/GET mix) one-at-a-time, waiting
//! for each response before sending the next. Per-operation latencies are
//! collected and aggregated into throughput + percentile figures.
//!
//! The wire protocol is re-implemented here on purpose so the benchmark stays a
//! standalone binary that does not depend on the server's internal modules.
//!
//! Usage:
//!   cargo run --release --bin bench -- \
//!       --addr 127.0.0.1:8383 --connections 64 --requests 100000 \
//!       --value-size 64 --keyspace 100000 --read-ratio 0.9

use std::time::Instant;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// Frame headers (must match src/common.rs).
const HDR_HANDSHAKE: u8 = 1;
const HDR_REQUEST: u8 = 3;

// Request ops (must match src/utils.rs).
const OP_GET: u8 = 1;
const OP_SET: u8 = 2;

#[derive(Clone)]
struct Config {
    addr: String,
    connections: usize,
    requests: usize, // total across all connections
    value_size: usize,
    keyspace: u64,
    read_ratio: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8383".to_string(),
            connections: 64,
            requests: 100_000,
            value_size: 64,
            keyspace: 100_000,
            read_ratio: 0.9,
        }
    }
}

fn parse_args() -> Config {
    let mut cfg = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut val = || args.next().expect("missing value for flag");
        match flag.as_str() {
            "--addr" => cfg.addr = val(),
            "--connections" | "-c" => cfg.connections = val().parse().unwrap(),
            "--requests" | "-n" => cfg.requests = val().parse().unwrap(),
            "--value-size" => cfg.value_size = val().parse().unwrap(),
            "--keyspace" => cfg.keyspace = val().parse().unwrap(),
            "--read-ratio" => cfg.read_ratio = val().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "bench --addr <host:port> --connections N --requests N \\\n      --value-size BYTES --keyspace N --read-ratio 0.0..1.0"
                );
                std::process::exit(0);
            }
            other => panic!("unknown flag {other}"),
        }
    }
    cfg
}

/// Tiny deterministic PRNG (xorshift64) — no external deps, fast, good enough
/// to scatter keys across shards.
#[inline]
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn encode_request(buf: &mut Vec<u8>, op: u8, key: &[u8], value: &[u8]) {
    let payload_len = 1 + 2 + key.len() + value.len();
    buf.clear();
    buf.push(HDR_REQUEST);
    buf.extend_from_slice(&(payload_len as u16).to_be_bytes());
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
}

async fn read_response(stream: &mut TcpStream) -> std::io::Result<()> {
    let mut head = [0u8; 3];
    stream.read_exact(&mut head).await?;
    let len = u16::from_be_bytes([head[1], head[2]]) as usize;
    if len > 0 {
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await?;
    }
    Ok(())
}

/// Drives one connection. Returns the latencies (in nanoseconds) it observed.
async fn run_connection(cfg: Config, conn_id: usize, ops: usize) -> Vec<u64> {
    let mut stream = TcpStream::connect(&cfg.addr)
        .await
        .expect("connect failed");
    stream.set_nodelay(true).ok();

    // Handshake (no response expected from the server).
    stream
        .write_all(&[HDR_HANDSHAKE, 0, 0])
        .await
        .expect("handshake write failed");

    let value = vec![b'x'; cfg.value_size];
    let mut buf = Vec::with_capacity(16 + cfg.value_size);
    let mut latencies = Vec::with_capacity(ops);
    // Seed per-connection so connections don't all hit the same keys in lockstep.
    let mut rng = 0x9E3779B97F4A7C15u64 ^ (conn_id as u64).wrapping_mul(0xD1B54A32D192ED03);

    let read_threshold = (cfg.read_ratio * u64::MAX as f64) as u64;

    for _ in 0..ops {
        let key_id = xorshift(&mut rng) % cfg.keyspace;
        let key = key_id.to_be_bytes();
        let is_read = xorshift(&mut rng) < read_threshold;

        if is_read {
            encode_request(&mut buf, OP_GET, &key, &[]);
        } else {
            encode_request(&mut buf, OP_SET, &key, &value);
        }

        let start = Instant::now();
        stream.write_all(&buf).await.expect("write failed");
        read_response(&mut stream).await.expect("read failed");
        latencies.push(start.elapsed().as_nanos() as u64);
    }

    latencies
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx]
}

fn fmt_ns(ns: u64) -> String {
    format!("{:.3} ms", ns as f64 / 1_000_000.0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cfg = parse_args();
    let per_conn = cfg.requests / cfg.connections;
    let total = per_conn * cfg.connections;

    println!(
        "target {} | {} connections | {} ops/conn | {} total | value {}B | read-ratio {:.2} | keyspace {}",
        cfg.addr, cfg.connections, per_conn, total, cfg.value_size, cfg.read_ratio, cfg.keyspace
    );
    println!("warming up + running...");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.connections);
    for conn_id in 0..cfg.connections {
        let cfg = cfg.clone();
        handles.push(tokio::spawn(run_connection(cfg, conn_id, per_conn)));
    }

    let mut all = Vec::with_capacity(total);
    for h in handles {
        all.extend(h.await.expect("connection task panicked"));
    }
    let wall = start.elapsed();

    all.sort_unstable();
    let throughput = total as f64 / wall.as_secs_f64();
    let mean = all.iter().sum::<u64>() as f64 / all.len().max(1) as f64;

    println!("\n=== results ===");
    println!("elapsed     : {:?}", wall);
    println!("operations  : {}", total);
    println!("throughput  : {:.0} ops/sec", throughput);
    println!("latency mean: {}", fmt_ns(mean as u64));
    println!("latency p50 : {}", fmt_ns(percentile(&all, 50.0)));
    println!("latency p90 : {}", fmt_ns(percentile(&all, 90.0)));
    println!("latency p99 : {}", fmt_ns(percentile(&all, 99.0)));
    println!("latency p999: {}", fmt_ns(percentile(&all, 99.9)));
    println!("latency max : {}", fmt_ns(*all.last().unwrap_or(&0)));
}
