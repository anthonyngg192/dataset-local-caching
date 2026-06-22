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
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf},
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
    pipeline: usize, // requests sent before reading their responses (1 = closed-loop)
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
            pipeline: 1,
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
            "--pipeline" | "-p" => cfg.pipeline = val().parse::<usize>().unwrap().max(1),
            "-h" | "--help" => {
                eprintln!(
                    "bench --addr <host:port> --connections N --requests N \\\n      --value-size BYTES --keyspace N --read-ratio 0.0..1.0 --pipeline N"
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

/// Append one encoded request frame to `buf` (does not clear it).
/// v2 envelope: [header:u8][req_id:u32 BE][len:u16 BE][payload].
fn encode_request(buf: &mut Vec<u8>, req_id: u32, op: u8, key: &[u8], value: &[u8]) {
    let payload_len = 1 + 2 + key.len() + value.len();
    buf.push(HDR_REQUEST);
    buf.extend_from_slice(&req_id.to_be_bytes());
    buf.extend_from_slice(&(payload_len as u16).to_be_bytes());
    buf.push(op);
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
}

/// Read one response frame, returning its req_id. The body is drained but not
/// inspected (throughput test).
async fn read_response(reader: &mut BufReader<OwnedReadHalf>) -> std::io::Result<u32> {
    let mut head = [0u8; 7];
    reader.read_exact(&mut head).await?;
    let req_id = u32::from_be_bytes([head[1], head[2], head[3], head[4]]);
    let len = u16::from_be_bytes([head[5], head[6]]) as usize;
    if len > 0 {
        let mut body = vec![0u8; len];
        reader.read_exact(&mut body).await?;
    }
    Ok(req_id)
}

/// Drives one connection. Returns the latencies (in nanoseconds) it observed.
async fn run_connection(cfg: Config, conn_id: usize, ops: usize) -> Vec<u64> {
    let stream = TcpStream::connect(&cfg.addr)
        .await
        .expect("connect failed");
    stream.set_nodelay(true).ok();

    // Split so we can buffer the read side: reading responses byte-frame by
    // byte-frame off the raw socket costs one syscall per `read_exact`; a
    // BufReader drains many responses per syscall, which matters a lot when a
    // single batch returns hundreds of small frames.
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Handshake (no response expected). v2 envelope: header + req_id + len.
    write_half
        .write_all(&[HDR_HANDSHAKE, 0, 0, 0, 0, 0, 0])
        .await
        .expect("handshake write failed");

    let value = vec![b'x'; cfg.value_size];
    let depth = cfg.pipeline.max(1);
    let mut buf = Vec::with_capacity(depth * (16 + cfg.value_size));
    let mut latencies = Vec::with_capacity(ops.div_ceil(depth));
    // Seed per-connection so connections don't all hit the same keys in lockstep.
    let mut rng = 0x9E3779B97F4A7C15u64 ^ (conn_id as u64).wrapping_mul(0xD1B54A32D192ED03);

    let read_threshold = (cfg.read_ratio * u64::MAX as f64) as u64;
    // Monotonic per-connection req_id. With `depth` outstanding at a time, this
    // acts like a slot window; the server echoes it and we drain by count.
    let mut req_id: u32 = 0;

    // Send `depth` requests, then read all `depth` responses. With depth == 1
    // this is the closed-loop case; larger depths amortize the round-trip and
    // expose the server's real ceiling. Each sample is the round-trip time of a
    // whole batch.
    let mut sent = 0;
    while sent < ops {
        let batch = depth.min(ops - sent);
        buf.clear();
        for _ in 0..batch {
            let key = (xorshift(&mut rng) % cfg.keyspace).to_be_bytes();
            req_id = req_id.wrapping_add(1);
            if xorshift(&mut rng) < read_threshold {
                encode_request(&mut buf, req_id, OP_GET, &key, &[]);
            } else {
                encode_request(&mut buf, req_id, OP_SET, &key, &value);
            }
        }

        let start = Instant::now();
        write_half.write_all(&buf).await.expect("write failed");
        for _ in 0..batch {
            read_response(&mut reader).await.expect("read failed");
        }
        latencies.push(start.elapsed().as_nanos() as u64);

        sent += batch;
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
        "target {} | {} connections | {} ops/conn | {} total | value {}B | read-ratio {:.2} | keyspace {} | pipeline {}",
        cfg.addr, cfg.connections, per_conn, total, cfg.value_size, cfg.read_ratio, cfg.keyspace, cfg.pipeline
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
    // With pipelining each latency sample is a batch round-trip, not a single op.
    let lat_unit = if cfg.pipeline > 1 {
        format!("per-batch of {}", cfg.pipeline)
    } else {
        "per-op".to_string()
    };

    println!("\n=== results ===");
    println!("elapsed     : {:?}", wall);
    println!("operations  : {}", total);
    println!("throughput  : {:.0} ops/sec", throughput);
    println!("latency ({lat_unit}):");
    println!("latency mean: {}", fmt_ns(mean as u64));
    println!("latency p50 : {}", fmt_ns(percentile(&all, 50.0)));
    println!("latency p90 : {}", fmt_ns(percentile(&all, 90.0)));
    println!("latency p99 : {}", fmt_ns(percentile(&all, 99.0)));
    println!("latency p999: {}", fmt_ns(percentile(&all, 99.9)));
    println!("latency max : {}", fmt_ns(*all.last().unwrap_or(&0)));
}
