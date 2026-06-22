// Interactive REPL for dataset-local — a small redis-cli-style shell.
//
//   cargo run --bin dataset-cli -- [--addr host:port] [--user U] [--pass P]
//
// Auth falls back to DATASET_USERNAME / DATASET_PASSWORD env vars.
// Commands: get <key> | set <key> <value...> | del <key> | help | quit

use dataset_client::Client;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() {
    let mut addr = "127.0.0.1:8383".to_string();
    let mut user = std::env::var("DATASET_USERNAME").unwrap_or_default();
    let mut pass = std::env::var("DATASET_PASSWORD").unwrap_or_default();

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--addr" => addr = args.next().unwrap_or(addr),
            "--user" => user = args.next().unwrap_or_default(),
            "--pass" => pass = args.next().unwrap_or_default(),
            "-h" | "--help" => {
                eprintln!("usage: dataset-cli [--addr host:port] [--user U] [--pass P]");
                return;
            }
            other => addr = other.to_string(), // bare address
        }
    }

    let client = match Client::connect(&addr, 64, &user, &pass).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect to {addr} failed: {e}");
            std::process::exit(1);
        }
    };
    println!("connected to {addr}  (commands: get / set / del / help / quit)");

    let mut stdout = tokio::io::stdout();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    loop {
        let _ = stdout.write_all(b"dataset> ").await;
        let _ = stdout.flush().await;

        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            _ => break, // EOF (Ctrl-D) or read error
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let (cmd, rest) = match line.split_once(' ') {
            Some((c, r)) => (c, r.trim()),
            None => (line, ""),
        };

        match cmd.to_ascii_lowercase().as_str() {
            "get" if !rest.is_empty() => match client.get(rest.as_bytes()).await {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(nil)"),
            },
            "set" if !rest.is_empty() => {
                let (key, value) = rest.split_once(' ').unwrap_or((rest, ""));
                let ok = client.set(key.as_bytes(), value.as_bytes()).await;
                println!("{}", if ok { "OK" } else { "ERR" });
            }
            "setex" if !rest.is_empty() => {
                // setex <key> <ttl_ms> <value...>
                let mut parts = rest.splitn(3, ' ');
                match (
                    parts.next(),
                    parts.next().and_then(|s| s.parse::<u32>().ok()),
                ) {
                    (Some(key), Some(ttl_ms)) => {
                        let value = parts.next().unwrap_or("");
                        let ok = client.setex(key.as_bytes(), value.as_bytes(), ttl_ms).await;
                        println!("{}", if ok { "OK" } else { "ERR" });
                    }
                    _ => eprintln!("ERR usage: setex <key> <ttl_ms> <value...>"),
                }
            }
            "del" if !rest.is_empty() => {
                let n = if client.del(rest.as_bytes()).await { 1 } else { 0 };
                println!("(integer) {n}");
            }
            "get" | "set" | "del" => eprintln!("ERR usage: {cmd} <key> ..."),
            "help" => println!(
                "get <key> | set <key> <value...> | setex <key> <ttl_ms> <value...> | del <key> | quit"
            ),
            "quit" | "exit" => break,
            other => eprintln!("ERR unknown command '{other}'"),
        }
    }

    println!();
}
