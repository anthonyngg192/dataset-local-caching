// Smoke test for the Rust client. Needs the server running on :8383.
//   cargo run --example smoke

use dataset_client::Client;

#[tokio::main]
async fn main() {
    let c = Client::connect("127.0.0.1:8383", 256).await.unwrap();

    println!("set  -> {}", c.set(b"hello", b"world").await);
    println!(
        "get  -> {:?}",
        c.get(b"hello").await.map(|b| String::from_utf8_lossy(&b).into_owned())
    );
    println!("del  -> {}", c.del(b"hello").await);
    println!("miss -> {:?}", c.get(b"hello").await);

    // Concurrency: 5000 requests in flight, matched by req_id out of order.
    const N: u32 = 5000;

    let mut handles = Vec::new();
    for i in 0..N {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            c.set(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .await
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let mut handles = Vec::new();
    for i in 0..N {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            let v = c.get(format!("k{i}").as_bytes()).await;
            v.as_deref() == Some(format!("v{i}").as_bytes())
        }));
    }
    let mut ok = true;
    for h in handles {
        ok &= h.await.unwrap();
    }
    println!("concurrent {N} get, all matched: {ok}");
}
