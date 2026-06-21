mod hashing;
mod shards;
mod common;
pub mod utils;
pub mod server;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("Hello, world!");
}
