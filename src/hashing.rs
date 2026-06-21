use wyhash::wyhash;
const ROUTE_SEED: u64 = 123456789;
#[derive(Clone, Debug)]
pub struct HashResult {
    #[allow(dead_code)]
    pub hash: u64,

    pub shard_id: usize,
}

pub fn route_hash(key: &[u8], worker_count: usize) -> HashResult {
    let hash = wyhash(key, ROUTE_SEED);
    HashResult {
        hash,
        shard_id: hash as usize & (worker_count - 1),
    }
}
