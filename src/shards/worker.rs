use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use lru::LruCache;

use crate::shards::disk::{Disk, IndexRecord};

/// Estimated fixed per-cache-entry overhead. Measured ~128 B.
const ENTRY_OVERHEAD: usize = 130;

/// Where a key's value lives on disk, plus its expiry (epoch ms, 0 = none).
#[derive(Clone, Copy)]
struct Loc {
    val_off: u64,
    vlen: u32,
    exp_ms: u64,
}

/// Per-shard tiered store:
/// - `index`  — every key → on-disk location (always in RAM; bounds key count).
/// - `cache`  — hot values only, byte-bounded LRU; a miss reads from `cool`.
/// - `expiries` — TTL min-heap keyed by epoch ms.
/// Single-threaded per worker → lock-free.
pub struct Worker {
    index: HashMap<Bytes, Loc>,
    cache: LruCache<Bytes, Bytes>,
    cache_bytes: usize,
    budget: usize, // cache byte cap; 0 = unbounded (cache everything)
    expiries: BinaryHeap<Reverse<(u64, Bytes)>>,
    disk: Disk,
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn cache_size(key_len: usize, value_len: usize) -> usize {
    key_len + value_len + ENTRY_OVERHEAD
}

impl Worker {
    /// Open the shard's files and rebuild the index by replaying the backup log.
    /// Values are loaded lazily (on first access), not at startup.
    pub fn start(budget: usize, dir: &Path) -> Self {
        let (disk, records) = Disk::open(dir).expect("open shard data files");
        let mut w = Self {
            index: HashMap::new(),
            cache: LruCache::unbounded(),
            cache_bytes: 0,
            budget,
            expiries: BinaryHeap::new(),
            disk,
        };

        let now = now_epoch_ms();
        for rec in records {
            match rec {
                IndexRecord::Set {
                    key,
                    val_off,
                    vlen,
                    exp_ms,
                } => {
                    if exp_ms != 0 && exp_ms <= now {
                        continue; // already expired — skip
                    }
                    if exp_ms != 0 {
                        w.expiries.push(Reverse((exp_ms, key.clone())));
                    }
                    w.index.insert(
                        key,
                        Loc {
                            val_off,
                            vlen,
                            exp_ms,
                        },
                    );
                }
                IndexRecord::Del { key } => {
                    w.index.remove(&key);
                }
            }
        }
        w
    }

    // --- cache bookkeeping ---

    fn cache_insert(&mut self, k: Bytes, v: Bytes) {
        let klen = k.len();
        let new_sz = cache_size(klen, v.len());
        match self.cache.put(k, v) {
            Some(old) => {
                self.cache_bytes =
                    self.cache_bytes.saturating_sub(cache_size(klen, old.len())) + new_sz;
            }
            None => self.cache_bytes += new_sz,
        }
    }

    fn cache_remove(&mut self, k: &[u8]) {
        if let Some(v) = self.cache.pop(k) {
            self.cache_bytes = self.cache_bytes.saturating_sub(cache_size(k.len(), v.len()));
        }
    }

    // --- public ops ---

    pub fn set(&mut self, k: Bytes, v: Bytes) {
        self.write(k, v, 0);
    }

    pub fn set_ex(&mut self, k: Bytes, v: Bytes, ttl: Duration) {
        let exp_ms = now_epoch_ms() + ttl.as_millis() as u64;
        self.write(k, v, exp_ms);
    }

    fn write(&mut self, k: Bytes, v: Bytes, exp_ms: u64) {
        let val_off = match self.disk.append_set(&k, &v, exp_ms) {
            Ok(off) => off,
            Err(e) => {
                tracing::error!("cool append failed: {e}");
                return;
            }
        };
        self.index.insert(
            k.clone(),
            Loc {
                val_off,
                vlen: v.len() as u32,
                exp_ms,
            },
        );
        if exp_ms != 0 {
            self.expiries.push(Reverse((exp_ms, k.clone())));
        }
        self.cache_insert(k, v); // newest = hot
        self.evict_to_budget();
    }

    pub fn delete(&mut self, k: &[u8]) -> bool {
        let existed = self.index.remove(k).is_some();
        self.cache_remove(k);
        if existed {
            if let Err(e) = self.disk.append_del(k) {
                tracing::error!("cool append (del) failed: {e}");
            }
        }
        existed
    }

    pub fn get(&mut self, k: &[u8]) -> Option<Bytes> {
        let loc = *self.index.get(k)?;

        // Lazy expiry (epoch). Expired records stay in the cool log with a past
        // exp_ms and are skipped on replay, so no disk delete is needed.
        if loc.exp_ms != 0 && loc.exp_ms <= now_epoch_ms() {
            self.index.remove(k);
            self.cache_remove(k);
            return None;
        }

        // Hot path: value in the RAM cache (promotes to MRU).
        if let Some(v) = self.cache.get(k) {
            return Some(v.clone());
        }

        // Cold path: read from cool, then cache it.
        match self.disk.read_at(loc.val_off, loc.vlen) {
            Ok(v) => {
                self.cache_insert(Bytes::copy_from_slice(k), v.clone());
                self.evict_to_budget();
                Some(v)
            }
            Err(e) => {
                tracing::error!("cold read failed: {e}");
                None
            }
        }
    }

    /// Active TTL sweep, bounded. Validates against the index's live expiry.
    pub fn expire_due(&mut self, budget: usize) {
        let now = now_epoch_ms();
        for _ in 0..budget {
            match self.expiries.peek() {
                Some(Reverse((t, _))) if *t <= now => {}
                _ => break,
            }
            let Reverse((t, key)) = self.expiries.pop().unwrap();
            if self.index.get(&key).is_some_and(|loc| loc.exp_ms == t) {
                self.index.remove(&key);
                self.cache_remove(&key);
            }
        }
    }

    /// LRU eviction of the hot **cache** at 90% high-water down to 80%. Lossless:
    /// the value stays in `cool` and the index keeps pointing at it, so a later
    /// GET reads it back from disk. (The `index` itself is never evicted.)
    pub fn evict_to_budget(&mut self) {
        if self.budget == 0 {
            return;
        }
        let high = self.budget / 10 * 9;
        let low = self.budget / 10 * 8;
        if self.cache_bytes < high {
            return;
        }
        while self.cache_bytes > low {
            match self.cache.pop_lru() {
                Some((k, v)) => {
                    self.cache_bytes =
                        self.cache_bytes.saturating_sub(cache_size(k.len(), v.len()));
                }
                None => break,
            }
        }
    }

    pub fn flush_os(&mut self) {
        if let Err(e) = self.disk.flush() {
            tracing::error!("disk flush failed: {e}");
        }
    }

    pub fn fsync(&mut self) {
        if let Err(e) = self.disk.fsync() {
            tracing::error!("disk fsync failed: {e}");
        }
    }
}
