use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use bytes::Bytes;

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
}

pub struct Worker {
    // Keys are `Bytes` (ref-counted): SET moves the key in with no byte copy,
    // GET/DEL look up by slice via `Bytes: Borrow<[u8]>`.
    store: HashMap<Bytes, Entry>,
    // Min-heap of `(expiry, key)` driving active expiration. May hold stale
    // entries (after a key is overwritten / re-TTL'd / deleted); each pop is
    // validated against the entry's current `expires_at`, which is the source of
    // truth. The heap only ever surfaces keys at/after their deadline, so active
    // cleanup never scans the whole shard.
    expiries: BinaryHeap<Reverse<(Instant, Bytes)>>,
}

impl Worker {
    pub fn start() -> Self {
        Self {
            store: HashMap::new(),
            expiries: BinaryHeap::new(),
        }
    }

    /// Plain SET: overwrites the entry and clears any TTL (persistent key).
    pub fn set(&mut self, k: Bytes, v: Bytes) -> bool {
        self.store
            .insert(
                k,
                Entry {
                    value: v,
                    expires_at: None,
                },
            )
            .is_some()
    }

    /// SET with a time-to-live. Overwriting an expired-but-uncollected key is
    /// just a normal overwrite — the new entry is fresh with the new deadline.
    pub fn set_ex(&mut self, k: Bytes, v: Bytes, ttl: Duration) -> bool {
        let expires_at = Instant::now() + ttl;
        self.expiries.push(Reverse((expires_at, k.clone())));
        self.store
            .insert(
                k,
                Entry {
                    value: v,
                    expires_at: Some(expires_at),
                },
            )
            .is_some()
    }

    /// Lazy expiration: an expired key reads as a miss and is dropped on the spot,
    /// so clients never see a logically-expired value regardless of when the
    /// active sweep runs.
    pub fn get(&mut self, k: &[u8]) -> Option<Bytes> {
        let (value, expires_at) = match self.store.get(k) {
            Some(e) => (e.value.clone(), e.expires_at),
            None => return None,
        };
        if let Some(exp) = expires_at {
            if exp <= Instant::now() {
                self.store.remove(k);
                return None;
            }
        }
        Some(value)
    }

    pub fn delete(&mut self, k: &[u8]) -> bool {
        self.store.remove(k).is_some()
    }

    /// Drop keys whose deadline has passed, up to `budget` per call so it never
    /// stalls command processing. Stops as soon as the soonest entry isn't due.
    pub fn expire_due(&mut self, budget: usize) {
        let now = Instant::now();
        for _ in 0..budget {
            match self.expiries.peek() {
                Some(Reverse((t, _))) if *t <= now => {}
                _ => break, // heap empty, or next deadline is in the future
            }
            let Reverse((t, key)) = self.expiries.pop().unwrap();
            // Only delete if this heap record matches the key's live deadline.
            // A different (or absent) `expires_at` means it was re-set/removed —
            // a stale record to discard.
            if let Some(e) = self.store.get(&key) {
                if e.expires_at == Some(t) {
                    self.store.remove(&key);
                }
            }
        }
    }
}
