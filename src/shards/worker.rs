use std::collections::HashMap;

use bytes::Bytes;

pub struct Worker {
    // Keys are `Bytes` (ref-counted), so SET moves the key in with no byte copy.
    // Lookups go through `Bytes: Borrow<[u8]>`, so GET/DEL match by slice without
    // allocating a key at all.
    store: HashMap<Bytes, Bytes>,
}

impl Worker {
    pub fn start() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, k: Bytes, v: Bytes) -> bool {
        self.store.insert(k, v).is_some()
    }

    pub fn get(&self, k: &[u8]) -> Option<Bytes> {
        self.store.get(k).cloned()
    }

    pub fn delete(&mut self, k: &[u8]) -> bool {
        self.store.remove(k).is_some()
    }
}
