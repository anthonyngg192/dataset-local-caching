use std::collections::HashMap;

use bytes::Bytes;

pub struct Worker {
    store: HashMap<Vec<u8>, Bytes>,
}

impl Worker {
    pub fn start() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, k: Vec<u8>, v: Bytes) -> bool {
        let result = self.store.insert(k, v);
        match result {
            Some(_) => true,
            None => false,
        }
    }

    pub fn get(&self, k: &Vec<u8>) -> Option<Bytes> {
        self.store.get(k).cloned()
    }

    pub fn delete(&mut self, k: &Vec<u8>) -> bool {
        match self.store.remove(k) {
            Some(_) => true,
            None => false,
        }
    }
}
