use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{Decode, Encode};

pub type RecordId = u64;

pub(crate) struct RecordIdGenerator {
    record_id: AtomicU64,
}

impl RecordIdGenerator {
    pub(crate) fn new() -> Self {
        Self {
            record_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn next(&self) -> RecordId {
        self.record_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn reset(&self) -> RecordId {
        self.record_id.store(0, Ordering::Relaxed);
        self.record_id.load(Ordering::Relaxed)
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct Record(RecordId, Operation);

impl Record {
    pub(crate) fn new(id: RecordId, op: Operation) -> Self {
        Self(id, op)
    }

    pub(crate) fn id(&self) -> RecordId {
        self.0
    }

    pub(crate) fn op(&self) -> &Operation {
        &self.1
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum Operation {
    Get(Vec<u8>, bool), // options
    Ingest(Vec<(Vec<u8>, Vec<u8>)>),
    Iter(Vec<u8>),
    Sync(u64),
    Seal(u64, bool),
    UpdateVersion(),
    Finish,
}

mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use crate::RecordIdGenerator;

    // test atomic id
    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn atomic_span_id() {
        // reset record id to be 0
        let gen = Arc::new(RecordIdGenerator::new());
        let mut handles = Vec::new();
        let ids_lock = Arc::new(Mutex::new(HashSet::new()));
        let count: u64 = 10;

        for _ in 0..count {
            let ids = ids_lock.clone();
            let gen = gen.clone();
            handles.push(tokio::spawn(async move {
                let id = gen.next();
                ids.lock().insert(id);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let ids = ids_lock.lock();

        for i in 0..count {
            assert_eq!(ids.contains(&i), true);
        }
    }
}
