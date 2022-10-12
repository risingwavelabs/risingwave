use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{Decode, Encode};

pub type RecordID = u64;

static NEXT_RECORD_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_record_id() -> RecordID {
    NEXT_RECORD_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub struct Record(RecordID, Operation);

impl Record {
    pub(crate) fn new(id: RecordID, op: Operation) -> Self {
        Self(id, op)
    }

    pub(crate) fn id(&self) -> RecordID {
        self.0
    }

    pub(crate) fn op(&self) -> &Operation {
        &self.1
    }
}

#[derive(Encode, Decode, PartialEq, Debug, Clone)]
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

    use crate::next_record_id;

    // test atomic id
    #[tokio::test()]
    async fn atomic_span_id() {
        let mut handles = Vec::new();
        let ids_lock = Arc::new(Mutex::new(HashSet::new()));
        let count: u64 = 100;

        for _ in 0..count {
            let ids = ids_lock.clone();
            handles.push(tokio::spawn(async move {
                let id = next_record_id();
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
