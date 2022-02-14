use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct WorkerIdRef(Arc<AtomicU32>);

impl WorkerIdRef {
    const INVALID_WORKER_ID: u32 = u32::MAX;

    pub fn new() -> Self {
        Self(Arc::new(Self::INVALID_WORKER_ID.into()))
    }

    pub fn for_test() -> Self {
        Self(Arc::new(233.into()))
    }

    pub fn get(&self) -> u32 {
        let worker_id = self.0.load(Ordering::SeqCst);
        if worker_id == Self::INVALID_WORKER_ID {
            panic!("invalid worker id, maybe not have registered with meta service");
        }
        worker_id
    }

    pub fn set(&self, worker_id: u32) {
        self.0.store(worker_id, Ordering::SeqCst);
    }
}
