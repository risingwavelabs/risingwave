// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{Decode, Encode};
use risingwave_common::hm_trace::TraceLocalId;

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
}

#[derive(Encode, Decode, Debug, PartialEq, Eq, Clone)]
pub struct Record(pub TraceLocalId, pub RecordId, pub Operation);

impl Record {
    pub(crate) fn new(local_id: TraceLocalId, record_id: RecordId, op: Operation) -> Self {
        Self(local_id, record_id, op)
    }

    pub(crate) fn new_local_none(record_id: RecordId, op: Operation) -> Self {
        Self::new(TraceLocalId::None, record_id, op)
    }

    pub(crate) fn local_id(&self) -> TraceLocalId {
        self.0
    }

    pub(crate) fn record_id(&self) -> RecordId {
        self.1
    }

    pub(crate) fn op(&self) -> &Operation {
        &self.2
    }
}

/// Operations represents Hummock operations
#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum Operation {
    /// Get operation of Hummock.
    /// (key, check_bloom_filter, epoch, table_id, retention_seconds)
    Get(Vec<u8>, bool, u64, u32, Option<u32>),

    /// Ingest operation of Hummock.
    /// (kv_pairs, epoch, table_id)
    Ingest(Vec<(Vec<u8>, Option<Vec<u8>>)>, u64, u32),

    /// Iter operation of Hummock
    /// (prefix_hint, left_bound, right_bound,epoch, table_id, retention_seconds)
    Iter(
        Option<Vec<u8>>,
        Bound<Vec<u8>>,
        Bound<Vec<u8>>,
        u64,
        u32,
        Option<u32>,
    ),

    /// Iter.next operation
    /// (record_id, kv_pair)
    IterNext(RecordId, Option<(Vec<u8>, Vec<u8>)>),

    /// Sync operation
    /// (epoch)
    Sync(u64),

    /// Seal operation
    /// (epoch, is_checkpoint)
    Seal(u64, bool),

    /// Update local_version
    UpdateVersion(),

    /// The end of an operation
    Finish,
}

#[cfg(test)]
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
            assert!(ids.contains(&i));
        }
    }
}
