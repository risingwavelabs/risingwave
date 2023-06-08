// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::util::worker_util::WorkerNodeId;

const WORKER_ID_SHIFT_BITS: u8 = 32;

/// `TxnIdGenerator` generates unique transaction ids as following format:
///
/// | worker id | sequence |
/// |-----------|----------|
/// |  32 bits  | 32 bits  |
///
/// Currently, we just need to guarantee the transaction ids are unique at runtime.
/// It doesn't matter, even if the sequence starts from zero after recovery.
#[derive(Debug)]
pub struct TxnIdGenerator {
    worker_id: u32,
    sequence: AtomicU32,
}

pub type TxnId = u64;

impl TxnIdGenerator {
    pub fn new(worker_id: WorkerNodeId) -> Self {
        Self {
            worker_id,
            sequence: AtomicU32::new(0),
        }
    }

    pub fn gen_txn_id(&self) -> TxnId {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        (self.worker_id as u64) << WORKER_ID_SHIFT_BITS | sequence as u64
    }
}
