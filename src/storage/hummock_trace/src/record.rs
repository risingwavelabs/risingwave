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

use bincode::{BorrowDecode, Decode, Encode};
use risingwave_common::hm_trace::TraceLocalId;
use risingwave_pb::meta::SubscribeResponse;

use crate::StorageType;

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

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub struct Record(
    pub StorageType,
    #[bincode(with_serde)] pub TraceLocalId,
    pub RecordId,
    pub Operation,
);

impl Record {
    pub(crate) fn new(
        storage_type: StorageType,
        local_id: TraceLocalId,
        record_id: RecordId,
        op: Operation,
    ) -> Self {
        Self(storage_type, local_id, record_id, op)
    }

    pub fn storage_type(&self) -> StorageType {
        self.0
    }

    pub fn local_id(&self) -> TraceLocalId {
        self.1
    }

    pub fn record_id(&self) -> RecordId {
        self.2
    }

    pub fn op(&self) -> &Operation {
        &self.3
    }

    #[cfg(test)]
    pub(crate) fn new_local_none(record_id: RecordId, op: Operation) -> Self {
        Self::new(StorageType::Global, TraceLocalId::None, record_id, op)
    }
}

type TraceKey = Vec<u8>;
type TraceValue = Vec<u8>;
type TableId = u32;
/// Operations represents Hummock operations
#[derive(Encode, Decode, PartialEq, Debug, Clone)]
pub enum Operation {
    /// Get operation of Hummock.
    /// (key, check_bloom_filter, epoch, table_id, retention_seconds)
    // Get(TraceKey, bool, u64, u32, Option<u32>),
    Get {
        key: TraceKey,
        epoch: u64,
        prefix_hint: Option<TraceKey>,
        check_bloom_filter: bool,
        retention_seconds: Option<u32>,
        table_id: TableId,
    },

    /// Ingest operation of Hummock.
    /// (kv_pairs, epoch, table_id)
    // Ingest(Vec<(TraceKey, Option<TraceValue>)>, u64, u32),
    Ingest {
        kv_pairs: Vec<(TraceKey, Option<TraceValue>)>,
        delete_ranges: Vec<(TraceKey, TraceKey)>,
        epoch: u64,
        table_id: TableId,
    },

    /// Iter operation of Hummock
    /// (prefix_hint, left_bound, right_bound, epoch, table_id, retention_seconds)
    Iter {
        key_range: (Bound<TraceKey>, Bound<TraceValue>),
        epoch: u64,
        prefix_hint: Option<TraceKey>,
        check_bloom_filter: bool,
        retention_seconds: Option<u32>,
        table_id: TableId,
    },

    /// Iter.next operation
    /// (record_id, kv_pair)
    IterNext(RecordId),

    /// Sync operation
    /// (epoch)
    Sync(u64),

    /// Seal operation
    /// (epoch, is_checkpoint)
    Seal(u64, bool),

    /// Update local_version
    UpdateVersion(u64),

    /// The end of an operation
    Finish,

    MetaMessage(Box<TraceSubResp>),

    Result(OperationResult),
}

impl Operation {
    pub fn get(
        key: TraceKey,
        epoch: u64,
        prefix_hint: Option<TraceKey>,
        check_bloom_filter: bool,
        retention_seconds: Option<u32>,
        table_id: TableId,
    ) -> Operation {
        Operation::Get {
            key,
            epoch,
            prefix_hint,
            check_bloom_filter,
            retention_seconds,
            table_id,
        }
    }

    pub fn ingest(
        kv_pairs: Vec<(TraceKey, Option<TraceValue>)>,
        delete_ranges: Vec<(TraceKey, TraceKey)>,
        epoch: u64,
        table_id: TableId,
    ) -> Operation {
        Operation::Ingest {
            kv_pairs,
            delete_ranges,
            epoch,
            table_id,
        }
    }
}

/// `TraceResult` discards Error and only marks whether succeeded or not.
/// Use Option rather than Result because it's overhead to serialize Error.
type TraceResult<T> = Option<T>;

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum OperationResult {
    Get(TraceResult<Option<TraceValue>>),
    Ingest(TraceResult<usize>),
    Iter(TraceResult<()>),
    IterNext(TraceResult<(TraceKey, TraceValue)>),
    Sync(TraceResult<usize>),
    Seal(TraceResult<()>),
    NotifyHummock(TraceResult<()>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct TraceSubResp(pub SubscribeResponse);

impl Encode for TraceSubResp {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // SubscribeResponse and its implementation of Serialize is generated
        // by prost and pbjson for ProtoBuf mapping.
        // Serialization methods like Bincode may not correctly serialize it.
        let encoded = ron::to_string(&self.0).unwrap();
        Encode::encode(&encoded, encoder)
    }
}

impl Decode for TraceSubResp {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let s: String = Decode::decode(decoder)?;
        let resp: SubscribeResponse = ron::from_str(&s).unwrap();
        Ok(Self(resp))
    }
}

impl<'de> bincode::BorrowDecode<'de> for TraceSubResp {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let s: String = BorrowDecode::borrow_decode(decoder)?;
        let resp: SubscribeResponse = ron::from_str(&s).unwrap();
        Ok(Self(resp))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    // test atomic id
    #[tokio::test(flavor = "multi_thread")]
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
