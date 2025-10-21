// Copyright 2025 RisingWave Labs
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

use std::sync::LazyLock;

use indexmap::IndexMap;
use indexmap::map::Entry;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk, StreamChunkBuilder};
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;

use crate::consistency::consistency_panic;

/// Behavior when inconsistency is detected when aggregating changes to [`ChangeBuffer`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InconsistencyBehavior {
    Panic,
    Warn,
    Tolerate,
}

impl InconsistencyBehavior {
    /// Report an inconsistency.
    #[track_caller]
    pub fn report(self, msg: &str) {
        match self {
            InconsistencyBehavior::Panic => consistency_panic!("{}", msg),
            InconsistencyBehavior::Warn => {
                static LOG_SUPPERSSER: LazyLock<LogSuppresser> =
                    LazyLock::new(LogSuppresser::default);

                if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                    tracing::warn!(suppressed_count, "{}", msg);
                }
            }
            InconsistencyBehavior::Tolerate => {}
        }
    }
}

mod private {
    pub trait Key: Eq + std::hash::Hash {}
    impl<K> Key for K where K: Eq + std::hash::Hash {}

    pub trait Row: Default {}
    impl<R> Row for R where R: Default {}
}

/// A buffer that accumulates changes and produce compacted changes.
#[derive(Debug)]
pub struct ChangeBuffer<K, R> {
    // We use an `IndexMap` to preserve the original order of the changes as much as possible.
    buffer: IndexMap<K, Record<R>>,
    ib: InconsistencyBehavior,
}

impl<K, R> ChangeBuffer<K, R>
where
    K: private::Key,
    R: private::Row,
{
    /// Apply an insertion of a row with the given key.
    pub fn insert(&mut self, key: K, new_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Record::Insert { new_row });
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                Record::Delete { old_row } => {
                    let old_row = std::mem::take(old_row);
                    e.insert(Record::Update { old_row, new_row });
                }
                Record::Insert { new_row: dst } => {
                    self.ib.report("inconsistent changes: double-inserting");
                    *dst = new_row;
                }
                Record::Update { new_row: dst, .. } => {
                    self.ib.report("inconsistent changes: double-inserting");
                    *dst = new_row;
                }
            },
        }
    }

    /// Apply a deletion of a row with the given key.
    pub fn delete(&mut self, key: K, old_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Record::Delete { old_row });
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                Record::Insert { .. } => {
                    // FIXME: though preserving the order well,
                    // this is not performant compared to `swap_remove`
                    e.shift_remove();
                }
                Record::Update { old_row, .. } => {
                    let old_row = std::mem::take(old_row);
                    e.insert(Record::Delete { old_row });
                }
                Record::Delete { old_row: dst } => {
                    self.ib.report("inconsistent changes: double-deleting");
                    *dst = old_row;
                }
            },
        }
    }

    /// Apply an update of a row with the given key.
    pub fn update(&mut self, key: K, old_row: R, new_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Record::Update { old_row, new_row });
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                Record::Insert { .. } => {
                    e.insert(Record::Insert { new_row });
                }
                Record::Update { new_row: dst, .. } => {
                    *dst = new_row;
                }
                Record::Delete { .. } => {
                    self.ib.report("inconsistent changes: update after delete");
                    e.insert(Record::Update { old_row, new_row });
                }
            },
        }
    }

    /// Apply a change record, with the key extracted by the given function.
    ///
    /// For `Record::Update`, inconsistency is reported if the old key and the new key are different.
    /// Further behavior is determined by the `InconsistencyBehavior`.
    pub fn apply_record(&mut self, record: Record<R>, key_fn: impl Fn(&R) -> K) {
        match record {
            Record::Insert { new_row } => self.insert(key_fn(&new_row), new_row),
            Record::Delete { old_row } => self.delete(key_fn(&old_row), old_row),
            Record::Update { old_row, new_row } => {
                let old_key = key_fn(&old_row);
                let new_key = key_fn(&new_row);

                // As long as `ib` is not `Panic`, we still gracefully handle the mismatched key.
                if old_key != new_key {
                    self.ib
                        .report("inconsistent changes: mismatched key in update");
                    self.delete(old_key, old_row);
                    self.insert(new_key, new_row);
                } else {
                    self.update(old_key, old_row, new_row);
                }
            }
        }
    }

    /// Apply an `Op` of a row with the given key.
    pub fn apply_op_row(&mut self, op: Op, key: K, row: R) {
        match op {
            Op::Insert | Op::UpdateInsert => self.insert(key, row),
            Op::Delete | Op::UpdateDelete => self.delete(key, row),
        }
    }
}

impl<K, R> ChangeBuffer<K, R> {
    /// Create a new `ChangeBuffer` that panics on inconsistency.
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new `ChangeBuffer` with the given capacity that panics on inconsistency.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: IndexMap::with_capacity(capacity),
            ib: InconsistencyBehavior::Panic,
        }
    }

    /// Set the inconsistency behavior.
    pub fn with_inconsistency_behavior(mut self, ib: InconsistencyBehavior) -> Self {
        self.ib = ib;
        self
    }

    /// Get the number of keys that have pending changes in the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Consume the buffer and produce a list of change records.
    pub fn into_records(self) -> impl ExactSizeIterator<Item = Record<R>> {
        self.buffer.into_values()
    }
}

impl<K, R: Row> ChangeBuffer<K, R> {
    /// Consume the buffer and produce a single compacted chunk.
    pub fn into_chunk(self, data_types: Vec<DataType>) -> Option<StreamChunk> {
        let mut builder = StreamChunkBuilder::unlimited(data_types, Some(self.buffer.len()));
        for record in self.into_records() {
            let none = builder.append_record_eliminate_noop_update(record);
            debug_assert!(none.is_none());
        }
        builder.take()
    }

    /// Consume the buffer and produce a list of compacted chunks with the given size at most.
    pub fn into_chunks(self, data_types: Vec<DataType>, chunk_size: usize) -> Vec<StreamChunk> {
        let mut res = Vec::new();
        let mut builder = StreamChunkBuilder::new(chunk_size, data_types);
        for record in self.into_records() {
            if let Some(chunk) = builder.append_record_eliminate_noop_update(record) {
                res.push(chunk);
            }
        }
        res.extend(builder.take());
        res
    }
}
