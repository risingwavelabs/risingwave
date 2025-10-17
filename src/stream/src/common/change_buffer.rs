use std::sync::LazyLock;

use indexmap::IndexMap;
use indexmap::map::Entry;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{StreamChunk, StreamChunkBuilder};
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;

use crate::consistency::consistency_panic;

/// Behavior when inconsistency is detected during compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum InconsistencyBehavior {
    #[default]
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

#[derive(Debug)]
pub struct ChangeBuffer<K, R> {
    buffer: IndexMap<K, Record<R>>,
    ib: InconsistencyBehavior,
}

impl<K, R> Default for ChangeBuffer<K, R> {
    fn default() -> Self {
        Self {
            buffer: IndexMap::new(),
            ib: InconsistencyBehavior::default(),
        }
    }
}

impl<K, R> ChangeBuffer<K, R>
where
    K: private::Key,
    R: private::Row,
{
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

    pub fn delete(&mut self, key: K, old_row: R) {
        let entry = self.buffer.entry(key);
        match entry {
            Entry::Vacant(e) => {
                e.insert(Record::Delete { old_row });
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                Record::Insert { .. } => {
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
}

impl<K, R> ChangeBuffer<K, R> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_inconsistency_behavior(mut self, ib: InconsistencyBehavior) -> Self {
        self.ib = ib;
        self
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn into_records(self) -> impl ExactSizeIterator<Item = Record<R>> {
        self.buffer.into_values()
    }
}

impl<K, R: Row> ChangeBuffer<K, R> {
    pub fn into_chunk(self, data_types: Vec<DataType>) -> Option<StreamChunk> {
        let mut builder = StreamChunkBuilder::unlimited(data_types, Some(self.buffer.len()));
        for record in self.into_records() {
            let none = builder.append_record_eliminate_noop_update(record);
            debug_assert!(none.is_none());
        }
        builder.take()
    }
}
