//! Aggregators with state store support

mod value;

use std::collections::btree_map;

use risingwave_common::expr::AggKind;
pub use value::*;
mod extreme;
mod extreme_serializer;
mod ordered_serializer;
mod string_agg;

use bytes::Bytes;
pub use extreme::*;
pub use ordered_serializer::*;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_storage::{Keyspace, StateStore};

use super::AggCall;

/// Represents an entry in the `flush_buffer`. No `FlushStatus` associated with a key means no-op.
///
/// ```plain
/// No-op --(insert)-> Insert --(delete)-> No-op
///  \------(delete)-> Delete --(insert)-> DeleteInsert --(delete)-> Delete
/// ```
enum FlushStatus {
    /// The entry will be deleted.
    Delete,
    /// The entry has been deleted in this epoch, and will be inserted.
    DeleteInsert(ScalarImpl),
    /// The entry will be inserted.
    Insert(ScalarImpl),
}

impl FlushStatus {
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete)
    }

    pub fn is_insert(&self) -> bool {
        matches!(self, Self::Insert(_))
    }

    pub fn is_delete_insert(&self) -> bool {
        matches!(self, Self::DeleteInsert(_))
    }

    /// Transform `FlushStatus` into an `Option`. If the last operation in the `FlushStatus` is
    /// `Delete`, return `None`. Otherwise, return the concrete value.
    pub fn into_option(self) -> Option<ScalarImpl> {
        match self {
            Self::DeleteInsert(value) | Self::Insert(value) => Some(value),
            Self::Delete => None,
        }
    }

    pub fn as_option(&self) -> Option<&ScalarImpl> {
        match self {
            Self::DeleteInsert(value) | Self::Insert(value) => Some(value),
            Self::Delete => None,
        }
    }

    /// Insert an entry and modify the corresponding flush state
    pub fn do_insert<K: Ord>(entry: btree_map::Entry<K, Self>, value: ScalarImpl) {
        match entry {
            btree_map::Entry::Vacant(e) => {
                // No-op -> Insert
                e.insert(Self::Insert(value));
            }
            btree_map::Entry::Occupied(mut e) => {
                if e.get().is_delete() {
                    // Delete -> DeleteInsert
                    e.insert(Self::DeleteInsert(value));
                } else {
                    panic!("invalid flush status");
                }
            }
        }
    }

    /// Delete an entry and modify the corresponding flush state
    pub fn do_delete<K: Ord>(entry: btree_map::Entry<K, Self>) {
        match entry {
            btree_map::Entry::Vacant(e) => {
                // No-op -> Delete
                e.insert(Self::Delete);
            }
            btree_map::Entry::Occupied(mut e) => {
                if e.get().is_insert() {
                    // Insert -> No-op
                    e.remove();
                } else if e.get().is_delete_insert() {
                    // DeleteInsert -> Delete
                    e.insert(Self::Delete);
                } else {
                    panic!("invalid flush status");
                }
            }
        }
    }
}

/// Verify if the data going through the state is valid by checking if `ops.len() ==
/// visibility.len() == data[x].len()`.
pub fn verify_batch(
    ops: risingwave_common::array::stream_chunk::Ops<'_>,
    visibility: Option<&risingwave_common::buffer::Bitmap>,
    data: &[&risingwave_common::array::ArrayImpl],
) -> bool {
    let mut all_lengths = vec![ops.len()];
    if let Some(visibility) = visibility {
        all_lengths.push(visibility.len());
    }
    all_lengths.extend(data.iter().map(|x| x.len()));
    all_lengths.iter().min() == all_lengths.iter().max()
}

/// All managed state for state aggregation. The managed state will manage the cache and integrate
/// the state with the underlying state store. Managed states can only be evicted from outer cache
/// when they are not dirty.
pub enum ManagedStateImpl<S: StateStore> {
    Value(ManagedValueState<S>),
    Extreme(Box<dyn ManagedExtremeState<S>>),
}

impl<S: StateStore> ManagedStateImpl<S> {
    pub async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        match self {
            Self::Value(state) => state.apply_batch(ops, visibility, data).await,
            Self::Extreme(state) => state.apply_batch(ops, visibility, data).await,
        }
    }

    /// Get the output of the state. Must flush before getting output.
    pub async fn get_output(&mut self) -> Result<Datum> {
        match self {
            Self::Value(state) => state.get_output().await,
            Self::Extreme(state) => state.get_output().await,
        }
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        match self {
            Self::Value(state) => state.is_dirty(),
            Self::Extreme(state) => state.is_dirty(),
        }
    }

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    pub fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        match self {
            Self::Value(state) => state.flush(write_batch),
            Self::Extreme(state) => state.flush(write_batch),
        }
    }

    /// Create a managed state from `agg_call`.
    pub async fn create_managed_state(
        agg_call: AggCall,
        keyspace: Keyspace<S>,
        row_count: Option<usize>,
        pk_length: usize,
        is_row_count: bool,
    ) -> Result<Self> {
        match agg_call.kind {
            AggKind::Max | AggKind::Min => {
                assert!(
                    row_count.is_some(),
                    "should set row_count for value states other than AggKind::RowCount"
                );
                Ok(Self::Extreme(
                    create_streaming_extreme_state(
                        agg_call,
                        keyspace,
                        row_count.unwrap(),
                        // TODO: estimate a good cache size instead of hard-coding
                        Some(1024),
                        pk_length,
                    )
                    .await?,
                ))
            }
            AggKind::StringAgg => {
                // TODO, It seems with `order by`, `StringAgg` needs more stuff from `AggCall`
                unimplemented!()
            }
            // TODO: for append-only lists, we can create `ManagedValueState` instead of
            // `ManagedExtremeState`.
            AggKind::Avg | AggKind::Count | AggKind::Sum => {
                assert!(
                    is_row_count || row_count.is_some(),
                    "should set row_count for value states other than AggKind::RowCount"
                );
                Ok(Self::Value(
                    ManagedValueState::new(agg_call, keyspace, row_count).await?,
                ))
            }
            AggKind::RowCount => {
                assert!(is_row_count);
                Ok(Self::Value(
                    ManagedValueState::new(agg_call, keyspace, row_count).await?,
                ))
            }
        }
    }
}
