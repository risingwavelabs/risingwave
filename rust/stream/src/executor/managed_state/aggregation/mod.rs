//! Aggregators with state store support

mod value;

use risingwave_common::expr::AggKind;
pub use value::*;
mod extreme;
mod extreme_serializer;
mod string_agg;

pub use extreme::*;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use super::super::{AggCall, PkDataTypes};

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
        epoch: u64,
    ) -> Result<()> {
        match self {
            Self::Value(state) => state.apply_batch(ops, visibility, data).await,
            Self::Extreme(state) => state.apply_batch(ops, visibility, data, epoch).await,
        }
    }

    /// Get the output of the state. Must flush before getting output.
    pub async fn get_output(&mut self, epoch: u64) -> Result<Datum> {
        match self {
            Self::Value(state) => state.get_output().await,
            Self::Extreme(state) => state.get_output(epoch).await,
        }
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        match self {
            Self::Value(state) => state.is_dirty(),
            Self::Extreme(state) => state.is_dirty(),
        }
    }

    /// Flush the internal state to a write batch.
    pub fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
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
        pk_data_types: PkDataTypes,
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
                        pk_data_types,
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
            AggKind::SingleValue => todo!(),
        }
    }
}
