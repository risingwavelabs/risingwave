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

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::StateStore;

use crate::executor::aggregation::{create_streaming_agg_state, AggCall, StreamingAggStateImpl};
use crate::executor::error::StreamExecutorResult;

/// A wrapper around [`StreamingAggStateImpl`], which fetches data from the state store and helps
/// update the state. We don't use any trait to wrap around all `ManagedXxxState`, so as to reduce
/// the overhead of creating boxed async future.
pub struct ManagedValueState {
    /// The internal single-value state.
    state: Box<dyn StreamingAggStateImpl>,

    /// Indicates whether this managed state is dirty. If this state is dirty, we cannot evict the
    /// state from memory.
    is_dirty: bool,

    /// Primary key to look up in relational table. For value state, there is only one row.
    /// If None, the pk is empty vector (simple agg). If not None, the pk is group key (hash agg).
    pk: Option<Row>,
}

impl ManagedValueState {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub async fn new<S: StateStore>(
        agg_call: AggCall,
        row_count: Option<usize>,
        pk: Option<&Row>,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let data = if row_count != Some(0) {
            // TODO: use the correct epoch
            let epoch = u64::MAX;

            // View the state table as single-value table, and get the value via empty primary key
            // or group key.
            let raw_data = state_table
                .get_row(pk.unwrap_or(&Row(vec![])), epoch)
                .await?;

            // According to row layout, the last field of the row is value and we sure the row is
            // not empty.
            raw_data.map(|row| row.0.last().unwrap().clone())
        } else {
            None
        };

        // Create the internal state based on the value we get.
        Ok(Self {
            state: create_streaming_agg_state(
                agg_call.args.arg_types(),
                &agg_call.kind,
                &agg_call.return_type,
                data,
            )?,
            is_dirty: false,
            pk: pk.cloned(),
        })
    }

    /// Apply a batch of data to the state.
    pub async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));
        self.is_dirty = true;
        self.state.apply_batch(ops, visibility, data)
    }

    /// Get the output of the state. Note that in our case, getting the output is very easy, as the
    /// output is the same as the aggregation state. In other aggregators, like min and max,
    /// `get_output` might involve a scan from the state store.
    pub async fn get_output(&mut self) -> StreamExecutorResult<Datum> {
        debug_assert!(!self.is_dirty());
        self.state.get_output()
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    /// Flush the internal state to a write batch.
    pub async fn flush<S: StateStore>(
        &mut self,
        _write_batch: &mut WriteBatch<S>,
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        // If the managed state is not dirty, the caller should not flush. But forcing a flush won't
        // cause incorrect result: it will only produce more I/O.
        debug_assert!(self.is_dirty());

        // Persist value into relational table. The inserted row should concat with pk (pk is in
        // front of value). In this case, the pk is just group key.
        let mut v = vec![self.state.get_output()?];
        v.extend(self.pk.as_ref().unwrap_or(&Row(vec![])).0.iter().cloned());
        state_table.insert(self.pk.as_ref().unwrap_or(&Row(vec![])), Row::new(v))?;

        self.is_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_storage::table::state_table::StateTable;

    use super::*;
    use crate::executor::aggregation::AggArgs;
    use crate::executor::test_utils::create_in_memory_keyspace;

    fn create_test_count_state() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Count,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            append_only: false,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state() {
        let keyspace = create_in_memory_keyspace();
        let mut state_table = StateTable::new(
            keyspace.clone(),
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![],
            None,
            vec![],
        );
        let mut managed_state =
            ManagedValueState::new(create_test_count_state(), Some(0), None, &state_table)
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        // apply a batch and get the output
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(1), Some(2), None])
                    .unwrap()
                    .into()],
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // flush to write batch and write to state store
        let epoch: u64 = 0;
        let mut write_batch = keyspace.state_store().start_write_batch();
        managed_state
            .flush(&mut write_batch, &mut state_table)
            .await
            .unwrap();
        state_table.commit(epoch).await.unwrap();

        // get output
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );

        // reload the state and check the output
        let mut managed_state =
            ManagedValueState::new(create_test_count_state(), None, None, &state_table)
                .await
                .unwrap();
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(3))
        );
    }

    fn create_test_max_agg_append_only() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Max,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            append_only: true,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state_append_only() {
        let keyspace = create_in_memory_keyspace();
        let pk_index = vec![0_usize, 1_usize];
        let mut state_table = StateTable::new(
            keyspace.clone(),
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![],
            None,
            pk_index,
        );
        let mut managed_state = ManagedValueState::new(
            create_test_max_agg_append_only(),
            Some(0),
            None,
            &state_table,
        )
        .await
        .unwrap();
        assert!(!managed_state.is_dirty());

        // apply a batch and get the output
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[
                    &I64Array::from_slice(&[Some(-1), Some(0), Some(2), Some(1), None])
                        .unwrap()
                        .into(),
                ],
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // flush to write batch and write to state store
        let epoch: u64 = 0;
        let mut write_batch = keyspace.state_store().start_write_batch();
        managed_state
            .flush(&mut write_batch, &mut state_table)
            .await
            .unwrap();
        state_table.commit(epoch).await.unwrap();

        // get output
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(2))
        );

        // reload the state and check the output
        let mut managed_state =
            ManagedValueState::new(create_test_max_agg_append_only(), None, None, &state_table)
                .await
                .unwrap();
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(2))
        );
    }
}
