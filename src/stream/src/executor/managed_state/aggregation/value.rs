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

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Row};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::aggregation::{create_streaming_agg_state, AggCall, StreamingAggStateImpl};
use crate::executor::error::StreamExecutorResult;

/// A wrapper around [`StreamingAggStateImpl`], which fetches data from the state store and helps
/// update the state. We don't use any trait to wrap around all `ManagedXxxState`, so as to reduce
/// the overhead of creating boxed async future.
pub struct ManagedValueState {
    /// Upstream column indices of agg arguments.
    arg_indices: Vec<usize>,

    /// The internal single-value state.
    state: Box<dyn StreamingAggStateImpl>,

    /// Indicates whether this managed state is dirty. If this state is dirty, we cannot evict the
    /// state from memory.
    is_dirty: bool,

    /// Primary key to look up in relational table. For value state, there is only one row.
    /// If None, the pk is empty vector (simple agg). If not None, the pk is group key (hash agg).
    group_key: Option<Row>,
}

impl ManagedValueState {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub async fn new<S: StateStore>(
        agg_call: AggCall,
        row_count: Option<usize>,
        group_key: Option<&Row>,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let data = if row_count != Some(0) {
            // View the state table as single-value table, and get the value via empty primary key
            // or group key.
            let raw_data = state_table
                .get_row(group_key.unwrap_or_else(Row::empty))
                .await?;

            // According to row layout, the last field of the row is value and we sure the row is
            // not empty.
            raw_data.map(|row| row.0.last().unwrap().clone())
        } else {
            None
        };

        // Create the internal state based on the value we get.
        Ok(Self {
            arg_indices: agg_call.args.val_indices().to_vec(),
            state: create_streaming_agg_state(
                agg_call.args.arg_types(),
                &agg_call.kind,
                &agg_call.return_type,
                data,
            )?,
            is_dirty: false,
            group_key: group_key.cloned(),
        })
    }

    /// Apply a chunk of data to the state.
    pub fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, columns));
        self.is_dirty = true;
        let data = self
            .arg_indices
            .iter()
            .map(|col_idx| columns[*col_idx])
            .collect_vec();
        self.state.apply_batch(ops, visibility, &data)
    }

    /// Get the output of the state. Note that in our case, getting the output is very easy, as the
    /// output is the same as the aggregation state. In other aggregators, like min and max,
    /// `get_output` might involve a scan from the state store.
    pub fn get_output(&self) -> Datum {
        self.state
            .get_output()
            .expect("agg call throw an error in streamAgg")
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn flush<S: StateStore>(
        &mut self,
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        // If the managed state is not dirty, the caller should not flush. But forcing a flush won't
        // cause incorrect result: it will only produce more I/O.
        debug_assert!(self.is_dirty());

        // Persist value into relational table. The inserted row should concat with pk (pk is in
        // front of value). In this case, the pk is just group key.

        let mut v = vec![];
        v.extend_from_slice(&self.group_key.as_ref().unwrap_or_else(Row::empty).0);
        v.push(self.get_output());

        state_table.insert(Row::new(v));

        self.is_dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::streaming_table::state_table::StateTable;

    use super::*;
    use crate::executor::aggregation::AggArgs;

    fn create_test_count_state() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Count,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state() {
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            TableId::from(0x2333),
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![],
            vec![],
        );
        let mut epoch: u64 = 0;
        state_table.init_epoch(epoch);
        epoch += 1;

        let mut managed_state =
            ManagedValueState::new(create_test_count_state(), Some(0), None, &state_table)
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        // apply a batch and get the output
        managed_state
            .apply_chunk(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(1), Some(2), None]).into()],
            )
            .unwrap();
        assert!(managed_state.is_dirty());

        // write to state store
        managed_state.flush(&mut state_table).unwrap();
        state_table.commit(epoch).await.unwrap();

        // get output
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(3)));

        // reload the state and check the output
        let managed_state =
            ManagedValueState::new(create_test_count_state(), None, None, &state_table)
                .await
                .unwrap();
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(3)));
    }

    fn create_test_max_agg_append_only() -> AggCall {
        AggCall {
            kind: risingwave_expr::expr::AggKind::Max,
            args: AggArgs::Unary(DataType::Int64, 0),
            return_type: DataType::Int64,
            order_pairs: vec![],
            append_only: true,
            filter: None,
        }
    }

    #[tokio::test]
    async fn test_managed_value_state_append_only() {
        let pk_index = vec![];
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            TableId::from(0x2333),
            vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)],
            vec![],
            pk_index,
        );
        let mut epoch: u64 = 0;
        state_table.init_epoch(epoch);
        epoch += 1;

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
            .apply_chunk(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(-1), Some(0), Some(2), Some(1), None]).into()],
            )
            .unwrap();
        assert!(managed_state.is_dirty());

        // write to state store
        managed_state.flush(&mut state_table).unwrap();
        state_table.commit(epoch).await.unwrap();

        // get output
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(2)));

        // reload the state and check the output
        let managed_state =
            ManagedValueState::new(create_test_max_agg_append_only(), None, None, &state_table)
                .await
                .unwrap();
        assert_eq!(managed_state.get_output(), Some(ScalarImpl::Int64(2)));
    }
}
