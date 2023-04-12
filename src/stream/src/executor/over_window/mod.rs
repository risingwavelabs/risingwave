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

use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;

use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;
use smallvec::SmallVec;

use self::window_func_call::WindowFuncCall;
use super::StreamExecutorResult;
use crate::cache::ExecutorCache;
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;

mod state;
mod window_func_call;

type MemcmpEncoded = Box<[u8]>;

/// Basic idea:
///
/// ──────────────┬──────────────────────────────────────────────────────curr evict row
///               │ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
///        111    │ ─┬─
///               │  │RANGE BETWEEN '1hr' PRECEDING AND '1hr' FOLLOWING
///        ─┬─    │  │ ─┬─
///   LAG(1)│        │  │
/// ────────┴──┬─────┼──┼────────────────────────────────────────────────curr output row
///     LEAD(1)│     │  │GROUPS 1 PRECEDING AND 1 FOLLOWING
///                  │
///                  │ 222
/// ─────────────────┴───────────────────────────────────────────────────curr input row
/// 111: additional buffered input (unneeded) for some window
/// 222: additional delay (already able to output) for some window
///
/// State table schema:
///     partition key | order key | pk | window function arguments
///
/// - Rows in range (`curr evict row`, `curr input row`] are in `state_table`.
/// - `curr evict row` <= min(last evict rows of all `WindowState`s).
/// - `WindowState` should output agg result for `curr output row`.
/// - Recover: iterate through `state_table`, push rows to `WindowState`, ignore ready windows.

/// Unique and ordered identifier for a row in internal states.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StateKey {
    order_key: ScalarImpl,
    pk: OwnedRow,
}

struct StatePos<'a> {
    /// Only 2 cases in which the `key` is `None`:
    /// 1. The state is empty.
    /// 2. It's a pure preceding window, and all ready outputs are consumed.
    key: Option<&'a StateKey>,
    is_ready: bool,
}

struct StateOutput {
    return_value: Datum,
    last_evicted_key: Option<StateKey>,
}

impl PartialOrd for StateKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other)) // TODO(): real partial cmp
    }
}

impl Ord for StateKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // TODO()
        let res = self.order_key.cmp(&other.order_key);
        if res.is_eq() {
            let order_types = vec![OrderType::ascending(); self.pk.len()];
            memcmp_encoding::encode_row(&self.pk, &order_types)
                .unwrap()
                .cmp(&memcmp_encoding::encode_row(&other.pk, &order_types).unwrap())
        } else {
            res
        }
    }
}

trait WindowFuncState {
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>); // TODO(): change to `input(chunk)`

    /// Get the current window frame position.
    fn curr_window(&self) -> StatePos<'_>;

    /// Return the output for the current ready window frame and push the window forward.
    fn output(&mut self) -> StateOutput;
}

struct Partition {
    states: Vec<Box<dyn WindowFuncState>>,
}

impl Partition {
    fn new(calls: &[WindowFuncCall]) -> Self {
        let states = calls.iter().map(|call| call.new_state()).collect();
        Self { states }
    }
}

/// One for each combination of partition key and order key.
///
/// Output schema:
///     partition key | order key | pk | window function results
struct ExecutorInner<S: StateStore> {
    window_func_calls: Vec<WindowFuncCall>,
    state_table: StateTable<S>,
    mapping: StateTableColumnMapping,
    pk_indices: Vec<usize>,
    partition_key_indices: Vec<usize>,
    order_key_index: usize,
}

struct ExecutionVars<S: StateStore> {
    // TODO(): use `K: HashKey` as key like in hash agg?
    // TODO(): use `ExecutorCache`
    partitions: BTreeMap<MemcmpEncoded, Partition>,
    _phantom: PhantomData<S>,
}

async fn apply_chunk<S: StateStore>(
    this: &mut ExecutorInner<S>,
    vars: &mut ExecutionVars<S>,
    chunk: StreamChunk,
) -> StreamExecutorResult<Vec<OwnedRow>> {
    let mut output_rows = vec![];

    // We assume that the input is sorted by order key.
    for record in chunk.records() {
        // TODO(): do this in batching way later

        let input_row = must_match!(record, Record::Insert { new_row } => new_row);

        let partition_key = input_row.project(&this.partition_key_indices);
        let encoded_partition_key = memcmp_encoding::encode_row(
            partition_key,
            &vec![OrderType::ascending(); this.partition_key_indices.len()],
        )?
        .into_boxed_slice();

        {
            let input_row: bool; // in case of accidental use

            // ensure_key_in_cache
            if !vars.partitions.contains_key(&encoded_partition_key) {
                let mut partition = Partition::new(&this.window_func_calls);

                {
                    // recover from state table

                    let iter = this
                        .state_table
                        .iter_with_pk_prefix(partition_key, PrefetchOptions::new_for_exhaust_iter())
                        .await?;

                    #[for_await]
                    for res in iter {
                        let row: OwnedRow = res?;
                        let order_key = row
                            .datum_at(
                                this.mapping
                                    .upstream_to_state_table(this.order_key_index)
                                    .unwrap(),
                            )
                            .expect("order key column must be non-NULL")
                            .into_scalar_impl();
                        let pk = (&row)
                            .project(
                                &this
                                    .pk_indices
                                    .iter()
                                    .map(|idx| this.mapping.upstream_to_state_table(*idx).unwrap())
                                    .collect_vec(),
                            )
                            .into_owned_row();
                        let key = StateKey { order_key, pk };
                        for (call, state) in this
                            .window_func_calls
                            .iter()
                            .zip_eq_fast(&mut partition.states)
                        {
                            state.append(
                                key.clone(),
                                (&row)
                                    .project(
                                        &call
                                            .args
                                            .val_indices()
                                            .iter()
                                            .map(|idx| {
                                                this.mapping.upstream_to_state_table(*idx).unwrap()
                                            })
                                            .collect_vec(),
                                    )
                                    .into_owned_row()
                                    .into_inner()
                                    .into(),
                            );
                        }
                    }

                    {
                        // ensure state correctness
                        // TODO(): need some proof
                        assert_eq!(
                            1,
                            partition
                                .states
                                .iter()
                                .map(|state| state.curr_window().key)
                                .dedup()
                                .count()
                        );
                    }

                    {
                        // ignore ready windows
                        while partition
                            .states
                            .iter()
                            .all(|state| state.curr_window().is_ready)
                        {
                            partition.states.iter_mut().for_each(|state| {
                                state.output();
                            });
                        }
                    }
                }

                vars.partitions
                    .insert(encoded_partition_key.clone(), partition);
            }
        }

        let partition = vars.partitions.get_mut(&encoded_partition_key).unwrap();

        // Materialize input to state table.
        this.state_table
            .insert(input_row.project(this.mapping.upstream_columns()));

        // Feed the row to all window states.
        let order_key = input_row
            .datum_at(this.order_key_index)
            .expect("order key column must be non-NULL")
            .into_scalar_impl();
        let pk = input_row.project(&this.pk_indices).into_owned_row();
        let key = StateKey { order_key, pk };
        for (call, state) in this
            .window_func_calls
            .iter()
            .zip_eq_fast(&mut partition.states)
        {
            state.append(
                key.clone(),
                input_row
                    .project(call.args.val_indices())
                    .into_owned_row()
                    .into_inner()
                    .into(),
            );
        }

        while partition
            .states
            .iter()
            .all(|state| state.curr_window().is_ready)
        {
            // All window states are ready to output, so we can yield an output row.
            let key = partition.states[0]
                .curr_window()
                .key
                .cloned()
                .expect("ready window must have state key");
            let outputs = partition.states.iter_mut().map(|state| state.output());
            // TODO(): evict unneeded rows from state table
            let output_row = partition_key
                .chain(row::once(Some(key.order_key)))
                .chain(key.pk)
                .chain(OwnedRow::new(
                    outputs.map(|output| output.return_value).collect(),
                ))
                .into_owned_row();
            output_rows.push(output_row);
        }
    }

    Ok(output_rows)
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;
    use std::ops::Bound;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::WindowFuncKind;
    use risingwave_storage::memory::MemoryStateStore;

    use super::window_func_call::{Frame, WindowFuncCall};
    use super::ExecutorInner;
    use crate::common::table::state_table::StateTable;
    use crate::common::StateTableColumnMapping;
    use crate::executor::aggregation::AggArgs;
    use crate::executor::over_window::ExecutionVars;

    #[tokio::test]
    async fn test() {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),   // order key
            Field::unnamed(DataType::Varchar), // partition key
            Field::unnamed(DataType::Int64),   // pk
            Field::unnamed(DataType::Int32),   // x
        ]);
        let pk_indices = vec![2];
        let partition_key_indices = vec![1];
        let order_key_index = 0;

        let store = MemoryStateStore::new();
        let mut epoch = EpochPair::new_test_epoch(1);

        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Varchar), // partition key
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32),   // order key
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),   // pk
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32),   // x
        ];
        let table_pk_indices = vec![0, 1, 2];
        let table_order_types = vec![
            OrderType::ascending(),
            OrderType::ascending(),
            OrderType::ascending(),
        ];
        let mapping = StateTableColumnMapping::new(vec![1, 0, 2, 3], None);

        let window_func_calls = vec![
            WindowFuncCall {
                kind: WindowFuncKind::Lag,
                args: AggArgs::Unary(DataType::Int32, 3),
                frame: Frame::Offset(-1),
            },
            WindowFuncCall {
                kind: WindowFuncKind::Lead,
                args: AggArgs::Unary(DataType::Int32, 3),
                frame: Frame::Offset(1),
            },
        ];

        {
            // test basic

            let state_table = StateTable::new_without_distribution(
                store.clone(),
                TableId::new(1),
                table_columns.clone(),
                table_order_types.clone(),
                table_pk_indices.clone(),
            )
            .await;

            let mut this = ExecutorInner {
                window_func_calls: window_func_calls.clone(),
                state_table,
                mapping: mapping.clone(),
                pk_indices: pk_indices.clone(),
                partition_key_indices: partition_key_indices.clone(),
                order_key_index,
            };
            let mut vars = ExecutionVars {
                partitions: Default::default(),
                _phantom: PhantomData,
            };

            epoch.inc();
            this.state_table.init_epoch(epoch);

            let chunk = StreamChunk::from_pretty(
                " i T  I   i
                + 1 p1 100 10
                + 1 p1 101 16
                + 4 p2 200 20
                + 5 p1 102 18
                + 7 p2 201 22
                + 8 p3 300 33",
            );

            let result = super::apply_chunk(&mut this, &mut vars, chunk)
                .await
                .unwrap();
            for row in result {
                println!("{:?}", row.as_inner());
            }
            println!("-------------------------");

            epoch.inc();
            this.state_table.commit(epoch).await.unwrap();
        }

        {
            // test recovery

            let state_table = StateTable::new_without_distribution(
                store.clone(),
                TableId::new(1),
                table_columns.clone(),
                table_order_types.clone(),
                table_pk_indices.clone(),
            )
            .await;

            let mut this = ExecutorInner {
                window_func_calls: window_func_calls.clone(),
                state_table,
                mapping: mapping.clone(),
                pk_indices: pk_indices.clone(),
                partition_key_indices: partition_key_indices.clone(),
                order_key_index,
            };
            let mut vars = ExecutionVars {
                partitions: Default::default(),
                _phantom: PhantomData,
            };

            epoch.inc();
            this.state_table.init_epoch(epoch);

            let chunk = StreamChunk::from_pretty(
                " i  T  I   i
                + 10 p1 103 13
                + 12 p2 202 28
                + 13 p3 301 39",
            );

            let result = super::apply_chunk(&mut this, &mut vars, chunk)
                .await
                .unwrap();
            for row in result {
                println!("{:?}", row.as_inner());
            }
            println!("-------------------------");

            epoch.inc();
            this.state_table.commit(epoch).await.unwrap();
        }
    }
}
