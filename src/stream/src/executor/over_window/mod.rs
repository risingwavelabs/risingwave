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

use std::collections::BTreeMap;
use std::marker::PhantomData;

use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::StreamChunk;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{must_match, row};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use self::call::WindowFuncCall;
use self::partition::Partition;
use self::state::StateKey;
use super::StreamExecutorResult;
use crate::cache::ExecutorCache;
use crate::common::table::state_table::StateTable;
use crate::common::StateTableColumnMapping;

mod call;
mod partition;
mod state;

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

type MemcmpEncoded = Box<[u8]>;

/// One for each combination of partition key and order key.
///
/// Output schema:
///     partition key | order key | pk | window function results
struct ExecutorInner<S: StateStore> {
    window_func_calls: Vec<WindowFuncCall>,
    state_table: StateTable<S>,
    mapping: StateTableColumnMapping,
    pk_indices: Vec<usize>,
    pk_data_types: Vec<DataType>,
    partition_key_indices: Vec<usize>,
    order_key_index: usize,
}

struct ExecutionVars<S: StateStore> {
    // TODO(rc): use `K: HashKey` as key like in hash agg?
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
                        let encoded_pk = memcmp_encoding::encode_row(
                            (&row).project(
                                &this
                                    .pk_indices
                                    .iter()
                                    .map(|idx| this.mapping.upstream_to_state_table(*idx).unwrap())
                                    .collect_vec(),
                            ),
                            &vec![OrderType::ascending(); this.pk_indices.len()],
                        )?
                        .into_boxed_slice();
                        let key = StateKey {
                            order_key,
                            encoded_pk,
                        };
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
                        assert!(partition.curr_windows_are_aligned());
                    }

                    {
                        // ignore ready windows
                        while partition
                            .states
                            .iter()
                            .all(|state| state.curr_window().is_ready)
                        {
                            partition.states.iter_mut().for_each(|state| {
                                state.slide();
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
        let encoded_pk = memcmp_encoding::encode_row(
            input_row.project(&this.pk_indices),
            &vec![OrderType::ascending(); this.pk_indices.len()],
        )?
        .into_boxed_slice();
        let key = StateKey {
            order_key,
            encoded_pk,
        };
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
            debug_assert!(partition.curr_windows_are_aligned());
            let key = partition.states[0]
                .curr_window()
                .key
                .cloned()
                .expect("ready window must have state key");
            let pk = memcmp_encoding::decode_row(
                &key.encoded_pk,
                &this.pk_data_types,
                &vec![OrderType::ascending(); this.pk_indices.len()],
            )?;
            let outputs = partition.states.iter_mut().map(|state| state.slide());
            // TODO(): evict unneeded rows from state table
            let output_row = partition_key
                .chain(row::once(Some(key.order_key)))
                .chain(pk)
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

    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::expr::WindowFuncKind;
    use risingwave_storage::memory::MemoryStateStore;

    use super::call::{Frame, WindowFuncCall};
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
        let pk_data_types = vec![DataType::Int64];
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
                pk_data_types: pk_data_types.clone(),
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
                pk_data_types: pk_data_types.clone(),
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
