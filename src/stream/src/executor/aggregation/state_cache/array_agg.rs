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

use risingwave_common::array::ListValue;
use risingwave_common::types::{Datum, DatumRef, ScalarRefImpl};
use smallvec::SmallVec;

use super::{OrderedCache, StateCacheAggregator};

pub struct ArrayAgg;

impl StateCacheAggregator for ArrayAgg {
    type Value = Datum;

    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value {
        value[0].map(ScalarRefImpl::into_scalar_impl)
    }

    fn aggregate(&self, cache: &OrderedCache<Self::Value>) -> Datum {
        let mut values = Vec::with_capacity(cache.len());
        for cache_value in cache.iter_values() {
            values.push(cache_value.clone());
        }
        Some(ListValue::new(values).into())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderPair;
    use risingwave_expr::expr::AggKind;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::aggregation::AggArgs;

    #[tokio::test]
    async fn test_array_agg_state_simple_agg_without_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Varchar, 0), // array_agg(a)
            return_type: DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            order_pairs: vec![],
            append_only: false,
            filter: None,
        };

        // see `LogicalAgg::infer_stream_agg_state` for the construction of state table
        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![3, 0]);
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![OrderType::Ascending],
            vec![0], // [_row_id]
        );

        let mut managed_state = ManagedArrayAggState::new(
            &agg_call,
            None,
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        let chunk = StreamChunk::from_pretty(
            " T i i I
            + a 1 8 123
            + b 5 2 128
            - b 5 2 128
            + . 7 6 129
            + c 1 3 130",
        );
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
        managed_state
            .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
            .await?;

        epoch.inc();
        state_table.commit_for_test(epoch).await.unwrap();

        let res = managed_state.get_output(&state_table).await?;
        match res {
            Some(ScalarImpl::List(res)) => {
                let res = res
                    .values()
                    .iter()
                    .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                    .collect_vec();
                assert_eq!(res.len(), 3);
                assert!(res.contains(&Some("a".to_string())));
                assert!(res.contains(&Some("c".to_string())));
                assert!(res.contains(&None));
            }
            _ => panic!("unexpected output"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_state_simple_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)
        // where `a` is the column to aggregate

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Int32, 1), // array_agg(b)
            return_type: DataType::Int32,
            order_pairs: vec![
                OrderPair::new(2, OrderType::Ascending),  // c ASC
                OrderPair::new(0, OrderType::Descending), // a DESC
            ],
            append_only: false,
            filter: None,
        };

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar), // a
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Int32), // b
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![2, 0, 3, 1]);
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending,  // c ASC
                OrderType::Descending, // a DESC
                OrderType::Ascending,  // _row_id ASC
            ],
            vec![0, 1, 2], // [c, a, _row_id]
        );

        let mut managed_state = ManagedArrayAggState::new(
            &agg_call,
            None,
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 2 128
                - b 5 2 128
                + c 2 3 130",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_int32).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some(2), Some(1)]);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 0 8 134
                + e 2 2 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_int32).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some(2), Some(2), Some(0), Some(1)]);
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_state_grouped_agg_with_order() -> StreamExecutorResult<()> {
        // Assumption of input schema:
        // (a: varchar, b: int32, c: int32, _row_id: int64)

        let input_pk_indices = vec![3];
        let agg_call = AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Varchar, 0),
            return_type: DataType::Varchar,
            order_pairs: vec![
                OrderPair::new(1, OrderType::Ascending), // b ASC
            ],
            append_only: false,
            filter: None,
        };

        let table_id = TableId::new(6666);
        let columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int32), // group by c
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int32), // order by b
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // _row_id
            ColumnDesc::unnamed(ColumnId::new(3), DataType::Varchar), // a
        ];
        let state_table_col_mapping = StateTableColumnMapping::new(vec![2, 1, 3, 0]);
        let mut state_table = StateTable::new_without_distribution(
            MemoryStateStore::new(),
            table_id,
            columns,
            vec![
                OrderType::Ascending, // c ASC
                OrderType::Ascending, // b ASC
                OrderType::Ascending, // _row_id ASC
            ],
            vec![0, 1, 2], // [c, b, _row_id]
        );

        let mut managed_state = ManagedArrayAggState::new(
            &agg_call,
            Some(&Row::new(vec![Some(8.into())])),
            &input_pk_indices,
            state_table_col_mapping,
            0,
        );

        let epoch = EpochPair::new_test_epoch(1);
        state_table.init_epoch(epoch);
        epoch.inc();

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + a 1 8 123
                + b 5 8 128
                + c 1 3 130 D // hide this row",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();
            epoch.inc();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                        .collect_vec();
                    assert_eq!(res, vec![Some("a".to_string()), Some("b".to_string())]);
                }
                _ => panic!("unexpected output"),
            }
        }

        {
            let chunk = StreamChunk::from_pretty(
                " T i i I
                + d 0 2 134 D // hide this row
                + e 2 8 137",
            );
            let (ops, columns, visibility) = chunk.into_inner();
            let column_refs: Vec<_> = columns.iter().map(|col| col.array_ref()).collect();
            managed_state
                .apply_chunk(&ops, visibility.as_ref(), &column_refs, &mut state_table)
                .await?;

            state_table.commit_for_test(epoch).await.unwrap();

            let res = managed_state.get_output(&state_table).await?;
            match res {
                Some(ScalarImpl::List(res)) => {
                    let res = res
                        .values()
                        .iter()
                        .map(|v| v.as_ref().map(ScalarImpl::as_utf8).cloned())
                        .collect_vec();
                    assert_eq!(
                        res,
                        vec![
                            Some("a".to_string()),
                            Some("e".to_string()),
                            Some("b".to_string())
                        ]
                    );
                }
                _ => panic!("unexpected output"),
            }
        }

        Ok(())
    }
}
