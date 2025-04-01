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

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures::future;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::hash::SerializedKey;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::aggregate::{AggCall, AggType, PbAggKind};
use risingwave_pb::stream_plan::PbAggNodeVersion;
use risingwave_storage::StateStore;

use crate::common::StateTableColumnMapping;
use crate::common::table::state_table::StateTable;
use crate::common::table::test_utils::gen_pbtable;
use crate::executor::aggregate::{
    AggExecutorArgs, AggStateStorage, HashAggExecutor, HashAggExecutorExtraArgs, SimpleAggExecutor,
    SimpleAggExecutorExtraArgs,
};
use crate::executor::{ActorContext, ActorContextRef, Executor, ExecutorInfo, PkIndices};

/// Generate aggExecuter's schema from `input`, `agg_calls` and `group_key_indices`.
/// For [`crate::executor::aggregate::HashAggExecutor`], the group key indices should be provided.
pub fn generate_agg_schema(
    input_ref: &Executor,
    agg_calls: &[AggCall],
    group_key_indices: Option<&[usize]>,
) -> Schema {
    let aggs = agg_calls
        .iter()
        .map(|agg| Field::unnamed(agg.return_type.clone()));

    let fields = if let Some(key_indices) = group_key_indices {
        let keys = key_indices
            .iter()
            .map(|idx| input_ref.schema().fields[*idx].clone());

        keys.chain(aggs).collect()
    } else {
        aggs.collect()
    };

    Schema { fields }
}

/// Create state storage for the given agg call.
/// Should infer the schema in the same way as `LogicalAgg::infer_stream_agg_state`.
pub async fn create_agg_state_storage<S: StateStore>(
    store: S,
    table_id: TableId,
    agg_call: &AggCall,
    group_key_indices: &[usize],
    pk_indices: &[usize],
    input_fields: Vec<Field>,
    is_append_only: bool,
) -> AggStateStorage<S> {
    match agg_call.agg_type {
            AggType::Builtin(PbAggKind::Min | PbAggKind::Max) if !is_append_only => {
                let mut column_descs = Vec::new();
                let mut order_types = Vec::new();
                let mut upstream_columns = Vec::new();
                let mut order_columns = Vec::new();

                let mut next_column_id = 0;
                let mut add_column = |upstream_idx: usize, data_type: DataType, order_type: Option<OrderType>| {
                    upstream_columns.push(upstream_idx);
                    column_descs.push(ColumnDesc::unnamed(
                        ColumnId::new(next_column_id),
                        data_type,
                    ));
                    if let Some(order_type) = order_type {
                        order_columns.push(ColumnOrder::new(upstream_idx as _, order_type));
                        order_types.push(order_type);
                    }
                    next_column_id += 1;
                };

                for idx in group_key_indices {
                    add_column(*idx, input_fields[*idx].data_type(), None);
                }

                add_column(agg_call.args.val_indices()[0], agg_call.args.arg_types()[0].clone(), if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Max)) {
                    Some(OrderType::descending())
                } else {
                    Some(OrderType::ascending())
                });

                for idx in pk_indices {
                    add_column(*idx, input_fields[*idx].data_type(), Some(OrderType::ascending()));
                }

                let state_table = StateTable::from_table_catalog(
                    &gen_pbtable(
                        table_id,
                        column_descs,
                        order_types.clone(),
                        (0..order_types.len()).collect(),
                        0,
                    ),
                    store,
                    None,
                ).await;

                AggStateStorage::MaterializedInput { table: state_table, mapping: StateTableColumnMapping::new(upstream_columns, None), order_columns }
            }
            AggType::Builtin(
                PbAggKind::Min /* append only */
                | PbAggKind::Max /* append only */
                | PbAggKind::Sum
                | PbAggKind::Sum0
                | PbAggKind::Count
                | PbAggKind::Avg
                | PbAggKind::ApproxCountDistinct
            ) => {
                AggStateStorage::Value
            }
            _ => {
                panic!("no need to mock other agg kinds here");
            }
        }
}

/// Create intermediate state table for agg executor.
pub async fn create_intermediate_state_table<S: StateStore>(
    store: S,
    table_id: TableId,
    agg_calls: &[AggCall],
    group_key_indices: &[usize],
    input_fields: Vec<Field>,
) -> StateTable<S> {
    let mut column_descs = Vec::new();
    let mut order_types = Vec::new();

    let mut next_column_id = 0;
    let mut add_column_desc = |data_type: DataType| {
        column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(next_column_id),
            data_type,
        ));
        next_column_id += 1;
    };

    group_key_indices.iter().for_each(|idx| {
        add_column_desc(input_fields[*idx].data_type());
        order_types.push(OrderType::ascending());
    });

    agg_calls.iter().for_each(|agg_call| {
        add_column_desc(agg_call.return_type.clone());
    });

    StateTable::from_table_catalog_inconsistent_op(
        &gen_pbtable(
            table_id,
            column_descs,
            order_types,
            (0..group_key_indices.len()).collect(),
            0,
        ),
        store,
        None,
    )
    .await
}

/// NOTE(kwannoel): This should only be used by `test` or `bench`.
#[allow(clippy::too_many_arguments)]
pub async fn new_boxed_hash_agg_executor<S: StateStore>(
    store: S,
    input: Executor,
    is_append_only: bool,
    agg_calls: Vec<AggCall>,
    row_count_index: usize,
    group_key_indices: Vec<usize>,
    pk_indices: PkIndices,
    extreme_cache_size: usize,
    emit_on_window_close: bool,
    executor_id: u64,
) -> Executor {
    let mut storages = Vec::with_capacity(agg_calls.iter().len());
    for (idx, agg_call) in agg_calls.iter().enumerate() {
        storages.push(
            create_agg_state_storage(
                store.clone(),
                TableId::new(idx as u32),
                agg_call,
                &group_key_indices,
                &pk_indices,
                input.info.schema.fields.clone(),
                is_append_only,
            )
            .await,
        )
    }

    let intermediate_state_table = create_intermediate_state_table(
        store,
        TableId::new(agg_calls.len() as u32),
        &agg_calls,
        &group_key_indices,
        input.info.schema.fields.clone(),
    )
    .await;

    let schema = generate_agg_schema(&input, &agg_calls, Some(&group_key_indices));
    let info = ExecutorInfo {
        schema,
        pk_indices,
        identity: format!("HashAggExecutor {:X}", executor_id),
    };

    let exec = HashAggExecutor::<SerializedKey, S>::new(AggExecutorArgs {
        version: PbAggNodeVersion::LATEST,

        input,
        actor_ctx: ActorContext::for_test(123),
        info: info.clone(),

        extreme_cache_size,

        agg_calls,
        row_count_index,
        storages,
        intermediate_state_table,
        distinct_dedup_tables: Default::default(),
        watermark_epoch: Arc::new(AtomicU64::new(0)),

        extra: HashAggExecutorExtraArgs {
            group_key_indices,
            chunk_size: 1024,
            max_dirty_groups_heap_size: 64 << 20,
            emit_on_window_close,
        },
    })
    .unwrap();
    (info, exec).into()
}

#[allow(clippy::too_many_arguments)]
pub async fn new_boxed_simple_agg_executor<S: StateStore>(
    actor_ctx: ActorContextRef,
    store: S,
    input: Executor,
    is_append_only: bool,
    agg_calls: Vec<AggCall>,
    row_count_index: usize,
    pk_indices: PkIndices,
    executor_id: u64,
    must_output_per_barrier: bool,
) -> Executor {
    let storages = future::join_all(agg_calls.iter().enumerate().map(|(idx, agg_call)| {
        create_agg_state_storage(
            store.clone(),
            TableId::new(idx as u32),
            agg_call,
            &[],
            &pk_indices,
            input.info.schema.fields.clone(),
            is_append_only,
        )
    }))
    .await;

    let intermediate_state_table = create_intermediate_state_table(
        store,
        TableId::new(agg_calls.len() as u32),
        &agg_calls,
        &[],
        input.info.schema.fields.clone(),
    )
    .await;

    let schema = generate_agg_schema(&input, &agg_calls, None);
    let info = ExecutorInfo {
        schema,
        pk_indices,
        identity: format!("SimpleAggExecutor {:X}", executor_id),
    };

    let exec = SimpleAggExecutor::new(AggExecutorArgs {
        version: PbAggNodeVersion::LATEST,

        input,
        actor_ctx,
        info: info.clone(),

        extreme_cache_size: 1024,

        agg_calls,
        row_count_index,
        storages,
        intermediate_state_table,
        distinct_dedup_tables: Default::default(),
        watermark_epoch: Arc::new(AtomicU64::new(0)),
        extra: SimpleAggExecutorExtraArgs {
            must_output_per_barrier,
        },
    })
    .unwrap();
    (info, exec).into()
}
