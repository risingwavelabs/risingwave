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

use itertools::Itertools;
use risingwave_common::array::{I64Array, Op};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
use risingwave_common::hash::Key128;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;
use risingwave_storage::memory::MemoryStateStore;
use strum_macros::Display;

use super::*;
use crate::common::table::test_utils::gen_pbtable;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::prelude::StateTable;
use crate::executor::test_utils::{MessageSender, MockSource};
use crate::executor::{
    ActorContext, HashJoinExecutor, JoinEncoding, JoinParams, JoinType as ConstJoinType,
};

#[derive(Clone, Copy, Debug, Display)]
pub enum HashJoinWorkload {
    InCache,
    NotInCache,
}

pub async fn create_in_memory_state_table(
    mem_state: MemoryStateStore,
    data_types: &[DataType],
    order_types: &[OrderType],
    pk_indices: &[usize],
    table_id: u32,
) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
    let column_descs = data_types
        .iter()
        .enumerate()
        .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
        .collect_vec();
    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::new(table_id),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            0,
        ),
        mem_state.clone(),
        None,
    )
    .await;

    // Create degree table
    let mut degree_table_column_descs = vec![];
    pk_indices.iter().enumerate().for_each(|(pk_id, idx)| {
        degree_table_column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(pk_id as i32),
            data_types[*idx].clone(),
        ))
    });
    degree_table_column_descs.push(ColumnDesc::unnamed(
        ColumnId::new(pk_indices.len() as i32),
        DataType::Int64,
    ));
    let degree_state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::new(table_id + 1),
            degree_table_column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
            0,
        ),
        mem_state,
        None,
    )
    .await;
    (state_table, degree_state_table)
}

/// 1. Refill state table of build side.
/// 2. Init executor.
/// 3. Push data to the probe side.
/// 4. Check memory utilization.
pub async fn setup_bench_stream_hash_join(
    amp: usize,
    workload: HashJoinWorkload,
    join_type: JoinType,
) -> (MessageSender, MessageSender, BoxedMessageStream) {
    let fields = vec![DataType::Int64, DataType::Int64, DataType::Int64];
    let orders = vec![OrderType::ascending(), OrderType::ascending()];
    let state_store = MemoryStateStore::new();

    // Probe side
    let (lhs_state_table, lhs_degree_state_table) =
        create_in_memory_state_table(state_store.clone(), &fields, &orders, &[0, 1], 0).await;

    // Build side
    let (mut rhs_state_table, rhs_degree_state_table) =
        create_in_memory_state_table(state_store.clone(), &fields, &orders, &[0, 1], 2).await;

    // Insert 100K records into the build side.
    if matches!(workload, HashJoinWorkload::NotInCache) {
        let stream_chunk = build_chunk(amp, 200_000);
        // Write to state table.
        rhs_state_table.write_chunk(stream_chunk);
    }

    let schema = Schema::new(fields.iter().cloned().map(Field::unnamed).collect());

    let (tx_l, source_l) = MockSource::channel();
    let source_l = source_l.into_executor(schema.clone(), vec![1]);
    let (tx_r, source_r) = MockSource::channel();
    let source_r = source_r.into_executor(schema, vec![1]);

    // Schema is the concatenation of the two source schemas.
    // [lhs(jk):0, lhs(pk):1, lhs(value):2, rhs(jk):0, rhs(pk):1, rhs(value):2]
    // [0,         1,         2,            3,         4,         5           ]
    let schema: Vec<_> = [source_l.schema().fields(), source_r.schema().fields()]
        .concat()
        .into_iter()
        .collect();
    let schema_len = schema.len();
    let info = ExecutorInfo::new(
        Schema { fields: schema },
        vec![0, 1, 3, 4],
        "HashJoinExecutor".to_owned(),
        0,
    );

    // join-key is [0], primary-key is [1].
    let params_l = JoinParams::new(vec![0], vec![1]);
    let params_r = JoinParams::new(vec![0], vec![1]);

    let cache_size = match workload {
        HashJoinWorkload::InCache => Some(1_000_000),
        HashJoinWorkload::NotInCache => None,
    };

    match join_type {
        JoinType::Inner => {
            let executor = HashJoinExecutor::<
                Key128,
                MemoryStateStore,
                { ConstJoinType::Inner },
                { JoinEncoding::MemoryOptimized },
            >::new_with_cache_size(
                ActorContext::for_test(123),
                info,
                source_l,
                source_r,
                params_l,
                params_r,
                vec![false], // null-safe
                (0..schema_len).collect_vec(),
                None,   // condition, it is an eq join, we have no condition
                vec![], // ineq pairs
                lhs_state_table,
                lhs_degree_state_table,
                rhs_state_table,
                rhs_degree_state_table,
                Arc::new(AtomicU64::new(0)), // watermark epoch
                false,                       // is_append_only
                Arc::new(StreamingMetrics::unused()),
                1024, // chunk_size
                2048, // high_join_amplification_threshold
                cache_size,
            );
            (tx_l, tx_r, executor.boxed().execute())
        }
        JoinType::LeftOuter => {
            let executor = HashJoinExecutor::<
                Key128,
                MemoryStateStore,
                { ConstJoinType::LeftOuter },
                { JoinEncoding::MemoryOptimized },
            >::new_with_cache_size(
                ActorContext::for_test(123),
                info,
                source_l,
                source_r,
                params_l,
                params_r,
                vec![false], // null-safe
                (0..schema_len).collect_vec(),
                None,   // condition, it is an eq join, we have no condition
                vec![], // ineq pairs
                lhs_state_table,
                lhs_degree_state_table,
                rhs_state_table,
                rhs_degree_state_table,
                Arc::new(AtomicU64::new(0)), // watermark epoch
                false,                       // is_append_only
                Arc::new(StreamingMetrics::unused()),
                1024, // chunk_size
                2048, // high_join_amplification_threshold
                cache_size,
            );
            (tx_l, tx_r, executor.boxed().execute())
        }
        _ => panic!("Unsupported join type"),
    }
}

fn build_chunk(size: usize, join_key_value: i64) -> StreamChunk {
    // Create column [0]: join key. Each record has the same value, to trigger join amplification.
    let mut int64_jk_builder = DataType::Int64.create_array_builder(size);
    int64_jk_builder.append_array(&I64Array::from_iter(vec![Some(join_key_value); size]).into());
    let jk = int64_jk_builder.finish();

    // Create column [1]: pk. The original pk will be here, it will be unique.
    let mut int64_pk_data_chunk_builder = DataType::Int64.create_array_builder(size);
    let seq = I64Array::from_iter((0..size as i64).map(Some));
    int64_pk_data_chunk_builder.append_array(&I64Array::from(seq).into());
    let pk = int64_pk_data_chunk_builder.finish();

    // Create column [2]: value. This can be an arbitrary value, so just clone the pk column.
    let values = pk.clone();

    // Build the stream chunk.
    let columns = vec![jk.into(), pk.into(), values.into()];
    let ops = vec![Op::Insert; size];
    StreamChunk::new(ops, columns)
}

pub async fn handle_streams(
    hash_join_workload: HashJoinWorkload,
    join_type: JoinType,
    amp: usize,
    mut tx_l: MessageSender,
    mut tx_r: MessageSender,
    mut stream: BoxedMessageStream,
) {
    // Init executors
    tx_l.push_barrier(test_epoch(1), false);
    tx_r.push_barrier(test_epoch(1), false);

    if matches!(hash_join_workload, HashJoinWorkload::InCache) {
        // Push a single record into tx_r, so 100K records to be matched are cached.
        let chunk = build_chunk(amp, 200_000);
        tx_r.push_chunk(chunk);

        // Ensure that the chunk on the rhs is processed, before inserting a chunk
        // into the lhs. This is to ensure that the rhs chunk is cached,
        // and we don't get interleaving of chunks between lhs and rhs.
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);
    }

    // Push a chunk of records into tx_l, matches 100K records in the build side.
    let chunk_size = match hash_join_workload {
        HashJoinWorkload::InCache => 64,
        HashJoinWorkload::NotInCache => 1,
    };
    let chunk = match join_type {
        // Make sure all match
        JoinType::Inner => build_chunk(chunk_size, 200_000),
        // Make sure no match is found.
        JoinType::LeftOuter => build_chunk(chunk_size, 300_000),
        _ => panic!("Unsupported join type"),
    };
    tx_l.push_chunk(chunk);

    match stream.next().await {
        Some(Ok(Message::Barrier(b))) => {
            assert_eq!(b.epoch.curr, test_epoch(1));
        }
        other => {
            panic!("Expected a barrier, got {:?}", other);
        }
    }

    if matches!(hash_join_workload, HashJoinWorkload::InCache) {
        match stream.next().await {
            Some(Ok(Message::Barrier(b))) => {
                assert_eq!(b.epoch.curr, test_epoch(2));
            }
            other => {
                panic!("Expected a barrier, got {:?}", other);
            }
        }
    }

    let expected_count = match join_type {
        JoinType::LeftOuter => chunk_size,
        JoinType::Inner => amp * chunk_size,
        _ => panic!("Unsupported join type"),
    };
    let mut current_count = 0;
    while current_count < expected_count {
        match stream.next().await {
            Some(Ok(Message::Chunk(c))) => {
                current_count += c.cardinality();
            }
            other => {
                panic!("Expected a barrier, got {:?}", other);
            }
        }
    }
}
