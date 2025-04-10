use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{I64Array, Op};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;

use super::prelude::*;
use super::*;
use crate::common::table::test_utils::gen_pbtable;
use crate::executor::asof_join::{AsOfJoinExecutor, JoinParams};
use crate::executor::join::{AsOfInequalityType, AsOfJoinTypePrimitive};
use crate::executor::prelude::StreamingMetrics;
use crate::executor::{AsOfDesc, AsOfJoinType};

#[derive(Clone, Debug)]
pub struct AsOfJoinWorkload {
    // inequality upper bound of records under a join key
    upper_bound: usize,
    // Cardinality of join keys
    jk_cardinality: usize,
    // amp will be upper_bound / step_size
    step_size_l: usize,
    step_size_r: usize,
}

impl AsOfJoinWorkload {
    pub fn new(
        jk_cardinality: usize,
        upper_bound: usize,
        step_size_l: usize,
        step_size_r: usize,
    ) -> Self {
        Self {
            upper_bound,
            jk_cardinality,
            step_size_l,
            step_size_r,
        }
    }
}

pub async fn setup_bench_stream_asof_join(
    join_type: AsOfJoinTypePrimitive,
) -> (MessageSender, MessageSender, BoxedMessageStream) {
    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int64), // join key
        Field::unnamed(DataType::Int64), // asof key
        Field::unnamed(DataType::Int64), // value
    ]);

    let (tx_l, source_l) = MockSource::channel();
    let source_l = source_l.into_executor(schema.clone(), vec![1]);
    let (tx_r, source_r) = MockSource::channel();
    let source_r = source_r.into_executor(schema.clone(), vec![1]);

    // Initialize state store and tables
    let mem_store = MemoryStateStore::new();

    let data_types = vec![DataType::Int64, DataType::Int64, DataType::Int64];
    let order_types = vec![OrderType::ascending(); 2];

    let left_state =
        create_in_memory_state_table(mem_store.clone(), &data_types, &order_types, &[0, 1], 0)
            .await;

    let right_state =
        create_in_memory_state_table(mem_store.clone(), &data_types, &order_types, &[0, 1], 1)
            .await;

    let output_schema: Schema = [source_l.schema().fields(), source_r.schema().fields()]
        .concat()
        .into_iter()
        .collect();

    let info = ExecutorInfo::new(
        output_schema.clone(),
        vec![0, 1, 3, 4],
        "AsOfJoinExecutor".to_owned(),
        0,
    );

    let params_l = JoinParams::new(vec![0], vec![1]);
    let params_r = JoinParams::new(vec![0], vec![1]);

    let asof_desc = AsOfDesc {
        left_idx: 1,  // index of asof key in left table
        right_idx: 1, // index of asof key in right table
        inequality_type: AsOfInequalityType::Le,
    };

    let join_executor = match join_type {
        AsOfJoinType::Inner => AsOfJoinExecutor::<MemoryStateStore, { AsOfJoinType::Inner }>::new(
            ActorContext::for_test(123),
            info,
            source_l,
            source_r,
            params_l,
            params_r,
            (0..output_schema.fields.len()).collect_vec(),
            left_state,
            right_state,
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
            1024,
            asof_desc,
        )
        .boxed(),
        AsOfJoinType::LeftOuter => {
            AsOfJoinExecutor::<MemoryStateStore, { AsOfJoinType::LeftOuter }>::new(
                ActorContext::for_test(123),
                info,
                source_l,
                source_r,
                params_l,
                params_r,
                (0..output_schema.fields.len()).collect_vec(),
                left_state,
                right_state,
                Arc::new(AtomicU64::new(0)),
                Arc::new(StreamingMetrics::unused()),
                1024,
                asof_desc,
            )
            .boxed()
        }
        _ => panic!("Unsupported join type"),
    };

    (tx_l, tx_r, join_executor.execute())
}

async fn create_in_memory_state_table(
    mem_state: MemoryStateStore,
    data_types: &[DataType],
    order_types: &[OrderType],
    pk_indices: &[usize],
    table_id: u32,
) -> StateTable<MemoryStateStore> {
    let column_descs = data_types
        .iter()
        .enumerate()
        .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
        .collect_vec();
    StateTable::from_table_catalog(
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
    .await
}

pub async fn handle_streams(
    workload: AsOfJoinWorkload,
    join_type: AsOfJoinTypePrimitive,
    mut tx_l: MessageSender,
    mut tx_r: MessageSender,
    mut stream: BoxedMessageStream,
) {
    // Initialize with barriers
    tx_l.push_barrier(test_epoch(1), false);
    tx_r.push_barrier(test_epoch(1), false);

    // Push a single record into tx_r.
    let chunk = build_chunk(
        workload.jk_cardinality,
        workload.upper_bound,
        workload.step_size_r,
    );
    tx_r.push_chunk(chunk);

    // Ensure that the chunk on the rhs is processed, before inserting a chunk
    // into the lhs. This is to ensure that the rhs chunk is cached,
    // and we don't get interleaving of chunks between lhs and rhs.
    tx_l.push_barrier(test_epoch(2), false);
    tx_r.push_barrier(test_epoch(2), false);

    let upper_bound_factor = match join_type {
        AsOfJoinType::Inner => 1.0,
        AsOfJoinType::LeftOuter => 1.2,
        _ => panic!("Unsupported join type"),
    };
    let chunk = build_chunk(
        workload.jk_cardinality,
        (workload.upper_bound as f64 * upper_bound_factor) as usize,
        workload.step_size_l,
    );

    tx_l.push_chunk(chunk);

    match stream.next().await {
        Some(Ok(Message::Barrier(b))) => {
            assert_eq!(b.epoch.curr, test_epoch(1));
        }
        other => {
            panic!("Expected a barrier, got {:?}", other);
        }
    }

    match stream.next().await {
        Some(Ok(Message::Barrier(b))) => {
            assert_eq!(b.epoch.curr, test_epoch(2));
        }
        other => {
            panic!("Expected a barrier, got {:?}", other);
        }
    }

    let expected_count = match join_type {
        AsOfJoinType::Inner => {
            workload.jk_cardinality
                * ((workload.upper_bound - workload.step_size_r + workload.step_size_l)
                    / workload.step_size_l) as usize
        }
        AsOfJoinType::LeftOuter => {
            workload.jk_cardinality
                * ((workload.upper_bound as f64 * upper_bound_factor) as usize
                    / workload.step_size_l) as usize
        }
        _ => panic!("Unsupported join type"),
    };

    let mut current_count = 0;
    while current_count < expected_count {
        match stream.next().await {
            Some(Ok(Message::Chunk(c))) => {
                current_count += c.cardinality();
                if workload.step_size_l >= 100 {
                    // dbg!(&expected_count, &current_count);
                }
            }
            other => {
                panic!("Expected a barrier, got {:?}", other);
            }
        }
    }
}

fn build_chunk(jk_cardinality: usize, upper_bound: usize, step_size: usize) -> StreamChunk {
    let chunk_size = jk_cardinality * upper_bound / step_size;
    // Create column [0]: join key. Each record has the same value, to trigger join amplification.
    let mut int64_jk_builder = DataType::Int64.create_array_builder(chunk_size);
    let iter =
        (0..jk_cardinality as i64).flat_map(|i| std::iter::repeat_n(i, upper_bound / step_size));
    int64_jk_builder.append_array(&I64Array::from_iter(iter).into());
    let jk = int64_jk_builder.finish();

    // Create column [1]: inequality key. It will be unique.
    let mut int64_pk_data_chunk_builder = DataType::Int64.create_array_builder(chunk_size);
    let iter = (0..jk_cardinality as i64).flat_map(|_| (0..upper_bound as i64).step_by(step_size));
    int64_pk_data_chunk_builder.append_array(&I64Array::from_iter(iter).into());
    let ik: risingwave_common::array::ArrayImpl = int64_pk_data_chunk_builder.finish();

    // Create column [2]: value. This can be an arbitrary value, so just clone the ik column.
    let values = ik.clone();

    // Build the stream chunk.
    let columns = vec![jk.into(), ik.into(), values.into()];
    let ops = vec![Op::Insert; chunk_size];
    StreamChunk::new(ops, columns)
}
