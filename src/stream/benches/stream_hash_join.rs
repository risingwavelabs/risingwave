// Copyright 2024 RisingWave Labs
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

#![feature(let_chains)]

use std::env;

use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{I64Array, Op, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::Key128;
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::test_epoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::hash_join_executor::*;
use risingwave_stream::executor::test_utils::*;
use risingwave_stream::executor::{ExecutorInfo, HashJoinExecutor, JoinParams, JoinType, Message};

use crate::prelude::*;

risingwave_expr_impl::enable!();

/// 1. Refill state table of build side.
/// 2. Init executor.
/// 3. Push data to the probe side.
/// 4. Check memory utilization.
async fn setup_bench_stream_hash_join(
    amp: usize,
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
    {
        // Create column [0]: join key. Each record has the same value, to trigger join amplification.
        let mut int64_jk_builder = DataType::Int64.create_array_builder(amp);
        int64_jk_builder
            .append_array(&I64Array::from_iter(vec![Some(200_000); amp].into_iter()).into());
        let jk = int64_jk_builder.finish();

        // Create column [1]: pk. The original pk will be here, it will be unique.
        let mut int64_pk_data_chunk_builder = DataType::Int64.create_array_builder(amp);
        let seq = I64Array::from_iter((0..amp as i64).map(|i| Some(i)));
        int64_pk_data_chunk_builder.append_array(&I64Array::from(seq).into());
        let pk = int64_pk_data_chunk_builder.finish();

        // Create column [2]: value. This can be an arbitrary value, so just clone the pk column.
        let values = pk.clone();

        // Build the stream chunk.
        let columns = vec![jk.into(), pk.into(), values.into()];
        let ops = vec![Op::Insert; amp];
        let stream_chunk = StreamChunk::new(ops, columns);

        // Write to state table.
        rhs_state_table.write_chunk(stream_chunk);
    }

    let schema = Schema::new(
        fields
            .iter()
            .cloned()
            .map(|data_type| Field::unnamed(data_type))
            .collect(),
    );

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
    let info = ExecutorInfo {
        schema: Schema { fields: schema },
        pk_indices: vec![0, 1, 3, 4],
        identity: "HashJoinExecutor".to_string(),
    };

    // join-key is [0], primary-key is [1].
    let params_l = JoinParams::new(vec![0], vec![1]);
    let params_r = JoinParams::new(vec![0], vec![1]);

    let executor = HashJoinExecutor::<Key128, MemoryStateStore, { JoinType::Inner }>::new(
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
    );
    (tx_l, tx_r, executor.boxed().execute())
}

async fn handle_streams(
    amp: usize,
    mut tx_l: MessageSender,
    mut tx_r: MessageSender,
    mut stream: BoxedMessageStream,
) {
    // Init executors
    tx_l.push_barrier(test_epoch(1), false);
    tx_r.push_barrier(test_epoch(1), false);
    // Push a single record into tx_l, matches 100K records in the build side.
    let chunk = StreamChunk::from_pretty(
        "  I      I I
         + 200000 0 1",
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

    let chunks = amp / 1024;
    let remainder = amp % 1024;

    for _ in 0..chunks {
        match stream.next().await {
            Some(Ok(Message::Chunk(c))) => {
                assert_eq!(c.cardinality(), 1024);
            }
            other => {
                panic!("Expected a barrier, got {:?}", other);
            }
        }
    }

    match stream.next().await {
        Some(Ok(Message::Chunk(c))) => {
            assert_eq!(c.cardinality(), remainder);
        }
        other => {
            panic!("Expected a barrier, got {:?}", other);
        }
    }
}

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[tokio::main]
async fn main() {
    let args: Vec<_> = env::args().collect();
    let amp = if let Some(raw_arg) = args.get(1)
        && let Ok(arg) = raw_arg.parse()
    {
        arg
    } else {
        100_000
    };
    let (tx_l, tx_r, out) = setup_bench_stream_hash_join(amp).await;
    {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        handle_streams(amp, tx_l, tx_r, out).await;
    }
}
