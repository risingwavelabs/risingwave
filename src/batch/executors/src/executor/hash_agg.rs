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

use std::marker::PhantomData;
use std::sync::Arc;

use futures_async_stream::try_stream;
use hashbrown::hash_map::Entry;
use itertools::Itertools;
use prometheus::core::Atomic;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::bitmap::FilterByBitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    WrapStreamExecutor,
};
use crate::monitor::BatchSpillMetrics;
use crate::spill::partition::{Partitioning, SpillContext};
use crate::spill::spill_op::SpillBackend;
use crate::spill::spill_op::SpillBackend::Disk;
use crate::task::{ShutdownToken, TaskId};

type AggHashMap<K, A> = hashbrown::HashMap<K, Vec<AggregateState>, PrecomputedBuildHasher, A>;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(
            Arc::new(self.aggs),
            self.group_key_columns,
            self.group_key_types,
            self.schema,
            self.child,
            self.identity,
            self.chunk_size,
            self.mem_context,
            self.spill_backend,
            self.spill_metrics,
            self.shutdown_rx,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder {
    aggs: Vec<BoxedAggregateFunction>,
    group_key_columns: Vec<usize>,
    group_key_types: Vec<DataType>,
    child: BoxedExecutor,
    schema: Schema,
    #[expect(dead_code)]
    task_id: TaskId,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
    shutdown_rx: ShutdownToken,
}

impl HashAggExecutorBuilder {
    fn deserialize(
        hash_agg_node: &HashAggNode,
        child: BoxedExecutor,
        task_id: TaskId,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        shutdown_rx: ShutdownToken,
    ) -> Result<BoxedExecutor> {
        let aggs: Vec<_> = hash_agg_node
            .get_agg_calls()
            .iter()
            .map(|agg| AggCall::from_protobuf(agg).and_then(|agg| build_agg(&agg)))
            .try_collect()?;

        let group_key_columns = hash_agg_node
            .get_group_key()
            .iter()
            .map(|x| *x as usize)
            .collect_vec();

        let child_schema = child.schema();

        let group_key_types = group_key_columns
            .iter()
            .map(|i| child_schema.fields[*i].data_type.clone())
            .collect_vec();

        let fields = group_key_types
            .iter()
            .cloned()
            .chain(aggs.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let builder = HashAggExecutorBuilder {
            aggs,
            group_key_columns,
            group_key_types,
            child,
            schema: Schema { fields },
            task_id,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            shutdown_rx,
        };

        Ok(builder.dispatch())
    }
}

impl BoxedExecutorBuilder for HashAggExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hash_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HashAgg
        )?;

        let identity = source.plan_node().get_identity();

        let spill_metrics = source.context().spill_metrics();

        Self::deserialize(
            hash_agg_node,
            child,
            source.task_id.clone(),
            identity.clone(),
            source.context().get_config().developer.chunk_size,
            source.context().create_executor_mem_context(identity),
            if source.context().get_config().enable_spill {
                Some(Disk)
            } else {
                None
            },
            spill_metrics,
            source.shutdown_rx().clone(),
        )
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub struct HashAggExecutor<K> {
    /// Aggregate functions.
    aggs: Arc<Vec<BoxedAggregateFunction>>,
    /// Column indexes that specify a group
    group_key_columns: Vec<usize>,
    /// Data types of group key columns
    group_key_types: Vec<DataType>,
    /// Output schema
    schema: Schema,
    child: BoxedExecutor,
    /// Used to initialize the state of the aggregation from the spilled files.
    init_agg_state_executor: Option<BoxedExecutor>,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill: SpillContext,
    shutdown_rx: ShutdownToken,
    _phantom: PhantomData<K>,
}

impl<K> HashAggExecutor<K> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        aggs: Arc<Vec<BoxedAggregateFunction>>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill_backend: Option<SpillBackend>,
        spill_metrics: Arc<BatchSpillMetrics>,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        Self::new_inner(
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            None,
            identity,
            chunk_size,
            mem_context,
            SpillContext::new(spill_backend, spill_metrics),
            shutdown_rx,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn new_inner(
        aggs: Arc<Vec<BoxedAggregateFunction>>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        init_agg_state_executor: Option<BoxedExecutor>,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        spill: SpillContext,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        HashAggExecutor {
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            init_agg_state_executor,
            identity,
            chunk_size,
            mem_context,
            spill,
            shutdown_rx,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey + Send + Sync> Executor for HashAggExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // Keep this partition's charges separate from the shared parent. Its private context
        // releases remaining state charges on completion, errors, or cancellation.
        let mem_context = MemoryContext::new(Some(self.mem_context.clone()), TrAdderAtomic::new(0));
        let child_schema = self.child.schema().clone();
        let mut need_to_spill = false;

        // Track only this executor's manual charges; spill partitions share the parent counter.
        let mut states_heap_size = 0;
        // hash map for each agg groups
        let mut groups = AggHashMap::<K, _>::with_hasher_in(
            PrecomputedBuildHasher,
            mem_context.global_allocator(),
        );

        if let Some(init_agg_state_executor) = self.init_agg_state_executor {
            // `init_agg_state_executor` exists which means this is a sub `HashAggExecutor` used to consume spilling data.
            // The spilled agg states by its parent executor need to be recovered first.
            let mut init_agg_state_stream = init_agg_state_executor.execute();
            #[for_await]
            for chunk in &mut init_agg_state_stream {
                let chunk = chunk?;
                let group_key_indices = (0..self.group_key_columns.len()).collect_vec();
                let keys = K::build_many(&group_key_indices, &chunk);
                let mut memory_usage_diff = 0;
                for (row_id, key) in keys.into_iter().enumerate() {
                    let mut agg_states = vec![];
                    for i in 0..self.aggs.len() {
                        let agg = &self.aggs[i];
                        let datum = chunk
                            .row_at(row_id)
                            .0
                            .datum_at(self.group_key_columns.len() + i)
                            .to_owned_datum();
                        let agg_state = agg.decode_state(datum)?;
                        memory_usage_diff += agg_state.estimated_size() as i64;
                        agg_states.push(agg_state);
                    }
                    groups.try_insert(key, agg_states).unwrap();
                }

                // Restored states are retained even when loading the partition exceeds the limit.
                states_heap_size += memory_usage_diff;
                mem_context.add_unchecked(memory_usage_diff);
                if self.spill.over_budget(&mem_context) {
                    warn!(
                        "not enough memory to load one partition agg state after spill which is not a normal case, so keep going"
                    );
                }
            }
        }

        let mut input_stream = self.child.execute();
        // consume all chunks to compute the agg result
        #[for_await]
        for chunk in &mut input_stream {
            let chunk = StreamChunk::from(chunk?);
            let keys = K::build_many(self.group_key_columns.as_slice(), &chunk);
            let mut memory_usage_diff = 0;
            for (row_id, key) in keys
                .into_iter()
                .enumerate()
                .filter_by_bitmap(chunk.visibility())
            {
                let mut new_group = false;
                let states = match groups.entry(key) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        new_group = true;
                        let states = self
                            .aggs
                            .iter()
                            .map(|agg| agg.create_state())
                            .try_collect()?;
                        entry.insert(states)
                    }
                };

                // TODO: currently not a vectorized implementation
                for (agg, state) in self.aggs.iter().zip_eq_fast(states) {
                    if !new_group {
                        memory_usage_diff -= state.estimated_size() as i64;
                    }
                    agg.update_range(state, &chunk, row_id..row_id + 1).await?;
                    memory_usage_diff += state.estimated_size() as i64;
                }
            }
            // States have already been updated and must remain charged until they are released.
            states_heap_size += memory_usage_diff;
            mem_context.add_unchecked(memory_usage_diff);
            if self.spill.should_spill(&mem_context)? {
                need_to_spill = true;
                break;
            }
        }

        if need_to_spill {
            // A spilling version of aggregation based on the RFC: Spill Hash Aggregation https://github.com/risingwavelabs/rfcs/pull/89
            // The agg states and the remaining input are partitioned by group key, so each
            // partition holds whole groups and is aggregated by a sub `HashAggExecutor`, which may
            // spill again.
            const AGG_STATE: usize = 0;
            const INPUT: usize = 1;
            let state_types = self.schema.data_types();
            let mut spill = (self.spill)
                .start(
                    &self.identity,
                    self.chunk_size,
                    [
                        (
                            state_types.clone(),
                            Partitioning::Hash((0..self.group_key_columns.len()).collect()),
                        ),
                        (
                            child_schema.data_types(),
                            Partitioning::Hash(self.group_key_columns.clone()),
                        ),
                    ],
                )
                .await?;

            let mut memory_usage_diff = 0;
            let mut state_builder = DataChunkBuilder::new(state_types, self.chunk_size);
            for (key, states) in groups {
                let key_row = key.deserialize(&self.group_key_types)?;
                let mut agg_datums = vec![];
                for (agg, state) in self.aggs.iter().zip_eq_fast(states) {
                    let encode_state = agg.encode_state(&state)?;
                    memory_usage_diff -= state.estimated_size() as i64;
                    agg_datums.push(encode_state);
                }
                let agg_state_row = OwnedRow::from_iter(agg_datums.into_iter());
                if let Some(chunk) = state_builder.append_one_row(key_row.chain(agg_state_row)) {
                    spill.write(AGG_STATE, chunk).await?;
                }
            }
            if let Some(chunk) = state_builder.consume_all() {
                spill.write(AGG_STATE, chunk).await?;
            }

            // Release memory occupied by agg hash map.
            debug_assert_eq!(memory_usage_diff, -states_heap_size);
            mem_context.add_unchecked(memory_usage_diff);

            #[for_await]
            for chunk in input_stream {
                spill.write(INPUT, chunk?).await?;
            }
            let partitions = spill.finish().await?;

            for i in 0..partitions.partition_num() {
                let ([agg_state_stream, input_stream], spill) = partitions.read(i).await?;
                let sub_hash_agg_executor: HashAggExecutor<K> = HashAggExecutor::new_inner(
                    self.aggs.clone(),
                    self.group_key_columns.clone(),
                    self.group_key_types.clone(),
                    self.schema.clone(),
                    Box::new(WrapStreamExecutor::new(child_schema.clone(), input_stream)),
                    Some(Box::new(WrapStreamExecutor::new(
                        self.schema.clone(),
                        agg_state_stream,
                    ))),
                    format!("{}-sub{}", self.identity.clone(), i),
                    self.chunk_size,
                    self.mem_context.clone(),
                    spill,
                    self.shutdown_rx.clone(),
                );

                #[for_await]
                for chunk in Box::new(sub_hash_agg_executor).execute() {
                    yield chunk?;
                }

                partitions.clear(i).await?;
            }
        } else {
            // Don't use `into_iter` here, it may cause memory leak.
            let mut result = groups.iter_mut();
            let cardinality = self.chunk_size;
            loop {
                let mut group_builders: Vec<_> = self
                    .group_key_types
                    .iter()
                    .map(|datatype| datatype.create_array_builder(cardinality))
                    .collect();

                let mut agg_builders: Vec<_> = self
                    .aggs
                    .iter()
                    .map(|agg| agg.return_type().create_array_builder(cardinality))
                    .collect();

                let mut has_next = false;
                let mut array_len = 0;
                for (key, states) in result.by_ref().take(cardinality) {
                    self.shutdown_rx.check()?;
                    has_next = true;
                    array_len += 1;
                    key.deserialize_to_builders(&mut group_builders[..], &self.group_key_types)?;
                    for ((agg, state), builder) in (self.aggs.iter())
                        .zip_eq_fast(states)
                        .zip_eq_fast(&mut agg_builders)
                    {
                        let result = agg.get_result(state).await?;
                        builder.append(result);
                    }
                }
                if !has_next {
                    break; // exit loop
                }

                let columns = group_builders
                    .into_iter()
                    .chain(agg_builders)
                    .map(|b| b.finish().into())
                    .collect::<Vec<_>>();

                let output = DataChunk::new(columns, array_len);
                yield output;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{AllocError, Allocator, Global, Layout};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};

    use allocator_api2::alloc::{AllocError as AllocErrorApi2, Allocator as AllocatorApi2};
    use futures_async_stream::for_await;
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
    use risingwave_pb::expr::{AggCall, InputRef};

    use super::*;
    use crate::executor::SortExecutor;
    use crate::executor::test_utils::{MockExecutor, diff_executor_output};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn execute_int32_grouped() {
        let parent_mem = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
        {
            let src_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i i
                 0 1 1
                 1 1 1
                 0 0 1
                 1 1 2
                 1 0 1
                 0 0 2
                 1 1 3
                 0 1 2",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));

            let agg_call = AggCall {
                kind: PbAggKind::Sum as i32,
                args: vec![InputRef {
                    index: 2,
                    r#type: Some(PbDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                }],
                return_type: Some(PbDataType {
                    type_name: TypeName::Int64 as i32,
                    ..Default::default()
                }),
                distinct: false,
                order_by: vec![],
                filter: None,
                direct_args: vec![],
                udf: None,
                scalar: None,
            };

            let agg_prost = HashAggNode {
                group_key: vec![0, 1],
                agg_calls: vec![agg_call],
            };

            let mem_context = MemoryContext::new(
                Some(parent_mem.clone()),
                LabelGuardedIntGauge::test_int_gauge::<4>(),
            );
            let actual_exec = HashAggExecutorBuilder::deserialize(
                &agg_prost,
                src_exec,
                TaskId::default(),
                "HashAggExecutor".to_owned(),
                CHUNK_SIZE,
                mem_context.clone(),
                None,
                BatchSpillMetrics::for_test(),
                ShutdownToken::empty(),
            )
            .unwrap();

            // TODO: currently the order is fixed unless the hasher is changed
            let expect_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i I
                 1 0 1
                 0 0 3
                 0 1 3
                 1 1 6",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));
            diff_executor_output(actual_exec, expect_exec).await;

            // Finishing a partition promptly clears its memory usage accounting.
            assert_eq!(mem_context.get_bytes_used(), 0);
        }

        // Ensure that agg memory counter has been dropped.
        assert_eq!(0, parent_mem.get_bytes_used());
    }

    #[tokio::test]
    async fn execute_count_star() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i
                 0
                 1
                 0
                 1
                 1
                 0
                 1
                 0",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32)]),
        );

        let agg_call = AggCall {
            kind: PbAggKind::Count as i32,
            args: vec![],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
        )
        .unwrap();

        let expect_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "I
                 8",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int64)]),
        );
        diff_executor_output(actual_exec, Box::new(expect_exec)).await;
    }

    /// A test to verify that `HashMap` may leak memory counter when using `into_iter`.
    #[test]
    #[should_panic] // TODO(MrCroxx): This bug is fixed and the test should panic. Remove the test and fix the related code later.
    fn test_hashmap_into_iter_bug() {
        let dropped: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        {
            struct MyAllocInner {
                drop_flag: Arc<AtomicBool>,
            }

            #[derive(Clone)]
            struct MyAlloc {
                #[expect(dead_code)]
                inner: Arc<MyAllocInner>,
            }

            impl Drop for MyAllocInner {
                fn drop(&mut self) {
                    println!("MyAlloc freed.");
                    self.drop_flag.store(true, Ordering::SeqCst);
                }
            }

            unsafe impl Allocator for MyAlloc {
                fn allocate(
                    &self,
                    layout: Layout,
                ) -> std::result::Result<NonNull<[u8]>, AllocError> {
                    let g = Global;
                    g.allocate(layout)
                }

                unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                    unsafe {
                        let g = Global;
                        g.deallocate(ptr, layout)
                    }
                }
            }

            unsafe impl AllocatorApi2 for MyAlloc {
                fn allocate(
                    &self,
                    layout: Layout,
                ) -> std::result::Result<NonNull<[u8]>, AllocErrorApi2> {
                    let g = Global;
                    g.allocate(layout).map_err(|_| AllocErrorApi2)
                }

                unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                    unsafe {
                        let g = Global;
                        g.deallocate(ptr, layout)
                    }
                }
            }

            let mut map = hashbrown::HashMap::with_capacity_in(
                10,
                MyAlloc {
                    inner: Arc::new(MyAllocInner {
                        drop_flag: dropped.clone(),
                    }),
                },
            );
            for i in 0..10 {
                map.entry(i).or_insert_with(|| "i".to_owned());
            }

            for (k, v) in map {
                println!("{}, {}", k, v);
            }
        }

        assert!(!dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i i
                 0 1 1",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32); 3]),
        );

        let agg_call = AggCall {
            kind: PbAggKind::Sum as i32,
            args: vec![InputRef {
                index: 2,
                r#type: Some(PbDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
            shutdown_rx,
        )
        .unwrap();

        shutdown_tx.cancel();

        #[for_await]
        for data in actual_exec.execute() {
            assert!(data.is_err());
            break;
        }
    }

    fn create_order_by_executor(child: BoxedExecutor) -> BoxedExecutor {
        let column_orders = child
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, _)| ColumnOrder {
                column_index: i,
                order_type: OrderType::ascending(),
            })
            .collect_vec();

        Box::new(SortExecutor::new(
            child,
            Arc::new(column_orders),
            "SortExecutor".into(),
            CHUNK_SIZE,
            MemoryContext::none(),
            None,
            BatchSpillMetrics::for_test(),
        ))
    }

    #[tokio::test]
    async fn test_spill_hash_agg() {
        let mut src_exec = MockExecutor::new(Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]));
        // The executor spills its states after the first chunk. The second chunk updates the same
        // groups from the spilled input, so both must land in the same partition.
        src_exec.add(DataChunk::from_pretty(
            "i i i
             0 1 1
             1 1 1
             0 0 1
             1 1 2",
        ));
        src_exec.add(DataChunk::from_pretty(
            "i i i
             1 0 1
             0 0 2
             1 1 3
             0 1 2",
        ));
        let src_exec = Box::new(src_exec);

        let agg_call = AggCall {
            kind: PbAggKind::Sum as i32,
            args: vec![InputRef {
                index: 2,
                r#type: Some(PbDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
            udf: None,
            scalar: None,
        };

        let agg_prost = HashAggNode {
            group_key: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let mem_context =
            MemoryContext::new_with_mem_limit(None, LabelGuardedIntGauge::test_int_gauge::<4>(), 0);
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            src_exec,
            TaskId::default(),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            mem_context.clone(),
            Some(SpillBackend::Memory),
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
        )
        .unwrap();

        let actual_exec = create_order_by_executor(actual_exec);

        let expect_exec = Box::new(MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i I
                 1 0 1
                 0 0 3
                 0 1 3
                 1 1 6",
            ),
            Schema::new(vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int64),
            ]),
        ));

        let expect_exec = create_order_by_executor(expect_exec);
        diff_executor_output(actual_exec, expect_exec).await;
    }
}
