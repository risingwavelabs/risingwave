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
use risingwave_common::bitmap::{BitmapBuilder, FilterByBitmap};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::aggregate::{AggCall, AggregateState, BoxedAggregateFunction};
use risingwave_pb::batch_plan::HashAggNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::monitor::BatchSpillMetrics;
use crate::spill::MIN_WORKING_SET_BYTES;
use crate::spill::partition::{PENDING_BYTES, PartitionWriter, read_coalesced};
use crate::spill::spill_file::{SpillDir, SpillFile};
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
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    spill_backend: Option<SpillBackend>,
    spill_metrics: Arc<BatchSpillMetrics>,
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
        HashAggExecutor {
            aggs,
            group_key_columns,
            group_key_types,
            schema,
            child,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
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

/// The inputs of a spilled partition: input rows, and states that a pass wrote out with their group
/// keys.
const ROWS: usize = 0;
const STATES: usize = 1;

/// Level from which a pass whose states keep growing beyond the memory limit fails instead of
/// writing them out again, so that splitting terminates even if groups keep sharing partitions.
const MAX_LEVELS: u32 = 32;

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    /// Aggregates the input in passes. A pass creates groups until the memory limit is exceeded,
    /// then admits no more: rows of its groups keep updating them, and rows of other groups are
    /// written to partitions by their group key. When its input ends, the pass outputs its groups,
    /// and each partition becomes the input of another pass.
    ///
    /// The states of admitted groups may keep growing, as those of `string_agg` or DISTINCT do. When
    /// they grow while the memory limit is exceeded, the pass writes them to the partitions of their
    /// groups and sends all its later rows there too, and the next passes resume them. That needs
    /// states that can be encoded and more than one group: otherwise the pass fails with
    /// `OutOfMemory`.
    ///
    /// Up to [`MIN_WORKING_SET_BYTES`] of the executor's memory, or of the states for their growth,
    /// is used regardless of the limit, so that spilling makes progress when other queries hold
    /// the whole budget.
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            aggs,
            group_key_columns,
            group_key_types,
            child,
            identity,
            chunk_size,
            mem_context,
            spill_backend,
            spill_metrics,
            shutdown_rx,
            ..
        } = *self;
        // A private context releases the remaining charges on completion, errors and cancellation.
        let mem_context = MemoryContext::new(Some(mem_context), TrAdderAtomic::new(0));
        let input_data_types = child.schema().data_types();
        // An encodable state is a datum of its aggregate's return type.
        let states_encodable = aggs
            .iter()
            .all(|agg| matches!(agg.create_state(), Ok(AggregateState::Datum(_))));
        let state_key_columns = (0..group_key_types.len()).collect_vec();
        let state_data_types = group_key_types
            .iter()
            .cloned()
            .chain(aggs.iter().map(|agg| agg.return_type()))
            .collect_vec();
        let start_spilling = |spill_dir: &mut Option<SpillDir>, level: u32| {
            let Some(spill_backend) = &spill_backend else {
                return Err(BatchError::OutOfMemory(mem_context.mem_limit()));
            };
            if spill_dir.is_none() {
                info!(identity, "batch hash agg executor starts to spill");
                *spill_dir = Some(SpillDir::create(
                    spill_backend.clone(),
                    spill_metrics.clone(),
                )?);
            }
            PartitionWriter::new(
                spill_dir.as_ref().unwrap(),
                level,
                [group_key_columns.clone(), state_key_columns.clone()],
                [input_data_types.clone(), state_data_types.clone()],
                chunk_size,
                &mem_context,
            )
        };
        let mut spill_dir = None;
        // Spilled partitions left to aggregate, with the spills that produced each.
        let mut partitions: Vec<([SpillFile; 2], u32)> = vec![];
        let mut states_input: Option<BoxedDataChunkStream> = None;
        let mut input = child.execute();
        let mut level = 0;
        let over_limit = || {
            mem_context.get_bytes_used() as u64 > MIN_WORKING_SET_BYTES
                && !mem_context.check_memory_usage()
        };
        loop {
            let mut states_heap_size = 0;
            let mut groups = AggHashMap::<K, _>::with_hasher_in(
                PrecomputedBuildHasher,
                mem_context.global_allocator(),
            );
            // Set once the pass stops admitting groups.
            let mut writer: Option<PartitionWriter<2>> = None;

            if let Some(states) = states_input.take() {
                #[for_await]
                for chunk in states {
                    let chunk = chunk?;
                    let keys = K::build_many(&state_key_columns, &chunk);
                    let mut rejected = writer
                        .is_some()
                        .then(|| BitmapBuilder::zeroed(chunk.capacity()));
                    let mut memory_usage_diff = 0;
                    for (row_id, key) in keys
                        .into_iter()
                        .enumerate()
                        .filter_by_bitmap(chunk.visibility())
                    {
                        if let Some(rejected) = &mut rejected {
                            rejected.set(row_id, true);
                            continue;
                        }
                        let row = chunk.row_at_unchecked_vis(row_id);
                        let states: Vec<_> = (aggs.iter().enumerate())
                            .map(|(i, agg)| {
                                agg.decode_state(
                                    row.datum_at(state_key_columns.len() + i).to_owned_datum(),
                                )
                            })
                            .try_collect()?;
                        memory_usage_diff += states
                            .iter()
                            .map(|state| state.estimated_size() as i64)
                            .sum::<i64>();
                        // A pass writes out each group's states at most once.
                        groups.try_insert(key, states).unwrap();
                    }
                    states_heap_size += memory_usage_diff;
                    mem_context.add_unchecked(memory_usage_diff);

                    if let Some(writer) = &mut writer {
                        let rejected = chunk.with_visibility(rejected.unwrap().finish());
                        writer.write(STATES, &rejected).await?;
                    } else if over_limit() {
                        writer = Some(start_spilling(&mut spill_dir, level)?);
                    }
                }
            }

            #[for_await]
            for chunk in &mut input {
                let chunk = StreamChunk::from(chunk?);
                let keys = K::build_many(group_key_columns.as_slice(), &chunk);
                let mut rejected = writer
                    .is_some()
                    .then(|| BitmapBuilder::zeroed(chunk.capacity()));
                let mut memory_usage_diff = 0;
                for (row_id, key) in keys
                    .into_iter()
                    .enumerate()
                    .filter_by_bitmap(chunk.visibility())
                {
                    let (states, new_group) = match &mut rejected {
                        Some(rejected) => match groups.get_mut(&key) {
                            Some(states) => (states, false),
                            None => {
                                rejected.set(row_id, true);
                                continue;
                            }
                        },
                        None => match groups.entry(key) {
                            Entry::Occupied(entry) => (entry.into_mut(), false),
                            Entry::Vacant(entry) => {
                                let states =
                                    aggs.iter().map(|agg| agg.create_state()).try_collect()?;
                                (entry.insert(states), true)
                            }
                        },
                    };

                    // TODO: currently not a vectorized implementation
                    for (agg, state) in aggs.iter().zip_eq_fast(states) {
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

                if let Some(writer) = &mut writer {
                    let rejected = chunk
                        .data_chunk()
                        .with_visibility(rejected.unwrap().finish());
                    writer.write(ROWS, &rejected).await?;
                    if memory_usage_diff > 0
                        && states_heap_size as u64 > MIN_WORKING_SET_BYTES
                        && !mem_context.check_memory_usage()
                    {
                        if !states_encodable || groups.len() <= 1 || level >= MAX_LEVELS {
                            return Err(BatchError::OutOfMemory(mem_context.mem_limit()));
                        }
                        // With no groups left, the pass rejects all its later rows.
                        let mut builders = vec![];
                        let (mut rows, mut bytes) = (0, 0);
                        for (key, states) in &groups {
                            if rows == 0 {
                                builders = (state_data_types.iter())
                                    .map(|data_type| data_type.create_array_builder(chunk_size))
                                    .collect_vec();
                            }
                            let (key_builders, state_builders) =
                                builders.split_at_mut(state_key_columns.len());
                            key.deserialize_to_builders(key_builders, &group_key_types)?;
                            for ((agg, state), builder) in
                                aggs.iter().zip_eq_fast(states).zip_eq_fast(state_builders)
                            {
                                bytes += state.estimated_size();
                                builder.append(agg.encode_state(state)?);
                            }
                            rows += 1;
                            if rows == chunk_size || bytes >= PENDING_BYTES {
                                let columns = builders.drain(..).map(|b| b.finish().into());
                                let states_chunk = DataChunk::new(columns.collect(), rows);
                                writer.write(STATES, &states_chunk).await?;
                                (rows, bytes) = (0, 0);
                            }
                        }
                        if rows > 0 {
                            let columns = builders.drain(..).map(|b| b.finish().into());
                            let states_chunk = DataChunk::new(columns.collect(), rows);
                            writer.write(STATES, &states_chunk).await?;
                        }
                        groups = AggHashMap::<K, _>::with_hasher_in(
                            PrecomputedBuildHasher,
                            mem_context.global_allocator(),
                        );
                        mem_context.add_unchecked(-states_heap_size);
                        states_heap_size = 0;
                    }
                } else if over_limit() {
                    writer = Some(start_spilling(&mut spill_dir, level)?);
                }
            }

            // Don't use `into_iter` here, it may cause memory leak.
            let mut result = groups.iter_mut();
            loop {
                let mut group_builders: Vec<_> = group_key_types
                    .iter()
                    .map(|datatype| datatype.create_array_builder(chunk_size))
                    .collect();

                let mut agg_builders: Vec<_> = aggs
                    .iter()
                    .map(|agg| agg.return_type().create_array_builder(chunk_size))
                    .collect();

                let mut has_next = false;
                let mut array_len = 0;
                for (key, states) in result.by_ref().take(chunk_size) {
                    shutdown_rx.check()?;
                    has_next = true;
                    array_len += 1;
                    key.deserialize_to_builders(&mut group_builders[..], &group_key_types)?;
                    for ((agg, state), builder) in (aggs.iter())
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
            drop(groups);
            mem_context.add_unchecked(-states_heap_size);

            if let Some(writer) = writer {
                for files in writer.finish().await? {
                    if files.iter().any(|file| file.rows() > 0) {
                        partitions.push((files, level + 1));
                    }
                }
            }
            let Some(([rows, states], partition_level)) = partitions.pop() else {
                break;
            };
            states_input = (states.rows() > 0).then(|| {
                read_coalesced(
                    states,
                    state_data_types.clone(),
                    chunk_size,
                    mem_context.clone(),
                )
            });
            input = read_coalesced(
                rows,
                input_data_types.clone(),
                chunk_size,
                mem_context.clone(),
            );
            level = partition_level;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{AllocError, Allocator, Global, Layout};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};

    use allocator_api2::alloc::{AllocError as AllocErrorApi2, Allocator as AllocatorApi2};
    use futures::StreamExt;
    use futures_async_stream::for_await;
    use risingwave_common::array::{Array, I32Array, I64Array, Utf8Array};
    use risingwave_common::metrics::LabelGuardedIntGauge;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
    use risingwave_pb::expr::{AggCall, InputRef};

    use super::*;
    use crate::executor::SortExecutor;
    use crate::executor::test_utils::{MockExecutor, PeakCounter, diff_executor_output};

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
        // Less than the minimum working set is aggregated in memory even with no memory budget.
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

    /// Output rows of `calls` over `input` grouped by its first column, sorted. Without
    /// `mem_context` the aggregation cannot spill.
    async fn spill_test_agg(
        calls: &[&str],
        input: &[DataChunk],
        mem_context: Option<MemoryContext>,
    ) -> Result<Vec<String>> {
        let aggs: Vec<_> = calls
            .iter()
            .map(|call| build_agg(&risingwave_expr::aggregate::AggCall::from_pretty(call)).unwrap())
            .collect();
        let fields = std::iter::once(Field::unnamed(DataType::Int32))
            .chain(aggs.iter().map(|agg| Field::unnamed(agg.return_type())))
            .collect();
        let mut child = MockExecutor::new(Schema::new(
            input[0]
                .columns()
                .iter()
                .map(|column| Field::unnamed(column.data_type()))
                .collect(),
        ));
        for chunk in input {
            child.add(chunk.clone());
        }
        let executor = Box::new(HashAggExecutor::<risingwave_common::hash::Key32>::new(
            Arc::new(aggs),
            vec![0],
            vec![DataType::Int32],
            Schema { fields },
            Box::new(child),
            "HashAggExecutor".to_owned(),
            CHUNK_SIZE,
            mem_context.clone().unwrap_or_else(MemoryContext::none),
            mem_context.is_some().then_some(SpillBackend::Memory),
            BatchSpillMetrics::for_test(),
            ShutdownToken::empty(),
        ));
        let mut rows = vec![];
        let mut stream = executor.execute();
        while let Some(chunk) = stream.next().await {
            rows.extend(chunk?.rows().map(|row| format!("{:?}", row.to_owned_row())));
        }
        rows.sort();
        Ok(rows)
    }

    /// Aggregates `input` in memory and with a memory limit of 0, and checks that both give the
    /// same groups. With no memory, a pass stops admitting groups once the executor holds the
    /// minimum working set.
    async fn check_spilled_agg(calls: &[&str], input: &[DataChunk]) {
        let expected = spill_test_agg(calls, input, None).await.unwrap();
        let actual = spill_test_agg(calls, input, Some(MemoryContext::for_spill_test()))
            .await
            .unwrap();
        assert_eq!(actual, expected, "{calls:?}");
    }

    /// Aggregates whose states cannot be encoded spill too, as long as the groups that a pass
    /// admitted stop growing once it admits no more. Here each input chunk holds whole groups.
    #[tokio::test]
    async fn test_spill_hash_agg_any_aggregate() {
        let input = (0..200)
            .map(|chunk| {
                let rows = 0..256i64;
                let row = |r: i64| chunk * 256 + r;
                DataChunk::new(
                    vec![
                        I32Array::from_iter(rows.clone().map(|r| {
                            (chunk != 3 || r < 224).then_some((chunk * 8 + r / 32) as i32)
                        }))
                        .into_ref(),
                        I32Array::from_iter(
                            rows.clone()
                                .map(|r| (row(r) % 5 != 0).then_some((row(r) % 17) as i32)),
                        )
                        .into_ref(),
                        I64Array::from_iter(rows.map(|r| Some(row(r)))).into_ref(),
                    ],
                    256,
                )
            })
            .collect_vec();
        for call in [
            "(count:int8 $1:int4)",
            "(sum:int8 $1:int4)",
            "(count:int8 $1:int4 distinct)",
            "(sum:int8 $1:int4 distinct)",
            "(array_agg:int4[] $1:int4 orderby $2:asc)",
            "(array_agg:int8[] $2:int8 orderby $2:desc)",
            "(first_value:int8 $2:int8 orderby $2:asc)",
            "(max:int4 $1:int4)",
        ] {
            check_spilled_agg(&[call], &input).await;
        }
    }

    /// Input of `chunks` chunks: a group key of `groups` values interleaved row by row, a string of
    /// `width` bytes, and a comma.
    fn growing_states_input(chunks: i32, groups: i32, width: usize) -> Vec<DataChunk> {
        let value = "v".repeat(width);
        (0..chunks)
            .map(|chunk| {
                let rows = (0..1024).map(|row| chunk * 1024 + row);
                DataChunk::new(
                    vec![
                        I32Array::from_iter(rows.map(|row| Some(row % groups))).into_ref(),
                        Utf8Array::from_iter((0..1024).map(|_| Some(value.as_str()))).into_ref(),
                        Utf8Array::from_iter((0..1024).map(|_| Some(","))).into_ref(),
                    ],
                    1024,
                )
            })
            .collect_vec()
    }

    /// States that keep growing after a pass stops admitting groups are written out with their
    /// groups when they can be encoded, so the aggregation stays within the memory limit.
    #[tokio::test]
    async fn test_spill_hash_agg_growing_states() {
        let input = growing_states_input(400, 1000, 100);
        let calls = [
            "(string_agg:varchar $1:varchar $2:varchar)",
            "(count:int8 $1:varchar)",
        ];
        let expected = spill_test_agg(&calls, &input, None).await.unwrap();
        // The states end up about 40 MB.
        let limit = 8 << 20;
        let counter = PeakCounter::default();
        let actual = spill_test_agg(
            &calls,
            &input,
            Some(MemoryContext::root(counter.clone(), limit)),
        )
        .await
        .unwrap();
        assert_eq!(actual, expected);
        let peak = counter.peak();
        assert!(peak < 3 * limit as i64, "peak {peak} bytes");
    }

    /// Growing states fail with `OutOfMemory` rather than exceed the memory limit when they cannot
    /// be encoded, or when they belong to a single group.
    #[tokio::test]
    async fn test_spill_hash_agg_growing_states_out_of_memory() {
        for (calls, groups) in [
            (["(array_agg:varchar[] $1:varchar)"], 4),
            (["(string_agg:varchar $1:varchar $2:varchar)"], 1),
        ] {
            let result = spill_test_agg(
                &calls,
                &growing_states_input(100, groups, 100),
                Some(MemoryContext::root(TrAdderAtomic::new(0), 1 << 20)),
            )
            .await;
            assert!(
                matches!(result, Err(BatchError::OutOfMemory(_))),
                "{calls:?}: {result:?}"
            );
        }
    }

    /// A partition larger than the minimum working set is aggregated in passes that spill again, at
    /// the next level.
    #[tokio::test]
    async fn test_spill_hash_agg_levels() {
        let padding = "x".repeat(250);
        let input = (0..100)
            .map(|chunk| {
                let rows = (0..1024).map(|row| chunk * 1024 + row);
                let payloads = rows
                    .clone()
                    .map(|row| format!("{}{padding}", row % 97))
                    .collect_vec();
                DataChunk::new(
                    vec![
                        I32Array::from_iter(rows.map(|row| Some(row * 7919 % 10_000))).into_ref(),
                        Utf8Array::from_iter(payloads.iter().map(|p| Some(p.as_str()))).into_ref(),
                    ],
                    1024,
                )
            })
            .collect_vec();
        check_spilled_agg(
            &["(count:int8 $1:varchar)", "(max:varchar $1:varchar)"],
            &input,
        )
        .await;
    }
}
