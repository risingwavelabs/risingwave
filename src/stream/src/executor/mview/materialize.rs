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

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::{Bound, Deref, Index};

use bytes::Bytes;
use futures::future::Either;
use futures::stream::{self, select_with_strategy};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnDesc, ConflictBehavior, TableId, checked_conflict_behaviors,
};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{CompactedRow, OwnedRow, RowExt};
use risingwave_common::types::{DEBEZIUM_UNAVAILABLE_VALUE, DataType, ScalarImpl};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType, cmp_datum};
use risingwave_common::util::value_encoding::{BasicSerde, ValueRowSerializer};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::catalog::Table;
use risingwave_pb::catalog::table::{Engine, OptionalAssociatedSourceId};
use risingwave_storage::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use risingwave_storage::store::{PrefetchOptions, TryWaitEpochOptions};
use risingwave_storage::table::KeyedRow;

use crate::cache::ManagedLruCache;
use crate::common::change_buffer::output_kind as cb_kind;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{
    StateTableBuilder, StateTableInner, StateTableOpConsistencyLevel,
};
use crate::executor::error::ErrorKind;
use crate::executor::monitor::MaterializeMetrics;
use crate::executor::mview::RefreshProgressTable;
use crate::executor::prelude::*;
use crate::executor::{BarrierInner, BarrierMutationType, EpochPair};
use crate::task::LocalBarrierManager;

#[derive(Debug, Clone)]
pub enum MaterializeStreamState<M> {
    NormalIngestion,
    MergingData,
    CleanUp,
    CommitAndYieldBarrier {
        barrier: BarrierInner<M>,
        expect_next_state: Box<MaterializeStreamState<M>>,
    },
    RefreshEnd {
        on_complete_epoch: EpochPair,
    },
}

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore, SD: ValueRowSerde> {
    input: Executor,

    schema: Schema,

    state_table: StateTableInner<S, SD>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_key_indices: Vec<usize>,

    actor_context: ActorContextRef,

    materialize_cache: MaterializeCache<SD>,

    conflict_behavior: ConflictBehavior,

    version_column_indices: Vec<u32>,

    may_have_downstream: bool,

    subscriber_ids: HashSet<u32>,

    metrics: MaterializeMetrics,

    /// No data will be written to hummock table. This Materialize is just a dummy node.
    /// Used for APPEND ONLY table with iceberg engine. All data will be written to iceberg table directly.
    is_dummy_table: bool,

    /// Indices of TOAST-able columns for PostgreSQL CDC tables. None means either non-CDC table or CDC table without TOAST-able columns.
    toastable_column_indices: Option<Vec<usize>>,

    /// Optional refresh arguments and state for refreshable materialized views
    refresh_args: Option<RefreshableMaterializeArgs<S, SD>>,

    /// Local barrier manager for reporting barrier events
    local_barrier_manager: LocalBarrierManager,
}

/// Arguments and state for refreshable materialized views
pub struct RefreshableMaterializeArgs<S: StateStore, SD: ValueRowSerde> {
    /// Table catalog for main table
    pub table_catalog: Table,

    /// Table catalog for staging table
    pub staging_table_catalog: Table,

    /// Flag indicating if this table is currently being refreshed
    pub is_refreshing: bool,

    /// During data refresh (between `RefreshStart` and `LoadFinish`),
    /// data will be written to both the main table and the staging table.
    ///
    /// The staging table is PK-only.
    ///
    /// After `LoadFinish`, we will do a `DELETE FROM main_table WHERE pk NOT IN (SELECT pk FROM staging_table)`, and then purge the staging table.
    pub staging_table: StateTableInner<S, SD>,

    /// Progress table for tracking refresh state per `VNode` for fault tolerance
    pub progress_table: RefreshProgressTable<S>,

    /// Table ID for this refreshable materialized view
    pub table_id: TableId,
}

impl<S: StateStore, SD: ValueRowSerde> RefreshableMaterializeArgs<S, SD> {
    /// Create new `RefreshableMaterializeArgs`
    pub async fn new(
        store: S,
        table_catalog: &Table,
        staging_table_catalog: &Table,
        progress_state_table: &Table,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        let table_id = table_catalog.id;

        // staging table is pk-only, and we don't need to check value consistency
        let staging_table = StateTableInner::from_table_catalog_inconsistent_op(
            staging_table_catalog,
            store.clone(),
            vnodes.clone(),
        )
        .await;

        let progress_state_table = StateTableInner::from_table_catalog_inconsistent_op(
            progress_state_table,
            store,
            vnodes,
        )
        .await;

        // Get primary key length from main table catalog
        let pk_len = table_catalog.pk.len();
        let progress_table = RefreshProgressTable::new(progress_state_table, pk_len);

        debug_assert_eq!(staging_table.vnodes(), progress_table.vnodes());

        Self {
            table_catalog: table_catalog.clone(),
            staging_table_catalog: staging_table_catalog.clone(),
            is_refreshing: false,
            staging_table,
            progress_table,
            table_id,
        }
    }
}

fn get_op_consistency_level(
    conflict_behavior: ConflictBehavior,
    may_have_downstream: bool,
    subscriber_ids: &HashSet<u32>,
) -> StateTableOpConsistencyLevel {
    if !subscriber_ids.is_empty() {
        StateTableOpConsistencyLevel::LogStoreEnabled
    } else if !may_have_downstream && matches!(conflict_behavior, ConflictBehavior::Overwrite) {
        // Table with overwrite conflict behavior could disable conflict check
        // if no downstream mv depends on it, so we use a inconsistent_op to skip sanity check as well.
        StateTableOpConsistencyLevel::Inconsistent
    } else {
        StateTableOpConsistencyLevel::ConsistentOldValue
    }
}

impl<S: StateStore, SD: ValueRowSerde> MaterializeExecutor<S, SD> {
    /// Create a new `MaterializeExecutor` with distribution specified with `distribution_keys` and
    /// `vnodes`. For singleton distribution, `distribution_keys` should be empty and `vnodes`
    /// should be `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: Executor,
        schema: Schema,
        store: S,
        arrange_key: Vec<ColumnOrder>,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
        version_column_indices: Vec<u32>,
        metrics: Arc<StreamingMetrics>,
        refresh_args: Option<RefreshableMaterializeArgs<S, SD>>,
        local_barrier_manager: LocalBarrierManager,
    ) -> Self {
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();

        // Extract TOAST-able column indices from table columns.
        // Only for PostgreSQL CDC tables.
        let toastable_column_indices = if table_catalog.cdc_table_type()
            == risingwave_pb::catalog::table::CdcTableType::Postgres
        {
            let toastable_indices: Vec<usize> = table_columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| match &column.data_type {
                    // Currently supports TOAST updates for:
                    // - jsonb (DataType::Jsonb)
                    // - varchar (DataType::Varchar)
                    // - bytea (DataType::Bytea)
                    // - One-dimensional arrays of the above types (DataType::List)
                    //   Note: Some array types may not be fully supported yet, see issue  https://github.com/risingwavelabs/risingwave/issues/22916 for details.

                    // For details on how TOAST values are handled, see comments in `is_debezium_unavailable_value`.
                    DataType::Varchar | DataType::List(_) | DataType::Bytea | DataType::Jsonb => {
                        Some(index)
                    }
                    _ => None,
                })
                .collect();

            if toastable_indices.is_empty() {
                None
            } else {
                Some(toastable_indices)
            }
        } else {
            None
        };

        let row_serde: BasicSerde = BasicSerde::new(
            Arc::from_iter(table_catalog.value_indices.iter().map(|val| *val as usize)),
            Arc::from(table_columns.into_boxed_slice()),
        );

        let arrange_key_indices: Vec<usize> = arrange_key.iter().map(|k| k.column_index).collect();
        let may_have_downstream = actor_context.initial_dispatch_num != 0;
        let subscriber_ids = actor_context.initial_subscriber_ids.clone();
        let op_consistency_level =
            get_op_consistency_level(conflict_behavior, may_have_downstream, &subscriber_ids);
        // Note: The current implementation could potentially trigger a switch on the inconsistent_op flag. If the storage relies on this flag to perform optimizations, it would be advisable to maintain consistency with it throughout the lifecycle.
        let state_table = StateTableBuilder::new(table_catalog, store, vnodes)
            .with_op_consistency_level(op_consistency_level)
            .enable_preload_all_rows_by_config(&actor_context.streaming_config)
            .build()
            .await;

        let mv_metrics = metrics.new_materialize_metrics(
            table_catalog.id,
            actor_context.id,
            actor_context.fragment_id,
        );

        let metrics_info =
            MetricsInfo::new(metrics, table_catalog.id, actor_context.id, "Materialize");

        let is_dummy_table =
            table_catalog.engine == Some(Engine::Iceberg as i32) && table_catalog.append_only;

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices,
            actor_context,
            materialize_cache: MaterializeCache::new(
                watermark_epoch,
                metrics_info,
                row_serde,
                version_column_indices.clone(),
            ),
            conflict_behavior,
            version_column_indices,
            is_dummy_table,
            may_have_downstream,
            subscriber_ids,
            metrics: mv_metrics,
            toastable_column_indices,
            refresh_args,
            local_barrier_manager,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mv_table_id = self.state_table.table_id();
        let data_types = self.schema.data_types();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        let _barrier_epoch = barrier.epoch; // Save epoch for later use (unused in normal execution)
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.state_table.init_epoch(first_epoch).await?;

        // default to normal ingestion
        let mut inner_state =
            Box::new(MaterializeStreamState::<BarrierMutationType>::NormalIngestion);
        // Initialize staging table for refreshable materialized views
        if let Some(ref mut refresh_args) = self.refresh_args {
            refresh_args.staging_table.init_epoch(first_epoch).await?;

            // Initialize progress table and load existing progress for recovery
            refresh_args.progress_table.recover(first_epoch).await?;

            // Check if refresh is already in progress (recovery scenario)
            let progress_stats = refresh_args.progress_table.get_progress_stats();
            if progress_stats.total_vnodes > 0 && !progress_stats.is_complete() {
                refresh_args.is_refreshing = true;
                tracing::info!(
                    total_vnodes = progress_stats.total_vnodes,
                    completed_vnodes = progress_stats.completed_vnodes,
                    "Recovered refresh in progress, resuming refresh operation"
                );

                // Since stage info is no longer stored in progress table,
                // we need to determine recovery state differently.
                // For now, assume all incomplete VNodes need to continue merging
                let incomplete_vnodes: Vec<_> = refresh_args
                    .progress_table
                    .get_all_progress()
                    .iter()
                    .filter(|(_, entry)| !entry.is_completed)
                    .map(|(&vnode, _)| vnode)
                    .collect();

                if !incomplete_vnodes.is_empty() {
                    // Some VNodes are incomplete, need to resume refresh operation
                    tracing::info!(
                        incomplete_vnodes = incomplete_vnodes.len(),
                        "Recovery detected incomplete VNodes, resuming refresh operation"
                    );
                    // Since stage tracking is now in memory, we'll determine the appropriate
                    // stage based on the executor's internal state machine
                } else {
                    // This should not happen if is_complete() returned false, but handle it gracefully
                    tracing::warn!("Unexpected recovery state: no incomplete VNodes found");
                }
            }
        }

        // Determine initial execution stage (for recovery scenarios)
        if let Some(ref refresh_args) = self.refresh_args
            && refresh_args.is_refreshing
        {
            // Recovery logic: Check if there are incomplete vnodes from previous run
            let incomplete_vnodes: Vec<_> = refresh_args
                .progress_table
                .get_all_progress()
                .iter()
                .filter(|(_, entry)| !entry.is_completed)
                .map(|(&vnode, _)| vnode)
                .collect();
            if !incomplete_vnodes.is_empty() {
                // Resume from merge stage since some VNodes were left incomplete
                inner_state = Box::new(MaterializeStreamState::<_>::MergingData);
                tracing::info!(
                    incomplete_vnodes = incomplete_vnodes.len(),
                    "Recovery: Resuming refresh from merge stage due to incomplete VNodes"
                );
            }
        }

        // Main execution loop: cycles through Stage 1 -> Stage 2 -> Stage 3 -> Stage 1...
        'main_loop: loop {
            match *inner_state {
                MaterializeStreamState::NormalIngestion => {
                    #[for_await]
                    '_normal_ingest: for msg in input.by_ref() {
                        let msg = msg?;
                        self.materialize_cache.evict();

                        match msg {
                            Message::Watermark(w) => {
                                yield Message::Watermark(w);
                            }
                            Message::Chunk(chunk) if self.is_dummy_table => {
                                self.metrics
                                    .materialize_input_row_count
                                    .inc_by(chunk.cardinality() as u64);
                                yield Message::Chunk(chunk);
                            }
                            Message::Chunk(chunk) => {
                                self.metrics
                                    .materialize_input_row_count
                                    .inc_by(chunk.cardinality() as u64);

                                // This is an optimization that handles conflicts only when a particular materialized view downstream has no MV dependencies.
                                // This optimization is applied only when there is no specified version column and the is_consistent_op flag of the state table is false,
                                // and the conflict behavior is overwrite. We can rely on the state table to overwrite the conflicting rows in the storage,
                                // while outputting inconsistent changes to downstream which no one will subscribe to.
                                let optimized_conflict_behavior = if let ConflictBehavior::Overwrite =
                                    self.conflict_behavior
                                    && !self.state_table.is_consistent_op()
                                    && self.version_column_indices.is_empty()
                                {
                                    ConflictBehavior::NoCheck
                                } else {
                                    self.conflict_behavior
                                };

                                match optimized_conflict_behavior {
                                    checked_conflict_behaviors!() => {
                                        if chunk.cardinality() == 0 {
                                            // empty chunk
                                            continue;
                                        }
                                        let (data_chunk, ops) = chunk.clone().into_parts();

                                        if self.state_table.value_indices().is_some() {
                                            // TODO(st1page): when materialize partial columns(), we should
                                            // construct some columns in the pk
                                            panic!(
                                                "materialize executor with data check can not handle only materialize partial columns"
                                            )
                                        };
                                        let values = data_chunk.serialize();

                                        let key_chunk =
                                            data_chunk.project(self.state_table.pk_indices());

                                        // For refreshable materialized views, write to staging table during refresh
                                        // Do not use generate_output here.
                                        if let Some(ref mut refresh_args) = self.refresh_args
                                            && refresh_args.is_refreshing
                                        {
                                            let key_chunk = chunk
                                                .clone()
                                                .project(self.state_table.pk_indices());
                                            tracing::trace!(
                                                staging_chunk = %key_chunk.to_pretty(),
                                                input_chunk = %chunk.to_pretty(),
                                                "writing to staging table"
                                            );
                                            if cfg!(debug_assertions) {
                                                // refreshable source should be append-only
                                                assert!(
                                                    key_chunk
                                                        .ops()
                                                        .iter()
                                                        .all(|op| op == &Op::Insert)
                                                );
                                            }
                                            refresh_args
                                                .staging_table
                                                .write_chunk(key_chunk.clone());
                                            refresh_args.staging_table.try_flush().await?;
                                        }

                                        let pks = {
                                            let mut pks = vec![vec![]; data_chunk.capacity()];
                                            key_chunk
                                                .rows_with_holes()
                                                .zip_eq_fast(pks.iter_mut())
                                                .for_each(|(r, vnode_and_pk)| {
                                                    if let Some(r) = r {
                                                        self.state_table
                                                            .pk_serde()
                                                            .serialize(r, vnode_and_pk);
                                                    }
                                                });
                                            pks
                                        };
                                        let (_, vis) = key_chunk.into_parts();
                                        let row_ops = ops
                                            .iter()
                                            .zip_eq_debug(pks.into_iter())
                                            .zip_eq_debug(values.into_iter())
                                            .zip_eq_debug(vis.iter())
                                            .filter_map(|(((op, k), v), vis)| {
                                                vis.then_some((*op, k, v))
                                            })
                                            .collect_vec();

                                        let change_buffer = self
                                            .materialize_cache
                                            .handle(
                                                row_ops,
                                                &self.state_table,
                                                self.conflict_behavior,
                                                &self.metrics,
                                                self.toastable_column_indices.as_deref(),
                                            )
                                            .await?;

                                        match change_buffer
                                            .into_chunk::<{ cb_kind::RETRACT }>(data_types.clone())
                                        {
                                            Some(output_chunk) => {
                                                self.state_table.write_chunk(output_chunk.clone());
                                                self.state_table.try_flush().await?;
                                                yield Message::Chunk(output_chunk);
                                            }
                                            None => continue,
                                        }
                                    }
                                    ConflictBehavior::NoCheck => {
                                        self.state_table.write_chunk(chunk.clone());
                                        self.state_table.try_flush().await?;

                                        // For refreshable materialized views, also write to staging table during refresh
                                        if let Some(ref mut refresh_args) = self.refresh_args
                                            && refresh_args.is_refreshing
                                        {
                                            let key_chunk = chunk
                                                .clone()
                                                .project(self.state_table.pk_indices());
                                            tracing::trace!(
                                                staging_chunk = %key_chunk.to_pretty(),
                                                input_chunk = %chunk.to_pretty(),
                                                "writing to staging table"
                                            );
                                            if cfg!(debug_assertions) {
                                                // refreshable source should be append-only
                                                assert!(
                                                    key_chunk
                                                        .ops()
                                                        .iter()
                                                        .all(|op| op == &Op::Insert)
                                                );
                                            }
                                            refresh_args
                                                .staging_table
                                                .write_chunk(key_chunk.clone());
                                            refresh_args.staging_table.try_flush().await?;
                                        }

                                        yield Message::Chunk(chunk);
                                    }
                                }
                            }
                            Message::Barrier(barrier) => {
                                *inner_state = MaterializeStreamState::CommitAndYieldBarrier {
                                    barrier,
                                    expect_next_state: Box::new(
                                        MaterializeStreamState::NormalIngestion,
                                    ),
                                };
                                continue 'main_loop;
                            }
                        }
                    }

                    return Err(StreamExecutorError::from(ErrorKind::Uncategorized(
                        anyhow::anyhow!(
                            "Input stream terminated unexpectedly during normal ingestion"
                        ),
                    )));
                }
                MaterializeStreamState::MergingData => {
                    let Some(refresh_args) = self.refresh_args.as_mut() else {
                        panic!(
                            "MaterializeExecutor entered CleanUp state without refresh_args configured"
                        );
                    };
                    tracing::info!(table_id = %refresh_args.table_id, "on_load_finish: Starting table replacement operation");

                    debug_assert_eq!(
                        self.state_table.vnodes(),
                        refresh_args.staging_table.vnodes()
                    );
                    debug_assert_eq!(
                        refresh_args.staging_table.vnodes(),
                        refresh_args.progress_table.vnodes()
                    );

                    let mut rows_to_delete = vec![];
                    let mut merge_complete = false;
                    let mut pending_barrier: Option<Barrier> = None;

                    // Scope to limit immutable borrows to state tables
                    {
                        let left_input = input.by_ref().map(Either::Left);
                        let right_merge_sort = pin!(
                            Self::make_mergesort_stream(
                                &self.state_table,
                                &refresh_args.staging_table,
                                &mut refresh_args.progress_table
                            )
                            .map(Either::Right)
                        );

                        // Prefer to select input stream to handle barriers promptly
                        // Rebuild the merge stream each time processing a barrier
                        let mut merge_stream =
                            select_with_strategy(left_input, right_merge_sort, |_: &mut ()| {
                                stream::PollNext::Left
                            });

                        #[for_await]
                        'merge_stream: for either in &mut merge_stream {
                            match either {
                                Either::Left(msg) => {
                                    let msg = msg?;
                                    match msg {
                                        Message::Watermark(w) => yield Message::Watermark(w),
                                        Message::Chunk(chunk) => {
                                            tracing::warn!(chunk = %chunk.to_pretty(), "chunk is ignored during merge phase");
                                        }
                                        Message::Barrier(b) => {
                                            pending_barrier = Some(b);
                                            break 'merge_stream;
                                        }
                                    }
                                }
                                Either::Right(result) => {
                                    match result? {
                                        Some((_vnode, row)) => {
                                            rows_to_delete.push(row);
                                        }
                                        None => {
                                            // Merge stream finished
                                            merge_complete = true;

                                            // If the merge stream finished, we need to wait for the next barrier to commit states
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Process collected rows for deletion
                    for row in &rows_to_delete {
                        self.state_table.delete(row);
                    }
                    if !rows_to_delete.is_empty() {
                        let to_delete_chunk = StreamChunk::from_rows(
                            &rows_to_delete
                                .iter()
                                .map(|row| (Op::Delete, row))
                                .collect_vec(),
                            &self.schema.data_types(),
                        );

                        yield Message::Chunk(to_delete_chunk);
                    }

                    // should wait for at least one barrier
                    assert!(pending_barrier.is_some(), "pending barrier is not set");

                    *inner_state = MaterializeStreamState::CommitAndYieldBarrier {
                        barrier: pending_barrier.unwrap(),
                        expect_next_state: if merge_complete {
                            Box::new(MaterializeStreamState::CleanUp)
                        } else {
                            Box::new(MaterializeStreamState::MergingData)
                        },
                    };
                    continue 'main_loop;
                }
                MaterializeStreamState::CleanUp => {
                    let Some(refresh_args) = self.refresh_args.as_mut() else {
                        panic!(
                            "MaterializeExecutor entered MergingData state without refresh_args configured"
                        );
                    };
                    tracing::info!(table_id = %refresh_args.table_id, "on_load_finish: resuming CleanUp Stage");

                    #[for_await]
                    for msg in input.by_ref() {
                        let msg = msg?;
                        match msg {
                            Message::Watermark(w) => yield Message::Watermark(w),
                            Message::Chunk(chunk) => {
                                tracing::warn!(chunk = %chunk.to_pretty(), "chunk is ignored during merge phase");
                            }
                            Message::Barrier(barrier) if !barrier.is_checkpoint() => {
                                *inner_state = MaterializeStreamState::CommitAndYieldBarrier {
                                    barrier,
                                    expect_next_state: Box::new(MaterializeStreamState::CleanUp),
                                };
                                continue 'main_loop;
                            }
                            Message::Barrier(barrier) => {
                                let staging_table_id = refresh_args.staging_table.table_id();
                                let epoch = barrier.epoch;
                                self.local_barrier_manager.report_refresh_finished(
                                    epoch,
                                    self.actor_context.id,
                                    refresh_args.table_id,
                                    staging_table_id,
                                );
                                tracing::debug!(table_id = %refresh_args.table_id, "on_load_finish: Reported staging table truncation and diff applied");

                                *inner_state = MaterializeStreamState::CommitAndYieldBarrier {
                                    barrier,
                                    expect_next_state: Box::new(
                                        MaterializeStreamState::RefreshEnd {
                                            on_complete_epoch: epoch,
                                        },
                                    ),
                                };
                                continue 'main_loop;
                            }
                        }
                    }
                }
                MaterializeStreamState::RefreshEnd { on_complete_epoch } => {
                    let Some(refresh_args) = self.refresh_args.as_mut() else {
                        panic!(
                            "MaterializeExecutor entered RefreshEnd state without refresh_args configured"
                        );
                    };
                    let staging_table_id = refresh_args.staging_table.table_id();

                    // Wait for staging table truncation to complete
                    let staging_store = refresh_args.staging_table.state_store().clone();
                    staging_store
                        .try_wait_epoch(
                            HummockReadEpoch::Committed(on_complete_epoch.prev),
                            TryWaitEpochOptions {
                                table_id: staging_table_id,
                            },
                        )
                        .await?;

                    tracing::info!(table_id = %refresh_args.table_id, "RefreshEnd: Refresh completed");

                    if let Some(ref mut refresh_args) = self.refresh_args {
                        refresh_args.is_refreshing = false;
                    }
                    *inner_state = MaterializeStreamState::NormalIngestion;
                    continue 'main_loop;
                }
                MaterializeStreamState::CommitAndYieldBarrier {
                    barrier,
                    mut expect_next_state,
                } => {
                    if let Some(ref mut refresh_args) = self.refresh_args {
                        match barrier.mutation.as_deref() {
                            Some(Mutation::RefreshStart {
                                table_id: refresh_table_id,
                                associated_source_id: _,
                            }) if *refresh_table_id == refresh_args.table_id => {
                                debug_assert!(
                                    !refresh_args.is_refreshing,
                                    "cannot start refresh twice"
                                );
                                refresh_args.is_refreshing = true;
                                tracing::info!(table_id = %refresh_table_id, "RefreshStart barrier received");

                                // Initialize progress tracking for all VNodes
                                Self::init_refresh_progress(
                                    &self.state_table,
                                    &mut refresh_args.progress_table,
                                    barrier.epoch.curr,
                                )?;
                            }
                            Some(Mutation::LoadFinish {
                                associated_source_id: load_finish_source_id,
                            }) => {
                                // Get associated source id from table catalog
                                let associated_source_id = match refresh_args
                                    .table_catalog
                                    .optional_associated_source_id
                                {
                                    Some(OptionalAssociatedSourceId::AssociatedSourceId(id)) => id,
                                    None => unreachable!("associated_source_id is not set"),
                                };

                                if load_finish_source_id.as_raw_id() == associated_source_id {
                                    tracing::info!(
                                        %load_finish_source_id,
                                        "LoadFinish received, starting data replacement"
                                    );
                                    expect_next_state =
                                        Box::new(MaterializeStreamState::<_>::MergingData);
                                }
                            }
                            _ => {}
                        }
                    }

                    // ===== normal operation =====

                    // If a downstream mv depends on the current table, we need to do conflict check again.
                    if !self.may_have_downstream
                        && barrier.has_more_downstream_fragments(self.actor_context.id)
                    {
                        self.may_have_downstream = true;
                    }
                    Self::may_update_depended_subscriptions(
                        &mut self.subscriber_ids,
                        &barrier,
                        mv_table_id,
                    );
                    let op_consistency_level = get_op_consistency_level(
                        self.conflict_behavior,
                        self.may_have_downstream,
                        &self.subscriber_ids,
                    );
                    let post_commit = self
                        .state_table
                        .commit_may_switch_consistent_op(barrier.epoch, op_consistency_level)
                        .await?;
                    if !post_commit.inner().is_consistent_op() {
                        assert_eq!(self.conflict_behavior, ConflictBehavior::Overwrite);
                    }

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.actor_context.id);

                    // Commit staging table for refreshable materialized views
                    let refresh_post_commit = if let Some(ref mut refresh_args) = self.refresh_args
                    {
                        // Commit progress table for fault tolerance

                        Some((
                            refresh_args.staging_table.commit(barrier.epoch).await?,
                            refresh_args.progress_table.commit(barrier.epoch).await?,
                        ))
                    } else {
                        None
                    };

                    let b_epoch = barrier.epoch;
                    yield Message::Barrier(barrier);

                    // Update the vnode bitmap for the state table if asked.
                    if let Some((_, cache_may_stale)) = post_commit
                        .post_yield_barrier(update_vnode_bitmap.clone())
                        .await?
                        && cache_may_stale
                    {
                        self.materialize_cache.lru_cache.clear();
                    }

                    // Handle staging table post commit
                    if let Some((staging_post_commit, progress_post_commit)) = refresh_post_commit {
                        staging_post_commit
                            .post_yield_barrier(update_vnode_bitmap.clone())
                            .await?;
                        progress_post_commit
                            .post_yield_barrier(update_vnode_bitmap)
                            .await?;
                    }

                    self.metrics
                        .materialize_current_epoch
                        .set(b_epoch.curr as i64);

                    // ====== transition to next state ======

                    *inner_state = *expect_next_state;
                }
            }
        }
    }

    /// Stream that yields rows to be deleted from main table.
    /// Yields `Some((vnode, row))` for rows that exist in main but not in staging.
    /// Yields `None` when finished processing all vnodes.
    #[try_stream(ok = Option<(VirtualNode, OwnedRow)>, error = StreamExecutorError)]
    async fn make_mergesort_stream<'a>(
        main_table: &'a StateTableInner<S, SD>,
        staging_table: &'a StateTableInner<S, SD>,
        progress_table: &'a mut RefreshProgressTable<S>,
    ) {
        for vnode in main_table.vnodes().clone().iter_vnodes() {
            let mut processed_rows = 0;
            // Check if this VNode has already been completed (for fault tolerance)
            let pk_range: (Bound<OwnedRow>, Bound<OwnedRow>) =
                if let Some(current_entry) = progress_table.get_progress(vnode) {
                    // Skip already completed VNodes during recovery
                    if current_entry.is_completed {
                        tracing::debug!(
                            vnode = vnode.to_index(),
                            "Skipping already completed VNode during recovery"
                        );
                        continue;
                    }
                    processed_rows += current_entry.processed_rows;
                    tracing::debug!(vnode = vnode.to_index(), "Started merging VNode");

                    if let Some(current_state) = &current_entry.current_pos {
                        (Bound::Excluded(current_state.clone()), Bound::Unbounded)
                    } else {
                        (Bound::Unbounded, Bound::Unbounded)
                    }
                } else {
                    (Bound::Unbounded, Bound::Unbounded)
                };

            let iter_main = main_table
                .iter_keyed_row_with_vnode(
                    vnode,
                    &pk_range,
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
                .await?;
            let iter_staging = staging_table
                .iter_keyed_row_with_vnode(
                    vnode,
                    &pk_range,
                    PrefetchOptions::prefetch_for_large_range_scan(),
                )
                .await?;

            pin_mut!(iter_main);
            pin_mut!(iter_staging);

            // Sort-merge join implementation using dual pointers
            let mut main_item: Option<KeyedRow<Bytes>> = iter_main.next().await.transpose()?;
            let mut staging_item: Option<KeyedRow<Bytes>> =
                iter_staging.next().await.transpose()?;

            while let Some(main_kv) = main_item {
                let main_key = main_kv.key();

                // Advance staging iterator until we find a key >= main_key
                let mut should_delete = false;
                while let Some(staging_kv) = &staging_item {
                    let staging_key = staging_kv.key();
                    match main_key.cmp(staging_key) {
                        std::cmp::Ordering::Greater => {
                            // main_key > staging_key, advance staging
                            staging_item = iter_staging.next().await.transpose()?;
                        }
                        std::cmp::Ordering::Equal => {
                            // Keys match, this row exists in both tables, no need to delete
                            break;
                        }
                        std::cmp::Ordering::Less => {
                            // main_key < staging_key, main row doesn't exist in staging, delete it
                            should_delete = true;
                            break;
                        }
                    }
                }

                // If staging_item is None, all remaining main rows should be deleted
                if staging_item.is_none() {
                    should_delete = true;
                }

                if should_delete {
                    yield Some((vnode, main_kv.row().clone()));
                }

                // Advance main iterator
                processed_rows += 1;
                tracing::info!(
                    "set progress table: vnode = {:?}, processed_rows = {:?}",
                    vnode,
                    processed_rows
                );
                progress_table.set_progress(
                    vnode,
                    Some(
                        main_kv
                            .row()
                            .project(main_table.pk_indices())
                            .to_owned_row(),
                    ),
                    false,
                    processed_rows,
                )?;
                main_item = iter_main.next().await.transpose()?;
            }

            // Mark this VNode as completed
            if let Some(current_entry) = progress_table.get_progress(vnode) {
                progress_table.set_progress(
                    vnode,
                    current_entry.current_pos.clone(),
                    true, // completed
                    current_entry.processed_rows,
                )?;

                tracing::debug!(vnode = vnode.to_index(), "Completed merging VNode");
            }
        }

        // Signal completion
        yield None;
    }

    /// return true when changed
    fn may_update_depended_subscriptions(
        depended_subscriptions: &mut HashSet<u32>,
        barrier: &Barrier,
        mv_table_id: TableId,
    ) {
        for subscriber_id in barrier.added_subscriber_on_mv_table(mv_table_id) {
            if !depended_subscriptions.insert(subscriber_id) {
                warn!(
                    ?depended_subscriptions,
                    ?mv_table_id,
                    subscriber_id,
                    "subscription id already exists"
                );
            }
        }

        if let Some(Mutation::DropSubscriptions {
            subscriptions_to_drop,
        }) = barrier.mutation.as_deref()
        {
            for (subscriber_id, upstream_mv_table_id) in subscriptions_to_drop {
                if *upstream_mv_table_id == mv_table_id
                    && !depended_subscriptions.remove(subscriber_id)
                {
                    warn!(
                        ?depended_subscriptions,
                        ?mv_table_id,
                        subscriber_id,
                        "drop non existing subscriber_id id"
                    );
                }
            }
        }
    }

    /// Initialize refresh progress tracking for all `VNodes`
    fn init_refresh_progress(
        state_table: &StateTableInner<S, SD>,
        progress_table: &mut RefreshProgressTable<S>,
        _epoch: u64,
    ) -> StreamExecutorResult<()> {
        debug_assert_eq!(state_table.vnodes(), progress_table.vnodes());

        // Initialize progress for all VNodes in the current bitmap
        for vnode in state_table.vnodes().iter_vnodes() {
            progress_table.set_progress(
                vnode, None,  // initial position
                false, // not completed yet
                0,     // initial processed rows
            )?;
        }

        tracing::info!(
            vnodes_count = state_table.vnodes().count_ones(),
            "Initialized refresh progress tracking for all VNodes"
        );

        Ok(())
    }
}

impl<S: StateStore> MaterializeExecutor<S, BasicSerde> {
    /// Create a new `MaterializeExecutor` without distribution info for test purpose.
    #[cfg(any(test, feature = "test"))]
    pub async fn for_test(
        input: Executor,
        store: S,
        table_id: TableId,
        keys: Vec<ColumnOrder>,
        column_ids: Vec<risingwave_common::catalog::ColumnId>,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
    ) -> Self {
        let arrange_columns: Vec<usize> = keys.iter().map(|k| k.column_index).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        let schema = input.schema().clone();
        let columns: Vec<ColumnDesc> = column_ids
            .into_iter()
            .zip_eq_fast(schema.fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(column_id, field.data_type()))
            .collect_vec();

        let row_serde = BasicSerde::new(
            Arc::from((0..columns.len()).collect_vec()),
            Arc::from(columns.clone().into_boxed_slice()),
        );
        let state_table = StateTableInner::from_table_catalog(
            &crate::common::table::test_utils::gen_pbtable(
                table_id,
                columns,
                arrange_order_types,
                arrange_columns.clone(),
                0,
            ),
            store,
            None,
        )
        .await;

        let metrics = StreamingMetrics::unused().new_materialize_metrics(table_id, 1, 2.into());

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices: arrange_columns.clone(),
            actor_context: ActorContext::for_test(0),
            materialize_cache: MaterializeCache::new(
                watermark_epoch,
                MetricsInfo::for_test(),
                row_serde,
                vec![],
            ),
            conflict_behavior,
            version_column_indices: vec![],
            is_dummy_table: false,
            toastable_column_indices: None,
            may_have_downstream: true,
            subscriber_ids: HashSet::new(),
            metrics,
            refresh_args: None, // Test constructor doesn't support refresh functionality
            local_barrier_manager: LocalBarrierManager::for_test(),
        }
    }
}

/// Fast string comparison to check if a string equals `DEBEZIUM_UNAVAILABLE_VALUE`.
/// Optimized by checking length first to avoid expensive string comparison.
fn is_unavailable_value_str(s: &str) -> bool {
    s.len() == DEBEZIUM_UNAVAILABLE_VALUE.len() && s == DEBEZIUM_UNAVAILABLE_VALUE
}

/// Check if a datum represents Debezium's unavailable value placeholder.
/// This function handles both scalar types and one-dimensional arrays.
fn is_debezium_unavailable_value(
    datum: &Option<risingwave_common::types::ScalarRefImpl<'_>>,
) -> bool {
    match datum {
        Some(risingwave_common::types::ScalarRefImpl::Utf8(val)) => is_unavailable_value_str(val),
        Some(risingwave_common::types::ScalarRefImpl::Jsonb(jsonb_ref)) => {
            // For jsonb type, check if it's a string containing the unavailable value
            jsonb_ref
                .as_str()
                .map(is_unavailable_value_str)
                .unwrap_or(false)
        }
        Some(risingwave_common::types::ScalarRefImpl::Bytea(bytea)) => {
            // For bytea type, we need to check if it contains the string bytes of DEBEZIUM_UNAVAILABLE_VALUE
            // This is because when processing bytea from Debezium, we convert the base64-encoded string
            // to `DEBEZIUM_UNAVAILABLE_VALUE` in the json.rs parser to maintain consistency
            if let Ok(bytea_str) = std::str::from_utf8(bytea) {
                is_unavailable_value_str(bytea_str)
            } else {
                false
            }
        }
        Some(risingwave_common::types::ScalarRefImpl::List(list_ref)) => {
            // For list type, check if it contains exactly one element with the unavailable value
            // This is because when any element in an array triggers TOAST, Debezium treats the entire
            // array as unchanged and sends a placeholder array with only one element
            if list_ref.len() == 1 {
                if let Some(Some(element)) = list_ref.get(0) {
                    // Recursively check the array element
                    is_debezium_unavailable_value(&Some(element))
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Fix TOAST columns by replacing unavailable values with old row values.
fn handle_toast_columns_for_postgres_cdc(
    old_row: &OwnedRow,
    new_row: &OwnedRow,
    toastable_indices: &[usize],
) -> OwnedRow {
    let mut fixed_row_data = new_row.as_inner().to_vec();

    for &toast_idx in toastable_indices {
        // Check if the new value is Debezium's unavailable value placeholder
        let is_unavailable = is_debezium_unavailable_value(&new_row.datum_at(toast_idx));
        if is_unavailable {
            // Replace with old row value if available
            if let Some(old_datum_ref) = old_row.datum_at(toast_idx) {
                fixed_row_data[toast_idx] = Some(old_datum_ref.into_scalar_impl());
            }
        }
    }

    OwnedRow::new(fixed_row_data)
}

type ChangeBuffer = crate::common::change_buffer::ChangeBuffer<Vec<u8>, OwnedRow>;

impl<S: StateStore, SD: ValueRowSerde> Execute for MaterializeExecutor<S, SD> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for MaterializeExecutor<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("arrange_key_indices", &self.arrange_key_indices)
            .finish()
    }
}

/// A cache for materialize executors.
struct MaterializeCache<SD> {
    lru_cache: ManagedLruCache<Vec<u8>, CacheValue>,
    row_serde: BasicSerde,
    version_column_indices: Vec<u32>,
    _serde: PhantomData<SD>,
}

type CacheValue = Option<CompactedRow>;

impl<SD: ValueRowSerde> MaterializeCache<SD> {
    fn new(
        watermark_sequence: AtomicU64Ref,
        metrics_info: MetricsInfo,
        row_serde: BasicSerde,
        version_column_indices: Vec<u32>,
    ) -> Self {
        let lru_cache: ManagedLruCache<Vec<u8>, CacheValue> =
            ManagedLruCache::unbounded(watermark_sequence, metrics_info);
        Self {
            lru_cache,
            row_serde,
            version_column_indices,
            _serde: PhantomData,
        }
    }

    /// First populate the cache from `table`, and then calculate a [`ChangeBuffer`].
    /// `table` will not be written in this method.
    async fn handle<S: StateStore>(
        &mut self,
        row_ops: Vec<(Op, Vec<u8>, Bytes)>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: ConflictBehavior,
        metrics: &MaterializeMetrics,
        toastable_column_indices: Option<&[usize]>,
    ) -> StreamExecutorResult<ChangeBuffer> {
        assert_matches!(conflict_behavior, checked_conflict_behaviors!());

        let key_set: HashSet<Box<[u8]>> = row_ops
            .iter()
            .map(|(_, k, _)| k.as_slice().into())
            .collect();

        // Populate the LRU cache with the keys in input chunk.
        // For new keys, row values are set to None.
        self.fetch_keys(
            key_set.iter().map(|v| v.deref()),
            table,
            conflict_behavior,
            metrics,
        )
        .await?;

        let mut change_buffer = ChangeBuffer::new();
        let row_serde = self.row_serde.clone();
        let version_column_indices = self.version_column_indices.clone();
        for (op, key, row) in row_ops {
            match op {
                Op::Insert | Op::UpdateInsert => {
                    let Some(old_row) = self.get_expected(&key) else {
                        // not exists before, meaning no conflict, simply insert
                        let new_row_deserialized =
                            row_serde.deserializer.deserialize(row.clone())?;
                        change_buffer.insert(key.clone(), new_row_deserialized);
                        self.lru_cache.put(key, Some(CompactedRow { row }));
                        continue;
                    };

                    // now conflict happens, handle it according to the specified behavior
                    match conflict_behavior {
                        ConflictBehavior::Overwrite => {
                            let old_row_deserialized =
                                row_serde.deserializer.deserialize(old_row.row.clone())?;
                            let new_row_deserialized =
                                row_serde.deserializer.deserialize(row.clone())?;

                            let need_overwrite = if !version_column_indices.is_empty() {
                                versions_are_newer_or_equal(
                                    &old_row_deserialized,
                                    &new_row_deserialized,
                                    &version_column_indices,
                                )
                            } else {
                                // no version column specified, just overwrite
                                true
                            };

                            if need_overwrite {
                                if let Some(toastable_indices) = toastable_column_indices {
                                    // For TOAST-able columns, replace Debezium's unavailable value placeholder with old row values.
                                    let final_row = handle_toast_columns_for_postgres_cdc(
                                        &old_row_deserialized,
                                        &new_row_deserialized,
                                        toastable_indices,
                                    );

                                    change_buffer.update(
                                        key.clone(),
                                        old_row_deserialized,
                                        final_row.clone(),
                                    );
                                    let final_row_bytes =
                                        Bytes::from(row_serde.serializer.serialize(final_row));
                                    self.lru_cache.put(
                                        key.clone(),
                                        Some(CompactedRow {
                                            row: final_row_bytes,
                                        }),
                                    );
                                } else {
                                    // No TOAST columns, use the original row bytes directly to avoid unnecessary serialization
                                    change_buffer.update(
                                        key.clone(),
                                        old_row_deserialized,
                                        new_row_deserialized,
                                    );
                                    self.lru_cache
                                        .put(key.clone(), Some(CompactedRow { row: row.clone() }));
                                }
                            };
                        }
                        ConflictBehavior::IgnoreConflict => {
                            // ignore conflict, do nothing
                        }
                        ConflictBehavior::DoUpdateIfNotNull => {
                            // In this section, we compare the new row and old row column by column and perform `DoUpdateIfNotNull` replacement.

                            let old_row_deserialized =
                                row_serde.deserializer.deserialize(old_row.row.clone())?;
                            let new_row_deserialized =
                                row_serde.deserializer.deserialize(row.clone())?;
                            let need_overwrite = if !version_column_indices.is_empty() {
                                versions_are_newer_or_equal(
                                    &old_row_deserialized,
                                    &new_row_deserialized,
                                    &version_column_indices,
                                )
                            } else {
                                true
                            };

                            if need_overwrite {
                                let mut row_deserialized_vec =
                                    old_row_deserialized.clone().into_inner().into_vec();
                                replace_if_not_null(
                                    &mut row_deserialized_vec,
                                    new_row_deserialized.clone(),
                                );
                                let mut updated_row = OwnedRow::new(row_deserialized_vec);

                                // Apply TOAST column fix for CDC tables with TOAST columns
                                if let Some(toastable_indices) = toastable_column_indices {
                                    // Note: we need to use old_row_deserialized again, but it was moved above
                                    // So we re-deserialize the old row
                                    let old_row_deserialized_again =
                                        row_serde.deserializer.deserialize(old_row.row.clone())?;
                                    updated_row = handle_toast_columns_for_postgres_cdc(
                                        &old_row_deserialized_again,
                                        &updated_row,
                                        toastable_indices,
                                    );
                                }

                                change_buffer.update(
                                    key.clone(),
                                    old_row_deserialized,
                                    updated_row.clone(),
                                );
                                let updated_row_bytes =
                                    Bytes::from(row_serde.serializer.serialize(updated_row));
                                self.lru_cache.put(
                                    key.clone(),
                                    Some(CompactedRow {
                                        row: updated_row_bytes,
                                    }),
                                );
                            }
                        }
                        _ => unreachable!(),
                    };
                }

                Op::UpdateDelete
                    if matches!(
                        conflict_behavior,
                        ConflictBehavior::Overwrite | ConflictBehavior::DoUpdateIfNotNull
                    ) =>
                {
                    // For `UpdateDelete`s, we skip processing them but directly handle the following `UpdateInsert`
                    // instead. This is because...
                    //
                    // - For `Overwrite`, we only care about the new row.
                    // - For `DoUpdateIfNotNull`, we don't want the whole row to be deleted, but instead perform
                    //   column-wise replacement when handling the `UpdateInsert`.
                    //
                    // However, for `IgnoreConflict`, we still need to delete the old row first, otherwise the row
                    // cannot be updated at all.
                }

                Op::Delete | Op::UpdateDelete => {
                    if let Some(old_row) = self.get_expected(&key) {
                        let old_row_deserialized =
                            row_serde.deserializer.deserialize(old_row.row.clone())?;
                        change_buffer.delete(key.clone(), old_row_deserialized);
                        // put a None into the cache to represent deletion
                        self.lru_cache.put(key, None);
                    } else {
                        // delete a non-existent value
                        // this is allowed in the case of mview conflict, so ignore
                    }
                }
            }
        }
        Ok(change_buffer)
    }

    async fn fetch_keys<'a, S: StateStore>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: ConflictBehavior,
        metrics: &MaterializeMetrics,
    ) -> StreamExecutorResult<()> {
        let mut futures = vec![];
        for key in keys {
            metrics.materialize_cache_total_count.inc();

            if self.lru_cache.contains(key) {
                if self.lru_cache.get(key).unwrap().is_some() {
                    metrics.materialize_data_exist_count.inc();
                }
                metrics.materialize_cache_hit_count.inc();
                continue;
            }
            futures.push(async {
                let key_row = table.pk_serde().deserialize(key).unwrap();
                let row = table.get_row(key_row).await?.map(CompactedRow::from);
                StreamExecutorResult::Ok((key.to_vec(), row))
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
        while let Some(result) = buffered.next().await {
            let (key, row) = result?;
            if row.is_some() {
                metrics.materialize_data_exist_count.inc();
            }
            // for keys that are not in the table, `value` is None
            match conflict_behavior {
                checked_conflict_behaviors!() => self.lru_cache.put(key, row),
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    fn get_expected(&mut self, key: &[u8]) -> &CacheValue {
        self.lru_cache.get(key).unwrap_or_else(|| {
            panic!(
                "the key {:?} has not been fetched in the materialize executor's cache ",
                key
            )
        })
    }

    fn evict(&mut self) {
        self.lru_cache.evict()
    }
}

/// Replace columns in an existing row with the corresponding columns in a replacement row, if the
/// column value in the replacement row is not null.
///
/// # Example
///
/// ```ignore
/// let mut row = vec![Some(1), None, Some(3)];
/// let replacement = vec![Some(10), Some(20), None];
/// replace_if_not_null(&mut row, replacement);
/// ```
///
/// After the call, `row` will be `[Some(10), Some(20), Some(3)]`.
fn replace_if_not_null(row: &mut Vec<Option<ScalarImpl>>, replacement: OwnedRow) {
    for (old_col, new_col) in row.iter_mut().zip_eq_fast(replacement) {
        if let Some(new_value) = new_col {
            *old_col = Some(new_value);
        }
    }
}

/// Compare multiple version columns lexicographically.
/// Returns true if `new_row` has a newer or equal version compared to `old_row`.
fn versions_are_newer_or_equal(
    old_row: &OwnedRow,
    new_row: &OwnedRow,
    version_column_indices: &[u32],
) -> bool {
    if version_column_indices.is_empty() {
        // No version columns specified, always consider new version as newer
        return true;
    }

    for &idx in version_column_indices {
        let old_value = old_row.index(idx as usize);
        let new_value = new_row.index(idx as usize);

        match cmp_datum(old_value, new_value, OrderType::ascending_nulls_first()) {
            std::cmp::Ordering::Less => return true,     // new is newer
            std::cmp::Ordering::Greater => return false, // old is newer
            std::cmp::Ordering::Equal => continue,       // equal, check next column
        }
    }

    // All version columns are equal, consider new version as equal (should overwrite)
    true
}

#[cfg(test)]
mod tests {

    use std::iter;
    use std::sync::atomic::AtomicU64;

    use rand::rngs::SmallRng;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::array::stream_chunk::{StreamChunkMut, StreamChunkTestExt};
    use risingwave_common::catalog::Field;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::BatchTable;

    use super::*;
    use crate::executor::test_utils::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    // https://github.com/risingwavelabs/risingwave/issues/13346
    #[tokio::test]
    async fn test_upsert_stream() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 1",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 2
            - 1 2",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert!(row.is_none());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_check_insert_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 4",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_delete_and_update_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
            + 8 3",
        );

        // test delete wrong value, delete inexistent pk
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            U- 2 4
            U+ 2 8
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 3), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(3_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(5_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_insert_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 5
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(5_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_delete_then_insert() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test insert after delete one pk, the latter insert should succeed.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            - 1 3
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        let _msg1 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();
        let _msg2 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_chunk()
            .unwrap();
        let _msg3 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();

        let row = table
            .get_row(
                &OwnedRow::new(vec![Some(1_i32.into())]),
                HummockReadEpoch::NoWait(u64::MAX),
            )
            .await
            .unwrap();
        assert_eq!(
            row,
            Some(OwnedRow::new(vec![Some(1_i32.into()), Some(6_i32.into())]))
        );
    }

    #[tokio::test]
    async fn test_ignore_delete_and_update_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter should be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
            + 8 3",
        );

        // test delete wrong value, delete inexistent pk
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            U- 2 4
            U+ 2 8
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 2), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(2_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(4_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_do_update_if_not_null_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // should get (8, 2)
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 .
            + 3 6
            U- 8 .
            U+ 8 2
            + 8 .",
        );

        // should not get (3, x), should not get (5, 0)
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // should get (2, None), (7, 8)
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            + 7 .
            U- 2 4
            U+ 2 .
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), StreamKey::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::DoUpdateIfNotNull,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(2_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(OwnedRow::new(vec![Some(2_i32.into()), None])));
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(OwnedRow::new(vec![Some(2_i32.into()), None])));

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    fn gen_fuzz_data(row_number: usize, chunk_size: usize) -> Vec<StreamChunk> {
        const KN: u32 = 4;
        const SEED: u64 = 998244353;
        let mut ret = vec![];
        let mut builder =
            StreamChunkBuilder::new(chunk_size, vec![DataType::Int32, DataType::Int32]);
        let mut rng = SmallRng::seed_from_u64(SEED);

        let random_vis = |c: StreamChunk, rng: &mut SmallRng| -> StreamChunk {
            let len = c.data_chunk().capacity();
            let mut c = StreamChunkMut::from(c);
            for i in 0..len {
                c.set_vis(i, rng.random_bool(0.5));
            }
            c.into()
        };
        for _ in 0..row_number {
            let k = (rng.next_u32() % KN) as i32;
            let v = rng.next_u32() as i32;
            let op = if rng.random_bool(0.5) {
                Op::Insert
            } else {
                Op::Delete
            };
            if let Some(c) =
                builder.append_row(op, OwnedRow::new(vec![Some(k.into()), Some(v.into())]))
            {
                ret.push(random_vis(c, &mut rng));
            }
        }
        if let Some(c) = builder.take() {
            ret.push(random_vis(c, &mut rng));
        }
        ret
    }

    async fn fuzz_test_stream_consistent_inner(conflict_behavior: ConflictBehavior) {
        const N: usize = 100000;

        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        let chunks = gen_fuzz_data(N, 128);
        let messages = iter::once(Message::Barrier(Barrier::new_test_barrier(test_epoch(1))))
            .chain(chunks.into_iter().map(Message::Chunk))
            .chain(iter::once(Message::Barrier(Barrier::new_test_barrier(
                test_epoch(2),
            ))))
            .collect();
        // Prepare stream executors.
        let source =
            MockSource::with_messages(messages).into_executor(schema.clone(), StreamKey::new());

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store.clone(),
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            conflict_behavior,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.expect_barrier().await;

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(0.into(), DataType::Int32),
            ColumnDesc::unnamed(1.into(), DataType::Int32),
        ];
        let pk_indices = vec![0];

        let mut table = StateTable::from_table_catalog(
            &crate::common::table::test_utils::gen_pbtable(
                TableId::from(1002),
                column_descs.clone(),
                order_types,
                pk_indices,
                0,
            ),
            memory_state_store.clone(),
            None,
        )
        .await;

        while let Message::Chunk(c) = materialize_executor.next().await.unwrap().unwrap() {
            // check with state table's memtable
            table.write_chunk(c);
        }
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_upsert() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::Overwrite).await
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_ignore() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::IgnoreConflict).await
    }
}
