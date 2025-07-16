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

use std::collections::HashSet;
use std::ops::Bound;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{ColumnDesc, ConflictBehavior, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::catalog::Table;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_storage::StateStore;
use risingwave_storage::mem_table::KeyOp;
use risingwave_storage::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::KeyedRow;

use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{StateTableInner, StateTableOpConsistencyLevel};
use crate::executor::monitor::MaterializeMetrics;
use crate::executor::mview::materialize::MaterializeCache;
use crate::executor::prelude::*;
use crate::executor::{MaterializeExecutor, generate_output};

/// `RefreshableMaterializeExecutor` extends `MaterializeExecutor` with refresh capabilities.
/// It handles `LoadFinish` mutations to reset state and reload data from source.
pub struct RefreshableMaterializeExecutor<S: StateStore, SD: ValueRowSerde> {
    input: Executor,

    schema: Schema,

    state_table: StateTableInner<S, SD>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_key_indices: Vec<usize>,

    actor_context: ActorContextRef,

    materialize_cache: MaterializeCache<SD>,

    conflict_behavior: ConflictBehavior,

    version_column_index: Option<u32>,

    may_have_downstream: bool,

    depended_subscription_ids: HashSet<u32>,

    metrics: MaterializeMetrics,

    /// Table ID for this refreshable materialized view
    table_id: TableId,

    /// Flag indicating if this table is currently being refreshed
    is_refreshing: bool,

    /// During data refresh (between `RefreshStart` and `LoadFinish`),
    /// data will be written to both the main table and the staging table.
    ///
    /// The staging table is PK-only.
    ///
    /// After `LoadFinish`, we will do a `DELETE FROM main_table WHERE pk NOT IN (SELECT pk FROM staging_table)`, and then purge the staging table.
    staging_table: StateTableInner<S, SD>,

    /// Store instance for creating staging tables
    store: S,

    /// Table catalog for main table
    table_catalog: Table,

    /// Table catalog for staging table (if this is a refreshable table)
    staging_table_catalog: Table,
}

impl<S: StateStore, SD: ValueRowSerde> RefreshableMaterializeExecutor<S, SD> {
    /// Create a new `RefreshableMaterializeExecutor`
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: Executor,
        schema: Schema,
        store: S,
        arrange_key: Vec<ColumnOrder>,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        staging_table_catalog: &Table,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
        version_column_index: Option<u32>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let table_id = TableId::new(table_catalog.id);
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();

        let row_serde: BasicSerde = BasicSerde::new(
            Arc::from_iter(table_catalog.value_indices.iter().map(|val| *val as usize)),
            Arc::from(table_columns.into_boxed_slice()),
        );

        let arrange_key_indices: Vec<usize> = arrange_key.iter().map(|k| k.column_index).collect();
        let may_have_downstream = actor_context.initial_dispatch_num != 0;
        let depended_subscription_ids = actor_context
            .related_subscriptions
            .get(&table_id)
            .cloned()
            .unwrap_or_default();
        // let op_consistency_level = get_op_consistency_level(
        //     conflict_behavior,
        //     may_have_downstream,
        //     &depended_subscription_ids,
        // );
        // TODO: add this optimization
        let op_consistency_level = StateTableOpConsistencyLevel::ConsistentOldValue;

        let state_table = StateTableInner::from_table_catalog_with_consistency_level(
            table_catalog,
            store.clone(),
            vnodes.clone(),
            op_consistency_level,
        )
        .await;
        let staging_table = StateTableInner::from_table_catalog_with_consistency_level(
            staging_table_catalog,
            store.clone(),
            vnodes.clone(),
            op_consistency_level,
        )
        .await;

        let mv_metrics =
            metrics.new_materialize_metrics(table_id, actor_context.id, actor_context.fragment_id);

        let metrics_info = MetricsInfo::new(
            metrics,
            table_catalog.id,
            actor_context.id,
            "RefreshableMaterialize",
        );

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
                version_column_index,
            ),
            conflict_behavior,
            version_column_index,
            may_have_downstream,
            depended_subscription_ids,
            metrics: mv_metrics,
            table_id,
            is_refreshing: false,
            staging_table,
            store,
            table_catalog: table_catalog.clone(),
            staging_table_catalog: staging_table_catalog.clone(),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let data_types = self.schema.data_types();

        // Extract necessary fields before consuming self.input
        let mut state_table = self.state_table;
        let mut staging_table = self.staging_table;
        let mut is_refreshing = self.is_refreshing;
        let table_id = self.table_id;
        let store = self.store;
        let staging_table_catalog = self.staging_table_catalog;
        let table_catalog = self.table_catalog;
        let associated_source_id = match table_catalog.optional_associated_source_id {
            Some(OptionalAssociatedSourceId::AssociatedSourceId(id)) => id,
            None => unreachable!("associated_source_id is not set"),
        };
        let conflict_behavior = self.conflict_behavior;
        // let mut may_have_downstream = self.may_have_downstream;
        // let mut depended_subscription_ids = self.depended_subscription_ids;
        let mut materialize_cache = self.materialize_cache;
        let metrics = self.metrics;
        let version_column_index = self.version_column_index;
        let actor_context = self.actor_context;

        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        tracing::info!(?first_epoch, ?table_catalog, "initializing state table");
        state_table.init_epoch(first_epoch).await?;
        tracing::info!(
            ?first_epoch,
            ?staging_table_catalog,
            "initializing staging table"
        );
        staging_table.init_epoch(first_epoch).await?;
        tracing::info!(?first_epoch, "initialization done");

        #[for_await]
        for msg in input {
            let msg = msg?;
            materialize_cache.evict();

            let msg = match msg {
                Message::Watermark(w) => Message::Watermark(w),
                Message::Chunk(chunk) => {
                    metrics
                        .materialize_input_row_count
                        .inc_by(chunk.cardinality() as u64);

                    debug_assert!(conflict_behavior == ConflictBehavior::Overwrite);

                    // TODO: add the do_not_handle_conflict optimization from MaterializeExecutor
                    {
                        if chunk.cardinality() == 0 {
                            // empty chunk
                            continue;
                        }
                        let (data_chunk, ops) = chunk.into_parts();

                        if cfg!(debug_assertions) {
                            // The input of a refreshable table should be append only - a batch source.
                            assert!(ops.iter().all(|op| matches!(op, Op::Insert)));
                        }

                        if state_table.value_indices().is_some() {
                            // TODO(st1page): when materialize partial columns(), we should
                            // construct some columns in the pk
                            panic!(
                                "materialize executor with data check can not handle only materialize partial columns"
                            )
                        };

                        let values = data_chunk.serialize();

                        let key_chunk = data_chunk.project(state_table.pk_indices());

                        let pks = {
                            let mut pks = vec![vec![]; data_chunk.capacity()];
                            key_chunk
                                .rows_with_holes()
                                .zip_eq_fast(pks.iter_mut())
                                .for_each(|(r, vnode_and_pk)| {
                                    if let Some(r) = r {
                                        state_table.pk_serde().serialize(r, vnode_and_pk);
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
                            .filter_map(|(((op, k), v), vis)| vis.then_some((*op, k, v)))
                            .collect_vec();

                        let change_buffer = materialize_cache
                            .handle(row_ops, &state_table, conflict_behavior, &metrics)
                            .await?;

                        match generate_output(change_buffer, data_types.clone())? {
                            Some(output_chunk) => {
                                state_table.write_chunk(output_chunk.clone());
                                state_table.try_flush().await?;

                                if is_refreshing {
                                    let pk_chunk = output_chunk.project(staging_table.pk_indices());

                                    // only keep insert ops
                                    let mut new_vis =
                                        BitmapBuilder::zeroed(pk_chunk.visibility().len());
                                    let mut new_ops = vec![];
                                    for (i, (op, row)) in pk_chunk.rows().enumerate() {
                                        match op {
                                            Op::Insert => {
                                                new_ops.push(Op::Insert);
                                                new_vis.set(i, true);
                                            }
                                            Op::UpdateInsert => {
                                                // convert update insert to insert
                                                new_ops.push(Op::Insert);
                                                new_vis.set(i, true);
                                            }
                                            Op::UpdateDelete => {
                                                // filter out update delete
                                                new_ops.push(Op::Delete);
                                                new_vis.set(i, false);
                                            }
                                            Op::Delete => {
                                                // FIXME: is it possible to meet DELETE operations?
                                                // Yes, for DML...
                                                // But we should ban DML during refresh.
                                                unreachable!()
                                            }
                                        }
                                    }
                                    // emit chunk if vis is not empty. i.e., some splits finished backfilling.
                                    let new_vis = new_vis.finish();
                                    if new_vis.count_ones() != 0 {
                                        let new_chunk = StreamChunk::with_visibility(
                                            new_ops,
                                            pk_chunk.columns().to_vec(),
                                            new_vis,
                                        );
                                        staging_table.write_chunk(new_chunk);
                                    }

                                    staging_table.try_flush().await?;
                                }

                                Message::Chunk(output_chunk)
                            }
                            None => continue,
                        }
                    }
                }

                Message::Barrier(b) => {
                    // Check if this barrier contains a RefreshStart mutation for our table
                    if let Some(m) = b.mutation.as_deref() {
                        tracing::info!(?m, "barrier mutation received");
                    }
                    match b.mutation.as_deref() {
                        Some(Mutation::RefreshStart {
                            table_id: refresh_table_id,
                            associated_source_id: _,
                        }) if *refresh_table_id == table_id => {
                            is_refreshing = true;
                            tracing::info!(table_id = %refresh_table_id, "RefreshStart barrier received");
                        }
                        Some(Mutation::LoadFinish {
                            associated_source_id: load_finish_source_id,
                        }) => {
                            // if load_finish_source_id.table_id() == associated_source_id
                            debug_assert!(is_refreshing);
                            // Reset the refreshing flag
                            is_refreshing = false;

                            tracing::info!(
                                %load_finish_source_id,
                                "LoadFinish received, starting data replacement"
                            );

                            // Execute the atomic swap from staging to main table
                            Self::on_load_finish(&mut state_table, &mut staging_table, table_id)
                                .await?;

                            tracing::info!(
                                %load_finish_source_id,
                                "Data replacement complete, refresh cycle finished"
                            );
                        }
                        _ => {}
                    }

                    // TODO: consider op_consistency_level
                    let post_commit = state_table.commit(b.epoch).await?;
                    if !post_commit.inner().is_consistent_op() {
                        assert_eq!(conflict_behavior, ConflictBehavior::Overwrite);
                    }
                    let update_vnode_bitmap = b.as_update_vnode_bitmap(actor_context.id);

                    let staging_post_commit = staging_table.commit(b.epoch).await?;
                    let staging_update_vnode_bitmap = b.as_update_vnode_bitmap(actor_context.id);

                    let b_epoch = b.epoch;
                    yield Message::Barrier(b);

                    // Update the vnode bitmap for the state table if asked.
                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale
                    {
                        materialize_cache.lru_cache.clear();
                    }
                    staging_post_commit
                        .post_yield_barrier(staging_update_vnode_bitmap)
                        .await?;

                    metrics.materialize_current_epoch.set(b_epoch.curr as i64);

                    continue;
                }
            };
            yield msg;
        }
    }

    /// `DELETE FROM original_table WHERE pk NOT IN (SELECT pk FROM tmp_table)`
    async fn on_load_finish(
        state_table: &mut StateTableInner<S, SD>,
        staging_table: &mut StateTableInner<S, SD>,
        table_id: TableId,
    ) -> StreamExecutorResult<()> {
        tracing::info!(table_id = %table_id, "Starting table replacement operation");

        // Naive version: Lookup Join: for each row in the main table, check if it exists in the staging table.
        // TODO: implement sort-merge join
        for vnode in state_table.vnodes().clone().iter_vnodes() {
            let pk_range: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, Bound::Unbounded);

            let mut rows_to_delete = vec![];

            {
                let iter = state_table
                    .iter_keyed_row_with_vnode(
                        vnode,
                        &pk_range,
                        PrefetchOptions::prefetch_for_large_range_scan(),
                    )
                    .await?;
                pin_mut!(iter);

                #[for_await]
                for kv in iter {
                    let kv: KeyedRow<Bytes> = kv?;
                    let key = state_table
                        .pk_serde()
                        .deserialize(kv.key())
                        .context("failed to deserialize pk")?;
                    let staging_row = staging_table.get_row(&key).await?;
                    if staging_row.is_none() {
                        rows_to_delete.push(kv.row().clone());
                    }
                }
            }

            for row in rows_to_delete {
                state_table.delete(row);
            }
        }

        // Clear the staging table for the next refresh
        staging_table.clear_all_rows().await?;
        tracing::info!(table_id = %table_id, "Staging table cleared and diff applied");

        Ok(())
    }
}

impl<S: StateStore, SD: ValueRowSerde> Execute for RefreshableMaterializeExecutor<S, SD> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
