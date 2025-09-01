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

use std::collections::{HashMap, HashSet};
use std::iter;
use std::time::Duration;

use anyhow::Context;
use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_connector::source::cdc::external::{
    CdcTableSnapshotSplitOption, ExternalCdcTableType, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};
use risingwave_connector::source::cdc::{CdcScanOptions, CdcTableSnapshotSplitAssignment};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use risingwave_meta_model::{TableId, cdc_table_snapshot_split};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamCdcScanNode, StreamCdcScanOptions};
use sea_orm::{EntityTrait, Set, TransactionTrait};

use crate::MetaResult;
use crate::controller::SqlMetaStore;
use crate::model::{Fragment, StreamJobFragments};

/// A CDC table snapshot splits can only be successfully initialized once.
/// Subsequent attempts to write to the metastore with the same primary key will be rejected.
pub(crate) async fn try_init_parallel_cdc_table_snapshot_splits(
    table_id: u32,
    table_desc: &ExternalTableDesc,
    meta_store: &SqlMetaStore,
    per_table_options: &Option<StreamCdcScanOptions>,
    insert_batch_size: u64,
    sleep_split_interval: u64,
    sleep_duration_millis: u64,
) -> MetaResult<()> {
    let split_options = if let Some(per_table_options) = per_table_options {
        if !CdcScanOptions::from_proto(per_table_options).is_parallelized_backfill() {
            return Ok(());
        }
        CdcTableSnapshotSplitOption {
            backfill_num_rows_per_split: per_table_options.backfill_num_rows_per_split,
            backfill_as_even_splits: per_table_options.backfill_as_even_splits,
            backfill_split_pk_column_index: per_table_options.backfill_split_pk_column_index,
        }
    } else {
        return Ok(());
    };
    let table_type = ExternalCdcTableType::from_properties(&table_desc.connect_properties);
    // Filter out additional columns to construct the external table schema
    let table_schema: Schema = table_desc
        .columns
        .iter()
        .filter(|col| {
            col.additional_column
                .as_ref()
                .is_none_or(|a_col| a_col.column_type.is_none())
        })
        .map(Into::into)
        .collect();
    let table_pk_indices = table_desc
        .pk
        .iter()
        .map(|k| k.column_index as usize)
        .collect_vec();
    let table_config = ExternalTableConfig::try_from_btreemap(
        table_desc.connect_properties.clone(),
        table_desc.secret_refs.clone(),
    )
    .context("failed to parse external table config")?;
    let schema_table_name = SchemaTableName::from_properties(&table_desc.connect_properties);
    let reader = table_type
        .create_table_reader(
            table_config,
            table_schema,
            table_pk_indices,
            schema_table_name,
        )
        .await?;
    let stream = reader.get_parallel_cdc_splits(split_options);
    let mut insert_batch = vec![];
    let mut splits_num = 0;
    let txn = meta_store.conn.begin().await?;
    pin_mut!(stream);
    #[for_await]
    for split in stream {
        let split: CdcTableSnapshotSplit = split?;
        splits_num += 1;
        insert_batch.push(cdc_table_snapshot_split::ActiveModel {
            table_id: Set(table_id.try_into().unwrap()),
            split_id: Set(split.split_id.to_owned()),
            left: Set(split.left_bound_inclusive.value_serialize()),
            right: Set(split.right_bound_exclusive.value_serialize()),
            is_backfill_finished: Set(0),
        });
        if insert_batch.len() >= insert_batch_size as usize {
            cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
                .exec(&txn)
                .await?;
        }
        if splits_num % sleep_split_interval == 0 {
            tokio::time::sleep(Duration::from_millis(sleep_duration_millis)).await;
        }
    }
    if !insert_batch.is_empty() {
        cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
            .exec(&txn)
            .await?;
    }
    txn.commit().await?;
    // TODO(zw): handle metadata backup&restore
    Ok(())
}

/// Returns true if the fragment is CDC scan and has parallelized backfill enabled.
pub(crate) fn is_parallelized_backfill_enabled_cdc_scan_fragment(fragment: &Fragment) -> bool {
    let mut b = false;
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::StreamCdcScan(node)) = &node.node_body {
            b = is_parallelized_backfill_enabled(node);
            false
        } else {
            true
        }
    });
    b
}

pub fn is_parallelized_backfill_enabled(node: &StreamCdcScanNode) -> bool {
    if let Some(options) = &node.options
        && CdcScanOptions::from_proto(options).is_parallelized_backfill()
    {
        return true;
    }
    false
}

pub(crate) async fn assign_cdc_table_snapshot_splits(
    original_table_id: u32,
    job: &StreamJobFragments,
    meta_store: &SqlMetaStore,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    let mut stream_scan_fragments = job
        .fragments
        .values()
        .filter(|f| is_parallelized_backfill_enabled_cdc_scan_fragment(f))
        .collect_vec();
    if stream_scan_fragments.is_empty() {
        return Ok(HashMap::default());
    }
    assert_eq!(
        stream_scan_fragments.len(),
        1,
        "Expect 1 scan fragment, {} was found.",
        stream_scan_fragments.len()
    );
    let stream_scan_fragment = stream_scan_fragments.swap_remove(0);
    assign_cdc_table_snapshot_splits_impl(
        original_table_id,
        stream_scan_fragment
            .actors
            .iter()
            .map(|a| a.actor_id)
            .collect(),
        meta_store,
        None,
    )
    .await
}

pub(crate) async fn assign_cdc_table_snapshot_splits_pairs(
    table_id_actor_ids: impl IntoIterator<Item = (u32, HashSet<u32>)>,
    meta_store: &SqlMetaStore,
    completed_cdc_job_ids: HashSet<u32>,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    let mut assignments = HashMap::default();
    for (table_id, actor_ids) in table_id_actor_ids {
        assignments.extend(
            assign_cdc_table_snapshot_splits_impl(
                table_id,
                actor_ids,
                meta_store,
                Some(&completed_cdc_job_ids),
            )
            .await?,
        );
    }
    Ok(assignments)
}

pub(crate) async fn assign_cdc_table_snapshot_splits_impl(
    table_id: u32,
    actor_ids: HashSet<u32>,
    meta_store: &SqlMetaStore,
    completed_cdc_job_ids: Option<&HashSet<u32>>,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    if actor_ids.is_empty() {
        return Err(anyhow::anyhow!("Expect at least 1 actor, 0 was found.").into());
    }
    // Try to avoid meta store access in try_get_cdc_table_snapshot_splits.
    let splits = if let Some(completed_cdc_job_ids) = completed_cdc_job_ids
        && completed_cdc_job_ids.contains(&table_id)
    {
        vec![single_merged_split()]
    } else {
        try_get_cdc_table_snapshot_splits(table_id, meta_store).await?
    };
    if splits.is_empty() {
        return Err(
            anyhow::anyhow!("Expect at least 1 CDC table snapshot splits, 0 was found.").into(),
        );
    }
    let splits_per_actor = splits.len().div_ceil(actor_ids.len());
    let mut assignments: HashMap<
        u32,
        Vec<risingwave_connector::source::CdcTableSnapshotSplitCommon<Vec<u8>>>,
        _,
    > = HashMap::default();
    for (actor_id, splits) in actor_ids.iter().copied().zip_eq_debug(
        splits
            .into_iter()
            .chunks(splits_per_actor)
            .into_iter()
            .map(|c| c.collect_vec())
            .chain(iter::repeat(Vec::default()))
            .take(actor_ids.len()),
    ) {
        assignments.insert(actor_id, splits);
    }
    Ok(assignments)
}

pub async fn try_get_cdc_table_snapshot_splits(
    table_id: u32,
    meta_store: &SqlMetaStore,
) -> MetaResult<Vec<CdcTableSnapshotSplitRaw>> {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect};
    let splits: Vec<(i64, Vec<u8>, Vec<u8>, i16)> = cdc_table_snapshot_split::Entity::find()
        .select_only()
        .columns([
            cdc_table_snapshot_split::Column::SplitId,
            cdc_table_snapshot_split::Column::Left,
            cdc_table_snapshot_split::Column::Right,
            cdc_table_snapshot_split::Column::IsBackfillFinished,
        ])
        .filter(
            cdc_table_snapshot_split::Column::TableId
                .eq(TryInto::<TableId>::try_into(table_id).unwrap()),
        )
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    let split_completed_count = splits
        .iter()
        .filter(|(_, _, _, is_backfill_finished)| *is_backfill_finished == 1)
        .count();
    assert!(
        split_completed_count <= 1,
        "split_completed_count = {}",
        split_completed_count
    );
    let is_backfill_finished = split_completed_count == 1;
    if is_backfill_finished && splits.len() != 1 {
        // CdcTableBackfillTracker::complete_job rewrites splits in a transaction.
        // This error should only happen when the meta store reads uncommitted data.
        tracing::error!(table_id, ?splits, "unexpected split count");
        bail!(
            "unexpected split count: table_id={table_id}, split_total_count={}, split_completed_count={split_completed_count}",
            splits.len()
        );
    }
    let splits: Vec<_> = splits
        .into_iter()
        // The try_init_parallel_cdc_table_snapshot_splits ensures that split with a larger split_id will always have a larger left bound.
        // Assigning consecutive splits to the same actor enables potential optimization in CDC backfill executor.
        .sorted_by_key(|(split_id, _, _, _)| *split_id)
        .map(
            |(split_id, left_bound_inclusive, right_bound_exclusive, _)| CdcTableSnapshotSplitRaw {
                split_id,
                left_bound_inclusive,
                right_bound_exclusive,
            },
        )
        .collect();
    Ok(splits)
}

fn single_merged_split() -> CdcTableSnapshotSplitRaw {
    CdcTableSnapshotSplitRaw {
        // TODO(zw): remove magic number
        split_id: 1,
        left_bound_inclusive: OwnedRow::new(vec![None]).value_serialize(),
        right_bound_exclusive: OwnedRow::new(vec![None]).value_serialize(),
    }
}
