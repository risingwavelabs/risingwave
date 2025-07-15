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
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_connector::source::cdc::external::{
    CdcTableSnapshotSplitOption, CdcTableType, ExternalTableConfig, ExternalTableReader,
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
            backfill_num_rows_per_split: Some(per_table_options.backfill_num_rows_per_split),
        }
    } else {
        return Ok(());
    };
    let table_type = CdcTableType::from_properties(&table_desc.connect_properties);
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
fn is_parallelized_backfill_enabled_cdc_scan_fragment(fragment: &Fragment) -> bool {
    match &fragment.nodes.node_body {
        Some(NodeBody::StreamCdcScan(node)) => is_parallelized_backfill_enabled(node),
        Some(NodeBody::Project(_)) => {
            for input in &fragment.nodes.input {
                if let Some(NodeBody::StreamCdcScan(node)) = &input.node_body {
                    return is_parallelized_backfill_enabled(node);
                }
            }
            false
        }
        _ => false,
    }
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
    table_fragments: impl Iterator<Item = &StreamJobFragments>,
    meta_store: &SqlMetaStore,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    let mut assignments = HashMap::default();
    for job in table_fragments {
        let mut stream_scan_fragments = job
            .fragments
            .values()
            .filter(|f| is_parallelized_backfill_enabled_cdc_scan_fragment(f))
            .collect_vec();
        if stream_scan_fragments.is_empty() {
            continue;
        }
        let stream_scan_fragment = stream_scan_fragments.swap_remove(0);
        let assignment = assign_cdc_table_snapshot_splits_impl(
            job.stream_job_id.table_id,
            stream_scan_fragment
                .actors
                .iter()
                .map(|a| a.actor_id)
                .collect(),
            meta_store,
        )
        .await?;
        assignments.extend(assignment);
    }
    Ok(assignments)
}

pub(crate) async fn assign_cdc_table_snapshot_splits_pairs(
    table_id_actor_ids: impl IntoIterator<Item = (u32, HashSet<u32>)>,
    meta_store: &SqlMetaStore,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    let mut assignments = HashMap::default();
    for (table_id, actor_ids) in table_id_actor_ids {
        assignments
            .extend(assign_cdc_table_snapshot_splits_impl(table_id, actor_ids, meta_store).await?);
    }
    Ok(assignments)
}

pub(crate) async fn assign_cdc_table_snapshot_splits_impl(
    table_id: u32,
    actor_ids: HashSet<u32>,
    meta_store: &SqlMetaStore,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    if actor_ids.is_empty() {
        return Err(anyhow::anyhow!(
            "A stream scan fragment should have at least 1 actor".to_owned()
        )
        .into());
    }
    let mut assignments = HashMap::default();
    let mut splits = try_get_cdc_table_snapshot_splits(table_id, meta_store).await?;
    if splits.is_empty() {
        tracing::error!(
            "Expect at least one CDC table snapshot splits, 0 was found. Fall back to one (null, null) split."
        );
        // Fall back to one (null, null) split to avoid missing read.
        let null = OwnedRow::new(vec![None]).value_serialize();
        splits = vec![CdcTableSnapshotSplitRaw {
            split_id: i64::MAX,
            left_bound_inclusive: null.clone(),
            right_bound_exclusive: null,
        }];
    };
    let splits_per_actor = splits.len().div_ceil(actor_ids.len());
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
    let splits: Vec<(i64, Vec<u8>, Vec<u8>)> = cdc_table_snapshot_split::Entity::find()
        .select_only()
        .columns([
            cdc_table_snapshot_split::Column::SplitId,
            cdc_table_snapshot_split::Column::Left,
            cdc_table_snapshot_split::Column::Right,
        ])
        .filter(
            cdc_table_snapshot_split::Column::TableId
                .eq(TryInto::<TableId>::try_into(table_id).unwrap()),
        )
        .into_tuple()
        .all(&meta_store.conn)
        .await?;
    let splits: Vec<_> = splits
        .into_iter()
        // The try_init_parallel_cdc_table_snapshot_splits ensures that split with a larger split_id will always have a larger left bound.
        // Assigning consecutive splits to the same actor enables potential optimization in CDC backfill executor.
        .sorted_by_key(|(split_id, _, _)| *split_id)
        .map(
            |(split_id, left_bound_inclusive, right_bound_exclusive)| CdcTableSnapshotSplitRaw {
                split_id,
                left_bound_inclusive,
                right_bound_exclusive,
            },
        )
        .collect();
    Ok(splits)
}
