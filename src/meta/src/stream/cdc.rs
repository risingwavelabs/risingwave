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

use std::collections::HashMap;
use std::iter;
use std::time::Duration;

use anyhow::Context;
use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::catalog::{FragmentTypeFlag, Schema};
use risingwave_common::row::Row;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_connector::source::cdc::CdcTableSnapshotSplitAssignment;
use risingwave_connector::source::cdc::external::{
    CdcTableSnapshotSplitOption, CdcTableType, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use risingwave_meta_model::{TableId, cdc_table_snapshot_split};
use risingwave_pb::plan_common::ExternalTableDesc;
use risingwave_pb::stream_plan::StreamCdcScanOptions;
use sea_orm::{EntityTrait, Set, TransactionTrait};

use crate::MetaResult;
use crate::controller::SqlMetaStore;
use crate::model::StreamJobFragments;

/// A CDC table snapshot splits can only be successfully initialized once.
/// Subsequent attempts to write to the metastore with the same primary key will be rejected.
pub(crate) async fn try_init_parallel_cdc_table_snapshot_splits(
    table_id: u32,
    table_desc: &ExternalTableDesc,
    meta_store: &SqlMetaStore,
    per_table_options: &Option<StreamCdcScanOptions>,
) -> MetaResult<()> {
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
    let split_options = if let Some(per_table_options) = per_table_options {
        CdcTableSnapshotSplitOption {
            backfill_num_rows_per_split: Some(per_table_options.backfill_num_rows_per_split),
        }
    } else {
        CdcTableSnapshotSplitOption {
            backfill_num_rows_per_split: None,
        }
    };
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
    // TODO(zw): make these configurable
    let insert_batch_size = 1000;
    let sleep_interval = 1000;
    let sleep_duration_millis = 500;

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
        if insert_batch.len() >= insert_batch_size {
            cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
                .exec(&txn)
                .await?;
        }
        if splits_num % sleep_interval == 0 {
            tokio::time::sleep(Duration::from_millis(sleep_duration_millis)).await;
        }
    }
    if !insert_batch.is_empty() {
        cdc_table_snapshot_split::Entity::insert_many(std::mem::take(&mut insert_batch))
            .exec(&txn)
            .await?;
    }
    txn.commit().await?;
    Ok(())
}

pub(crate) async fn assign_cdc_table_snapshot_splits(
    table_fragments: impl Iterator<Item = &StreamJobFragments>,
    meta_store: &SqlMetaStore,
) -> MetaResult<CdcTableSnapshotSplitAssignment> {
    let mut assignments = HashMap::default();
    for jobs in table_fragments {
        let mut stream_scan_fragments = jobs
            .fragments
            .values()
            .filter(|f| f.fragment_type_mask.contains(FragmentTypeFlag::StreamScan))
            .collect_vec();
        if stream_scan_fragments.is_empty() {
            continue;
        }
        if stream_scan_fragments.len() > 1 {
            return Err(anyhow::anyhow!(format!(
                "expect exactly one stream scan fragment, got: {:?}",
                jobs.fragments
            ))
            .into());
        }
        let stream_scan_fragment = stream_scan_fragments.swap_remove(0);
        let splits =
            try_get_cdc_table_snapshot_splits(jobs.stream_job_id.table_id, meta_store).await?;
        let Some(splits) = splits else {
            assert_eq!(
                stream_scan_fragment.actors.len(),
                1,
                "A stream scan fragment should only have one actor if there are no splits."
            );
            continue;
        };
        if stream_scan_fragment.actors.is_empty() {
            return Err(anyhow::anyhow!(
                "A stream scan fragment should have at least 1 actor".to_owned()
            )
            .into());
        }
        let splits_per_actor = splits.len().div_ceil(stream_scan_fragment.actors.len());
        for (actor_id, splits) in stream_scan_fragment
            .actors
            .iter()
            .map(|a| a.actor_id)
            .zip_eq_debug(
                splits
                    .into_iter()
                    .chunks(splits_per_actor)
                    .into_iter()
                    .map(|c| c.collect_vec())
                    .chain(iter::repeat(Vec::default()))
                    .take(stream_scan_fragment.actors.len()),
            )
        {
            assignments.insert(actor_id, splits);
        }
    }
    Ok(assignments)
}

async fn try_get_cdc_table_snapshot_splits(
    table_id: u32,
    meta_store: &SqlMetaStore,
) -> MetaResult<Option<Vec<CdcTableSnapshotSplitRaw>>> {
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
    if splits.is_empty() {
        return Ok(None);
    }
    Ok(Some(splits))
}
