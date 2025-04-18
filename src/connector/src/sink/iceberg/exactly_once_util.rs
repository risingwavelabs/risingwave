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

use risingwave_meta_model::exactly_once_iceberg_sink::{self, Column, Entity, Model};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, Order, PaginatorTrait, QueryFilter, QueryOrder,
    Set,
};
use thiserror_ext::AsReport;

use crate::sink::Result;

pub async fn persist_pre_commit_metadata(
    sink_id: u32,
    db: DatabaseConnection,
    start_epoch: u64,
    end_epoch: u64,
    pre_commit_metadata: Vec<u8>,
    snapshot_id: i64,
) -> Result<()> {
    let m = exactly_once_iceberg_sink::ActiveModel {
        sink_id: Set(sink_id as i32),
        end_epoch: Set(end_epoch.try_into().unwrap()),
        start_epoch: Set(start_epoch.try_into().unwrap()),
        metadata: Set(pre_commit_metadata),
        committed: Set(false),
        snapshot_id: Set(snapshot_id),
    };
    match exactly_once_iceberg_sink::Entity::insert(m).exec(&db).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("Error inserting into system table: {:?}", e.as_report());
            Err(e.into())
        }
    }
}

pub async fn mark_row_is_committed_by_sink_id_and_end_epoch(
    db: &DatabaseConnection,
    sink_id: u32,
    end_epoch: u64,
) -> Result<()> {
    match Entity::update(exactly_once_iceberg_sink::ActiveModel {
        sink_id: Set(sink_id as i32),
        end_epoch: Set(end_epoch.try_into().unwrap()),
        committed: Set(true),
        ..Default::default()
    })
    .exec(db)
    .await
    {
        Ok(_) => {
            tracing::info!(
                "Sink id = {}: mark written data status to committed, end_epoch = {}.",
                sink_id,
                end_epoch
            );
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "Error marking item to committed from iceberg exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn delete_row_by_sink_id_and_end_epoch(
    db: &DatabaseConnection,
    sink_id: u32,
    end_epoch: u64,
) -> Result<()> {
    let end_epoch_i64: i64 = end_epoch.try_into().unwrap();
    match Entity::delete_many()
        .filter(Column::SinkId.eq(sink_id))
        .filter(Column::EndEpoch.lt(end_epoch_i64))
        .exec(db)
        .await
    {
        Ok(result) => {
            let deleted_count = result.rows_affected;

            if deleted_count == 0 {
                tracing::info!(
                    "Sink id = {}: no item deleted in iceberg exactly once system table, end_epoch < {}.",
                    sink_id,
                    end_epoch
                );
            } else {
                tracing::info!(
                    "Sink id = {}: deleted item in iceberg exactly once system table, end_epoch < {}.",
                    sink_id,
                    end_epoch
                );
            }
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "Sink id = {}: error deleting from iceberg exactly once system table: {:?}",
                sink_id,
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn iceberg_sink_has_pre_commit_metadata(
    db: &DatabaseConnection,
    sink_id: u32,
) -> Result<bool> {
    match exactly_once_iceberg_sink::Entity::find()
        .filter(exactly_once_iceberg_sink::Column::SinkId.eq(sink_id as i32))
        .count(db)
        .await
    {
        Ok(count) => Ok(count > 0),
        Err(e) => {
            tracing::error!(
                "Error querying pre-commit metadata from system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn get_pre_commit_info_by_sink_id(
    db: &DatabaseConnection,
    sink_id: u32,
) -> Result<Vec<(u64, Vec<u8>, i64, bool)>> {
    let models: Vec<Model> = Entity::find()
        .filter(Column::SinkId.eq(sink_id as i32))
        .order_by(Column::EndEpoch, Order::Asc)
        .all(db)
        .await?;

    let mut result: Vec<(u64, Vec<u8>, i64, bool)> = Vec::new();

    for model in models {
        result.push((
            model.end_epoch.try_into().unwrap(),
            model.metadata,
            model.snapshot_id,
            model.committed,
        ));
    }

    Ok(result)
}
