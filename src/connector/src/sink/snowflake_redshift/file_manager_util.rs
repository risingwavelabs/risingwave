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

use risingwave_meta_model::snowflake_redshift_sink_file::{ActiveModel, Column, Entity, Model};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use thiserror_ext::AsReport;

use crate::sink::Result;

pub async fn insert_file_paths_with_sink_id(
    sink_id: u32,
    db: DatabaseConnection,
    end_epoch: u64,
    file_paths: Vec<String>,
) -> Result<()> {
    let m = ActiveModel {
        sink_id: Set(sink_id as i32),
        end_epoch: Set(end_epoch.try_into().unwrap()),
        file_paths: Set(risingwave_meta_model::StringArray::from(file_paths)),
    };
    match Entity::insert(m).exec(&db).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!("Error inserting into system table: {:?}", e.as_report());
            Err(e.into())
        }
    }
}

pub async fn get_file_paths_by_sink_id(
    db: &DatabaseConnection,
    sink_id: u32,
) -> Result<(Vec<String>, u64)> {
    let models: Vec<Model> = Entity::find()
        .filter(Column::SinkId.eq(sink_id as i32))
        .all(db)
        .await?;

    let mut file_paths = Vec::new();
    let mut max_end_epoch = 0;

    for model in models {
        file_paths.extend(model.file_paths.into_string_array());
        max_end_epoch = max_end_epoch.max(model.end_epoch as u64);
    }

    Ok((file_paths, max_end_epoch))
}

pub async fn delete_row_by_sink_id_and_end_epoch(
    db: &DatabaseConnection,
    sink_id: u32,
    end_epoch: u64,
) -> Result<()> {
    let end_epoch_i64: i64 = end_epoch.try_into().unwrap();
    match Entity::delete_many()
        .filter(Column::SinkId.eq(sink_id))
        .filter(Column::EndEpoch.lte(end_epoch_i64))
        .exec(db)
        .await
    {
        Ok(result) => {
            let deleted_count = result.rows_affected;

            if deleted_count == 0 {
                tracing::info!(
                    "Sink id = {}: no item deleted in snowflake redshift file system table, end_epoch < {}.",
                    sink_id,
                    end_epoch
                );
            } else {
                tracing::info!(
                    "Sink id = {}: deleted item in snowflake redshift file system table, end_epoch < {}.",
                    sink_id,
                    end_epoch
                );
            }
            Ok(())
        }
        Err(e) => {
            tracing::error!(
                "Sink id = {}: error deleting from snowflake redshift file system table: {:?}",
                sink_id,
                e.as_report()
            );
            Err(e.into())
        }
    }
}
