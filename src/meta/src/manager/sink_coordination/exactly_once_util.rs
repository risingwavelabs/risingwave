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

use risingwave_meta_model::pending_sink_state::{self};
use risingwave_meta_model::{Epoch, SinkId, SinkSchemachange};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionTrait,
};
use thiserror_ext::AsReport;

// This file contains methods for accessing system tables in the meta store with two-phase commit sink support.

pub async fn persist_pre_commit_metadata(
    db: &DatabaseConnection,
    sink_id: SinkId,
    epoch: u64,
    commit_metadata: Vec<u8>,
    schema_change: Option<&PbSinkSchemaChange>,
) -> anyhow::Result<()> {
    let schema_change = schema_change.map(Into::into);
    let m = pending_sink_state::ActiveModel {
        sink_id: Set(sink_id),
        epoch: Set(epoch as Epoch),
        sink_state: Set(pending_sink_state::SinkState::Pending),
        metadata: Set(commit_metadata),
        schema_change: Set(schema_change),
    };
    match pending_sink_state::Entity::insert(m).exec(db).await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error inserting into exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn commit_and_prune_epoch(
    db: &DatabaseConnection,
    sink_id: SinkId,
    epoch: u64,
    prev_epoch: Option<u64>,
) -> anyhow::Result<()> {
    let txn = db.begin().await?;
    pending_sink_state::Entity::update(pending_sink_state::ActiveModel {
        sink_id: Set(sink_id),
        epoch: Set(epoch as Epoch),
        sink_state: Set(pending_sink_state::SinkState::Committed),
        ..Default::default()
    })
    .exec(&txn)
    .await?;

    if let Some(prev_epoch) = prev_epoch {
        pending_sink_state::Entity::delete_many()
            .filter(
                pending_sink_state::Column::SinkId
                    .eq(sink_id)
                    .and(pending_sink_state::Column::Epoch.eq(prev_epoch as Epoch)),
            )
            .exec(&txn)
            .await?;
    }

    match txn.commit().await {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error marking item to committed exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn clean_aborted_records(
    db: &DatabaseConnection,
    sink_id: SinkId,
    aborted_epochs: Vec<u64>,
) -> anyhow::Result<()> {
    match pending_sink_state::Entity::delete_many()
        .filter(
            pending_sink_state::Column::SinkId
                .eq(sink_id)
                .and(pending_sink_state::Column::Epoch.is_in(aborted_epochs)),
        )
        .exec(db)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error deleting records from exactly once system table: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

pub async fn list_sink_states_ordered_by_epoch(
    db: &DatabaseConnection,
    sink_id: SinkId,
) -> anyhow::Result<
    Vec<(
        u64,
        pending_sink_state::SinkState,
        Vec<u8>,
        Option<PbSinkSchemaChange>,
    )>,
> {
    let rows: Vec<(
        Epoch,
        pending_sink_state::SinkState,
        Vec<u8>,
        Option<SinkSchemachange>,
    )> = match pending_sink_state::Entity::find()
        .select_only()
        .columns([
            pending_sink_state::Column::Epoch,
            pending_sink_state::Column::SinkState,
            pending_sink_state::Column::Metadata,
            pending_sink_state::Column::SchemaChange,
        ])
        .filter(pending_sink_state::Column::SinkId.eq(sink_id))
        .order_by(pending_sink_state::Column::Epoch, Order::Asc)
        .into_tuple()
        .all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("Error querying pending sink states: {:?}", e.as_report());
            return Err(e.into());
        }
    };

    Ok(rows
        .into_iter()
        .map(|(epoch, state, metadata, schema_change)| {
            (
                epoch as u64,
                state,
                metadata,
                schema_change.map(|v| v.to_protobuf()),
            )
        })
        .collect())
}
