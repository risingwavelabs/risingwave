// Copyright 2026 RisingWave Labs
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
use risingwave_meta_model::{Epoch, SinkId, SinkSchemachange, iceberg_pk_index_pending_remap};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, Order, QueryFilter, QueryOrder, QuerySelect, Set,
    TransactionTrait,
};
use thiserror_ext::AsReport;

// Helpers for accessing the `pending_sink_state` system table used by exactly-once sink coordinators
// (both the generic sink coordinator and the Iceberg pk-index sink coordinator).

pub async fn persist_pre_commit_metadata(
    db: &DatabaseConnection,
    sink_id: SinkId,
    epoch: u64,
    commit_metadata: Option<Vec<u8>>,
    schema_change: Option<&PbSinkSchemaChange>,
) -> anyhow::Result<()> {
    fail::fail_point!("iceberg_v3_persist_pre_commit_fail", |_| Err(
        anyhow::anyhow!("injected: iceberg_v3_persist_pre_commit_fail")
    ));
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
    fail::fail_point!("iceberg_v3_commit_prune_fail", |_| Err(anyhow::anyhow!(
        "injected: iceberg_v3_commit_prune_fail"
    )));
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

type PendingSinkStateRow = (
    Epoch,
    pending_sink_state::SinkState,
    Option<Vec<u8>>,
    Option<SinkSchemachange>,
);

pub async fn list_sink_states_ordered_by_epoch(
    db: &DatabaseConnection,
    sink_id: SinkId,
) -> anyhow::Result<
    Vec<(
        u64,
        pending_sink_state::SinkState,
        Option<Vec<u8>>,
        Option<PbSinkSchemaChange>,
    )>,
> {
    let rows: Vec<PendingSinkStateRow> = match pending_sink_state::Entity::find()
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

// Helpers for accessing the `iceberg_pk_index_pending_remap` system table used by the Iceberg
// pk-index sink coordinator to durably record pending compaction remaps (so they can be re-triggered
// on meta restart) and self-heal them once the remap is provably applied.

/// A recovered pending remap: the `remap_id` plus the decoded `mapping_paths` (row-provenance NDJSON
/// paths for the writer to remap against) and `input_files` (removed input DATA/DV paths). The
/// coordinator-side window fix-up (`remap_in_window_position_deletes`) matches stray in-flight
/// position-deletes against these removed input DATA file paths and remaps them via `mapping_paths`.
pub struct PendingRemapRecord {
    pub remap_id: i64,
    pub mapping_paths: Vec<String>,
    pub input_files: Vec<String>,
}

/// Durably record a pending pk-index compaction remap. Called right after the compaction overwrite
/// commits and BEFORE the `IcebergPkIndexRemap` barrier mutation is enqueued, so a crash in between
/// is recoverable. Idempotent on `(sink_id, remap_id)`: a re-issued insert for an already-recorded
/// remap is a no-op.
pub async fn persist_pending_remap(
    db: &DatabaseConnection,
    sink_id: SinkId,
    remap_id: i64,
    mapping_paths: &[String],
    input_files: &[String],
) -> anyhow::Result<()> {
    let m = iceberg_pk_index_pending_remap::ActiveModel {
        sink_id: Set(sink_id),
        remap_id: Set(remap_id),
        mapping_paths: Set(serde_json::to_vec(mapping_paths)?),
        input_files: Set(serde_json::to_vec(input_files)?),
    };
    let mut on_conflict = sea_orm::sea_query::OnConflict::columns([
        iceberg_pk_index_pending_remap::Column::SinkId,
        iceberg_pk_index_pending_remap::Column::RemapId,
    ]);
    // Workaround to support MySQL for `DO NOTHING`: a no-op update of a PK column.
    on_conflict.update_column(iceberg_pk_index_pending_remap::Column::SinkId);
    match iceberg_pk_index_pending_remap::Entity::insert(m)
        .on_conflict(on_conflict)
        .do_nothing()
        .exec(db)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error inserting into iceberg_pk_index_pending_remap: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

/// List all durable pending remaps for `sink_id`, decoding the JSON-encoded path blobs.
pub async fn list_pending_remaps(
    db: &DatabaseConnection,
    sink_id: SinkId,
) -> anyhow::Result<Vec<PendingRemapRecord>> {
    let rows = match iceberg_pk_index_pending_remap::Entity::find()
        .filter(iceberg_pk_index_pending_remap::Column::SinkId.eq(sink_id))
        .order_by(iceberg_pk_index_pending_remap::Column::RemapId, Order::Asc)
        .all(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!("Error querying pending remaps: {:?}", e.as_report());
            return Err(e.into());
        }
    };

    rows.into_iter()
        .map(|row| {
            Ok(PendingRemapRecord {
                remap_id: row.remap_id,
                mapping_paths: serde_json::from_slice(&row.mapping_paths)?,
                input_files: serde_json::from_slice(&row.input_files)?,
            })
        })
        .collect()
}

/// Delete a durable pending remap once it is provably applied (self-heal) or superseded. Deleting a
/// missing `(sink_id, remap_id)` is a harmless no-op.
pub async fn clear_pending_remap(
    db: &DatabaseConnection,
    sink_id: SinkId,
    remap_id: i64,
) -> anyhow::Result<()> {
    match iceberg_pk_index_pending_remap::Entity::delete_many()
        .filter(
            iceberg_pk_index_pending_remap::Column::SinkId
                .eq(sink_id)
                .and(iceberg_pk_index_pending_remap::Column::RemapId.eq(remap_id)),
        )
        .exec(db)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::error!(
                "Error deleting from iceberg_pk_index_pending_remap: {:?}",
                e.as_report()
            );
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_meta_model::object::ObjectType;
    use risingwave_meta_model::{ObjectId, SinkId, UserId, object};

    use super::*;
    use crate::manager::MetaSrvEnv;

    fn remap_ids(records: &[PendingRemapRecord]) -> Vec<i64> {
        let mut ids: Vec<i64> = records.iter().map(|r| r.remap_id).collect();
        ids.sort_unstable();
        ids
    }

    /// Insert a minimal `SINK` object so the `iceberg_pk_index_pending_remap.sink_id -> object.oid`
    /// foreign key is satisfied. `owner_id = 1` (the `root` user seeded by the init migration).
    async fn seed_sink_object(db: &DatabaseConnection, oid: u32) {
        let obj = object::ActiveModel {
            oid: Set(ObjectId::new(oid)),
            obj_type: Set(ObjectType::Sink),
            owner_id: Set(UserId::new(1)),
            schema_id: Set(None),
            database_id: Set(None),
            initialized_at: Default::default(),
            created_at: Default::default(),
            initialized_at_cluster_version: Set(None),
            created_at_cluster_version: Set(None),
        };
        object::Entity::insert(obj).exec(db).await.unwrap();
    }

    /// A durable pending remap is cleared ONLY by [`clear_pending_remap`] (driven by the writer's
    /// `REMAP_DONE` signal), and that clear is scoped to a single `(sink_id, remap_id)` and
    /// idempotent. There is deliberately no snapshot-based self-heal: a record survives every list
    /// (i.e. every recovery re-trigger) until its own `REMAP_DONE` arrives.
    #[tokio::test]
    async fn test_pending_remap_cleared_only_by_remap_id_signal() {
        let env = MetaSrvEnv::for_test().await;
        let db = env.meta_store_ref().conn.clone();
        seed_sink_object(&db, 9001).await;
        let sink_id = SinkId::new(9001);

        persist_pending_remap(&db, sink_id, 100, &["m-100".into()], &["in-a".into()])
            .await
            .unwrap();
        persist_pending_remap(&db, sink_id, 200, &["m-200".into()], &["in-b".into()])
            .await
            .unwrap();
        assert_eq!(
            remap_ids(&list_pending_remaps(&db, sink_id).await.unwrap()),
            vec![100, 200]
        );

        // A `REMAP_DONE` for remap 100 clears exactly that record; the disjoint remap 200 survives
        // (it is NOT cleared merely because the overwrite already dropped its input files).
        clear_pending_remap(&db, sink_id, 100).await.unwrap();
        assert_eq!(
            remap_ids(&list_pending_remaps(&db, sink_id).await.unwrap()),
            vec![200]
        );

        // Idempotent: a duplicate `REMAP_DONE` for the already-cleared record is a harmless no-op.
        clear_pending_remap(&db, sink_id, 100).await.unwrap();
        assert_eq!(
            remap_ids(&list_pending_remaps(&db, sink_id).await.unwrap()),
            vec![200]
        );

        // The surviving record is removed only by its own `REMAP_DONE`.
        clear_pending_remap(&db, sink_id, 200).await.unwrap();
        assert!(list_pending_remaps(&db, sink_id).await.unwrap().is_empty());
    }

    /// Persisting the same `(sink_id, remap_id)` twice (e.g. a re-driven compaction report) does not
    /// duplicate the durable record.
    #[tokio::test]
    async fn test_persist_pending_remap_idempotent() {
        let env = MetaSrvEnv::for_test().await;
        let db = env.meta_store_ref().conn.clone();
        seed_sink_object(&db, 9002).await;
        let sink_id = SinkId::new(9002);

        persist_pending_remap(&db, sink_id, 7, &["a".into()], &["b".into()])
            .await
            .unwrap();
        persist_pending_remap(&db, sink_id, 7, &["a".into()], &["b".into()])
            .await
            .unwrap();
        assert_eq!(list_pending_remaps(&db, sink_id).await.unwrap().len(), 1);
    }
}
