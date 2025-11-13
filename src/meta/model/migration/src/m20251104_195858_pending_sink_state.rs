use sea_orm_migration::prelude::*;

use crate::drop_tables;
use crate::sea_orm::{FromQueryResult, Statement};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[derive(FromQueryResult, Debug)]
struct OldRow {
    pub sink_id: i32,
    pub end_epoch: i64,
    pub metadata: Vec<u8>,
    pub snapshot_id: i64,
    pub committed: bool,
}

// Logic referenced from src/connector/src/sink/iceberg/mod.rs
fn transform_metadata(metadata: Vec<u8>, snapshot_id: i64) -> Vec<u8> {
    let mut write_results_bytes: Vec<Vec<u8>> = serde_json::from_slice(&metadata).unwrap();
    let snapshot_id_bytes: Vec<u8> = snapshot_id.to_le_bytes().to_vec();
    write_results_bytes.push(snapshot_id_bytes);
    serde_json::to_vec(&write_results_bytes).unwrap()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PendingSinkState::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PendingSinkState::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::Epoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::SinkState)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::Metadata)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(PendingSinkState::SinkId)
                            .col(PendingSinkState::Epoch),
                    )
                    .to_owned(),
            )
            .await?;

        if manager
            .has_table(ExactlyOnceIcebergSinkMetadata::Table.to_string())
            .await?
        {
            let conn = manager.get_connection();

            let (sql, values) = Query::select()
                .columns([
                    ExactlyOnceIcebergSinkMetadata::SinkId,
                    ExactlyOnceIcebergSinkMetadata::EndEpoch,
                    ExactlyOnceIcebergSinkMetadata::Metadata,
                    ExactlyOnceIcebergSinkMetadata::SnapshotId,
                    ExactlyOnceIcebergSinkMetadata::Committed,
                ])
                .from(ExactlyOnceIcebergSinkMetadata::Table)
                .to_owned()
                .build_any(&*conn.get_database_backend().get_query_builder());

            let rows = conn
                .query_all(Statement::from_sql_and_values(
                    conn.get_database_backend(),
                    sql,
                    values,
                ))
                .await?;

            if !rows.is_empty() {
                let mut insert = Query::insert();
                insert
                    .into_table(PendingSinkState::Table)
                    .columns([
                        PendingSinkState::SinkId,
                        PendingSinkState::Epoch,
                        PendingSinkState::SinkState,
                        PendingSinkState::Metadata,
                    ])
                    .on_conflict(
                        sea_query::OnConflict::columns([
                            PendingSinkState::SinkId,
                            PendingSinkState::Epoch,
                        ])
                        .do_nothing()
                        .to_owned(),
                    );
                for row in rows {
                    let OldRow {
                        sink_id,
                        end_epoch,
                        metadata,
                        snapshot_id,
                        committed,
                    } = OldRow::from_query_result(&row, "")?;
                    let sink_state = if committed { "COMMITTED" } else { "PENDING" };
                    let combined_metadata = transform_metadata(metadata, snapshot_id);
                    insert.values_panic([
                        sink_id.into(),
                        end_epoch.into(),
                        sink_state.into(),
                        combined_metadata.into(),
                    ]);
                }

                manager.exec_stmt(insert.to_owned()).await?;
            }
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, PendingSinkState);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum PendingSinkState {
    Table,
    SinkId,
    Epoch,
    SinkState,
    Metadata,
}

#[derive(DeriveIden)]
enum ExactlyOnceIcebergSinkMetadata {
    Table,
    SinkId,
    EndEpoch,
    Metadata,
    SnapshotId,
    Committed,
}
