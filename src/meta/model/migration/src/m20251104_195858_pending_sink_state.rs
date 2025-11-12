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

            let select_sql = "
                SELECT sink_id, end_epoch, metadata, snapshot_id, committed
                FROM exactly_once_iceberg_sink_metadata
            ";
            let old_rows: Vec<OldRow> = OldRow::find_by_statement(Statement::from_string(
                conn.get_database_backend(),
                select_sql.to_owned(),
            ))
            .all(conn)
            .await?;

            if !old_rows.is_empty() {
                // Batched insert
                let mut insert = Query::insert();
                insert.into_table(PendingSinkState::Table).columns([
                    PendingSinkState::SinkId,
                    PendingSinkState::Epoch,
                    PendingSinkState::SinkState,
                    PendingSinkState::Metadata,
                ]);

                for r in old_rows {
                    let sink_state = if r.committed { "COMMITTED" } else { "PENDING" };
                    let combined_metadata = transform_metadata(r.metadata, r.snapshot_id);
                    insert.values_panic([
                        r.sink_id.into(),
                        r.end_epoch.into(),
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
}
