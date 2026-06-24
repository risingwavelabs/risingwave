use sea_orm_migration::prelude::*;

use crate::m20230908_072257_init::Object;
use crate::sea_orm::{DatabaseBackend, Statement};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

const FK_NAME: &str = "FK_pending_sink_state_object_id";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();
        let backend = manager.get_database_backend();

        // Clean up rows leaked by sink drops before the foreign key existed, so the key
        // below passes validation. See https://github.com/risingwavelabs/risingwave/issues/26044.
        conn.execute(Statement::from_string(
            backend,
            "DELETE FROM pending_sink_state WHERE sink_id NOT IN (SELECT oid FROM object)",
        ))
        .await?;

        match backend {
            DatabaseBackend::MySql | DatabaseBackend::Postgres => {
                manager
                    .alter_table(
                        Table::alter()
                            .table(PendingSinkState::Table)
                            .add_foreign_key(
                                TableForeignKey::new()
                                    .name(FK_NAME)
                                    .from_tbl(PendingSinkState::Table)
                                    .from_col(PendingSinkState::SinkId)
                                    .to_tbl(Object::Table)
                                    .to_col(Object::Oid)
                                    .on_delete(ForeignKeyAction::Cascade),
                            )
                            .to_owned(),
                    )
                    .await?;
            }
            DatabaseBackend::Sqlite => {
                // SQLite cannot add a foreign key to an existing table; recreate it instead.
                // SQLite is not for prod usage, so recreating the table is fine.
                let tmp_table = Alias::new("pending_sink_state_new");
                manager
                    .create_table(
                        Table::create()
                            .table(tmp_table.clone())
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
                                    .null(),
                            )
                            .col(
                                ColumnDef::new(PendingSinkState::SchemaChange)
                                    .rw_binary(manager)
                                    .null(),
                            )
                            .primary_key(
                                Index::create()
                                    .col(PendingSinkState::SinkId)
                                    .col(PendingSinkState::Epoch),
                            )
                            .foreign_key(
                                &mut ForeignKey::create()
                                    .name(FK_NAME)
                                    .from(tmp_table.clone(), PendingSinkState::SinkId)
                                    .to(Object::Table, Object::Oid)
                                    .on_delete(ForeignKeyAction::Cascade)
                                    .to_owned(),
                            )
                            .to_owned(),
                    )
                    .await?;
                conn.execute(Statement::from_string(
                    backend,
                    "INSERT INTO pending_sink_state_new \
                        (sink_id, epoch, sink_state, metadata, schema_change) \
                     SELECT sink_id, epoch, sink_state, metadata, schema_change \
                     FROM pending_sink_state",
                ))
                .await?;
                manager
                    .drop_table(Table::drop().table(PendingSinkState::Table).to_owned())
                    .await?;
                manager
                    .rename_table(
                        Table::rename()
                            .table(tmp_table, PendingSinkState::Table)
                            .to_owned(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        match manager.get_database_backend() {
            DatabaseBackend::MySql | DatabaseBackend::Postgres => {
                manager
                    .alter_table(
                        Table::alter()
                            .table(PendingSinkState::Table)
                            .drop_foreign_key(Alias::new(FK_NAME))
                            .to_owned(),
                    )
                    .await?;
            }
            DatabaseBackend::Sqlite => {
                // No-op: the key is part of the table definition and SQLite is not for prod usage.
            }
        }
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
    SchemaChange,
}
