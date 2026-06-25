use sea_orm_migration::prelude::*;

use crate::m20230908_072257_init::Object;
use crate::sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

const FK_NAME: &str = "FK_pending_sink_state_object_id";
const NEW_TABLE: &str = "pending_sink_state_new";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // The model declares the `sink_id -> object(oid)` cascade but the foreign key was never
        // created, leaking rows on DROP SINK. Add the key and clean up the leaked rows.
        // https://github.com/risingwavelabs/risingwave/issues/26044
        let backend = manager.get_database_backend();
        match backend {
            DatabaseBackend::MySql | DatabaseBackend::Postgres => {
                manager
                    .get_connection()
                    .execute(Statement::from_string(
                        backend,
                        "DELETE FROM pending_sink_state WHERE sink_id NOT IN (SELECT oid FROM object)",
                    ))
                    .await?;
                manager
                    .alter_table(
                        Table::alter()
                            .table(PendingSinkState::Table)
                            .add_foreign_key(&object_foreign_key())
                            .to_owned(),
                    )
                    .await?;
            }
            DatabaseBackend::Sqlite => {
                // SQLite can't `ALTER TABLE ADD FOREIGN KEY`; recreate the table with it instead.
                recreate_table(manager, true).await?;
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
                recreate_table(manager, false).await?;
            }
        }
        Ok(())
    }
}

fn object_foreign_key() -> TableForeignKey {
    TableForeignKey::new()
        .name(FK_NAME)
        .from_tbl(PendingSinkState::Table)
        .from_col(PendingSinkState::SinkId)
        .to_tbl(Object::Table)
        .to_col(Object::Oid)
        .on_delete(ForeignKeyAction::Cascade)
        .to_owned()
}

/// Recreate `pending_sink_state` with (or without) the object foreign key, dropping leaked rows
/// during the copy. The swap runs in one transaction so it stays atomic and re-runnable even
/// though the migrator opens none for SQLite. SQLite-only: it can't alter foreign keys in place.
async fn recreate_table(manager: &SchemaManager<'_>, with_foreign_key: bool) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    // TODO(#26046): on sea-orm 2.0, drop this manual transaction and override
    // `MigrationTrait::use_transaction()` to return `Some(true)` instead.
    let txn = manager.get_connection().begin().await?;
    {
        let txn_manager = SchemaManager::new(&txn);

        let mut create = Table::create();
        create
            .table(Alias::new(NEW_TABLE))
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
                    .rw_binary(&txn_manager)
                    .null(),
            )
            .col(
                ColumnDef::new(PendingSinkState::SchemaChange)
                    .rw_binary(&txn_manager)
                    .null(),
            )
            .primary_key(
                Index::create()
                    .col(PendingSinkState::SinkId)
                    .col(PendingSinkState::Epoch),
            );
        if with_foreign_key {
            create.foreign_key(
                ForeignKey::create()
                    .name(FK_NAME)
                    .from_tbl(Alias::new(NEW_TABLE))
                    .from_col(PendingSinkState::SinkId)
                    .to_tbl(Object::Table)
                    .to_col(Object::Oid)
                    .on_delete(ForeignKeyAction::Cascade),
            );
        }
        txn_manager.create_table(create).await?;

        txn.execute(Statement::from_string(
            backend,
            "INSERT INTO pending_sink_state_new \
             (sink_id, epoch, sink_state, metadata, schema_change) \
             SELECT sink_id, epoch, sink_state, metadata, schema_change \
             FROM pending_sink_state WHERE sink_id IN (SELECT oid FROM object)",
        ))
        .await?;

        txn_manager
            .drop_table(Table::drop().table(PendingSinkState::Table).to_owned())
            .await?;
        txn_manager
            .rename_table(
                Table::rename()
                    .table(Alias::new(NEW_TABLE), PendingSinkState::Table)
                    .to_owned(),
            )
            .await?;
    }
    txn.commit().await?;
    Ok(())
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
