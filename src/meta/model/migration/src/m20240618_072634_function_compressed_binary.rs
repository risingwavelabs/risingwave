use sea_orm_migration::prelude::*;

use crate::sea_orm::{DatabaseBackend, DbBackend, Statement};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Fix mismatch column `compressed_binary` type and do data migration
        match manager.get_database_backend() {
            DbBackend::MySql => {
                // Creating function with compressed binary will fail in previous version, so we can
                // safely assume that the column is always empty and we can just modify the column type
                // without any data migration.
                manager
                    .alter_table(
                        Table::alter()
                            .table(Function::Table)
                            .modify_column(
                                ColumnDef::new(Function::CompressedBinary).rw_binary(manager),
                            )
                            .to_owned(),
                    )
                    .await?;
            }
            DbBackend::Postgres => {
                manager.get_connection().execute(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "ALTER TABLE function ALTER COLUMN compressed_binary TYPE bytea USING compressed_binary::bytea",
                )).await?;
            }
            DbBackend::Sqlite => {
                // Sqlite does not support modifying column type, so we need to do data migration and column renaming.
                // Note that: all these DDLs are not transactional, so if some of them fail, we need to manually run it again.
                let conn = manager.get_connection();
                conn.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "ALTER TABLE function ADD COLUMN compressed_binary_new BLOB",
                ))
                .await?;
                conn.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "UPDATE function SET compressed_binary_new = compressed_binary",
                ))
                .await?;
                conn.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "ALTER TABLE function DROP COLUMN compressed_binary",
                ))
                .await?;
                conn.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "ALTER TABLE function RENAME COLUMN compressed_binary_new TO compressed_binary",
                ))
                .await?;
            }
        }

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // DO nothing, the operations in `up` are idempotent and required to fix the column type mismatch.
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    CompressedBinary,
}
