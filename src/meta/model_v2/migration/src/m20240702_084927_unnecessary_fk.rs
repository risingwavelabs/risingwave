use sea_orm_migration::prelude::*;

use crate::sea_orm::DbBackend;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DbBackend::MySql {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("sink"))
                        .drop_foreign_key(Alias::new("FK_sink_connection_id"))
                        .drop_foreign_key(Alias::new("FK_sink_target_table_id"))
                        .to_owned(),
                )
                .await?;
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("source"))
                        .drop_foreign_key(Alias::new("FK_source_connection_id"))
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // Do nothing.
        Ok(())
    }
}
