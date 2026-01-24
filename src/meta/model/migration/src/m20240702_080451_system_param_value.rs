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
                        .table(SystemParameter::Table)
                        .modify_column(ColumnDef::new(SystemParameter::Value).text().not_null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DbBackend::MySql {
            manager
                .alter_table(
                    Table::alter()
                        .table(SystemParameter::Table)
                        .modify_column(ColumnDef::new(SystemParameter::Value).string().not_null())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }
}

#[derive(DeriveIden)]
enum SystemParameter {
    Table,
    Value,
}
