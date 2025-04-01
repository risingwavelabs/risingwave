use sea_orm::DbBackend;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DbBackend::Postgres {
            manager
                .alter_table(
                    Table::alter()
                        .table(Function::Table)
                        .modify_column(ColumnDef::new(Function::Body).text().to_owned())
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // do nothing
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    Body,
}
