use sea_orm_migration::prelude::{Table as MigrationTable, *};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add a new column to the `function` table
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(ColumnDef::new(Function::SecretRef).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Function::SecretRef)
                    .to_owned(),
            )
            .await?;
        
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    SecretRef,
}
