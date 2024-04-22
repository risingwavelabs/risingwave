use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(ColumnDef::new(Function::Runtime).string())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(ColumnDef::new(Function::FunctionType).string())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Alias::new(Function::Runtime.to_string()))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Alias::new(Function::FunctionType.to_string()))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    Runtime,
    FunctionType,
}
