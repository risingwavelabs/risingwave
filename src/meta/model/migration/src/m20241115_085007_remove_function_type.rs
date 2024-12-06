use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Function::Table)
                    .drop_column(Function::FunctionType)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Function::Table)
                    .add_column(ColumnDef::new(Function::FunctionType).string())
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    FunctionType,
}
