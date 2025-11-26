use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(ColumnDef::new(Table::CleanWatermarkIndices).json())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Table::CleanWatermarkIndices)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Table {
    Table,
    CleanWatermarkIndices,
}
