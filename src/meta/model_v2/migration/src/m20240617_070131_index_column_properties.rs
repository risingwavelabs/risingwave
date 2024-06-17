use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Index::Table)
                    .add_column(ColumnDef::new(Index::IndexColumnProperties).binary())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Index::Table)
                    .drop_column(Alias::new(Index::IndexColumnProperties.to_string()))
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Index {
    Table,
    IndexColumnProperties,
}
