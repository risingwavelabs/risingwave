use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Index::Table)
                    .add_column(ColumnDef::new(Index::IndexColumnProperties).rw_binary(manager))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Index::Table)
                    .drop_column(Index::IndexColumnProperties)
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
