use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                sea_query::Table::alter()
                    .table(Table::Table)
                    .add_column(ColumnDef::new(Table::VectorIndexInfo).rw_binary(manager))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                sea_query::Table::alter()
                    .table(Table::Table)
                    .drop_column(Table::VectorIndexInfo)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Table {
    Table,
    VectorIndexInfo,
}
