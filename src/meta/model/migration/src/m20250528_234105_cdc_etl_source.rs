use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .add_column(ColumnDef::new(Source::CdcEtlInfo).rw_binary(manager))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .drop_column(Source::CdcEtlInfo)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Source {
    Table,
    CdcEtlInfo,
}
