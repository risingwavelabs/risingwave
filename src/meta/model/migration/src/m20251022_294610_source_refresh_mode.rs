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
                    .table(Source::Table)
                    // leave binary here for future extension to complex refresh mode, like timer-based refresh mode
                    .add_column(
                        ColumnDef::new(Source::RefreshMode)
                            .rw_binary(manager)
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .drop_column(Source::RefreshMode)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Source {
    Table,
    RefreshMode,
}
