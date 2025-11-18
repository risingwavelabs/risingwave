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
                    .table(StreamingJob::Table)
                    // MySQL does not support default value for long text column.
                    // So we have to make it nullable here.
                    .add_column(ColumnDef::new(StreamingJob::ConfigOverride).rw_long_text(manager))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .drop_column(StreamingJob::ConfigOverride)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    ConfigOverride,
}
