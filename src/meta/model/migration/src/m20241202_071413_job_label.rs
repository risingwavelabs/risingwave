use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

pub const DEFAULT_STREAMING_JOB_LABEL: &str = "default";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .add_column(
                        ColumnDef::new(StreamingJob::Label)
                            .string()
                            .default(DEFAULT_STREAMING_JOB_LABEL.to_string())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .drop_column(StreamingJob::Label)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    Label,
}
