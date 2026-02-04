use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .add_column(
                        ColumnDef::new(StreamingJob::AdaptiveParallelismStrategy)
                            .string()
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
                    .table(StreamingJob::Table)
                    .drop_column(StreamingJob::AdaptiveParallelismStrategy)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    AdaptiveParallelismStrategy,
}
