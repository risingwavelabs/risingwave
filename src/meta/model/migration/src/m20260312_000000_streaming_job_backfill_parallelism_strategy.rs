use sea_orm_migration::prelude::*;

// Legacy jobs never persisted a separate backfill adaptive strategy. During creation they either
// used an explicit fixed `backfill_parallelism` or fell back to the main job strategy. Starting
// this column as NULL preserves that old behavior because runtime recovery now resolves
// `backfill_adaptive_parallelism_strategy.or(adaptive_parallelism_strategy)`.
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
                        ColumnDef::new(StreamingJob::BackfillAdaptiveParallelismStrategy)
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
                    .drop_column(StreamingJob::BackfillAdaptiveParallelismStrategy)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    BackfillAdaptiveParallelismStrategy,
}
