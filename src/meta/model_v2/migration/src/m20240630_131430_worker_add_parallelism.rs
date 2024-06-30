use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(ColumnDef::new(WorkerProperty::Parallelism).integer())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::ParallelUnitIds)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::Parallelism)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(
                        ColumnDef::new(WorkerProperty::ParallelUnitIds)
                            .json_binary()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    Parallelism,
    ParallelUnitIds,
}
