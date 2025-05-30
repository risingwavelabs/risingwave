use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Database::Table)
                    .add_column(ColumnDef::new(Database::BarrierIntervalMs).integer().null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Database::Table)
                    .add_column(
                        ColumnDef::new(Database::CheckpointFrequency)
                            .big_integer()
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
                    .table(Database::Table)
                    .drop_column(Database::BarrierIntervalMs)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Database::Table)
                    .drop_column(Database::CheckpointFrequency)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Database {
    Table,
    BarrierIntervalMs,
    CheckpointFrequency,
}
