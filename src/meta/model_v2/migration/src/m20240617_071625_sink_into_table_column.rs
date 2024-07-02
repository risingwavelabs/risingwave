use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .add_column(ColumnDef::new(Sink::OriginalTargetColumns).binary())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .drop_column(Sink::OriginalTargetColumns)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    OriginalTargetColumns,
}
