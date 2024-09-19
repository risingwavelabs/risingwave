use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .add_column(ColumnDef::new(Source::RateLimit).integer())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Source::Table)
                    .drop_column(Source::RateLimit)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Source {
    Table,
    RateLimit,
}
