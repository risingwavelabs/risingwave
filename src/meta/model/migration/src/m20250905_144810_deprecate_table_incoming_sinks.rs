use sea_orm_migration::prelude::{Table as MigrationTable, *};
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Table::IncomingSinks)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

#[derive(DeriveIden)]
#[allow(clippy::enum_variant_names)]
enum Table {
    Table,
    IncomingSinks,
}
