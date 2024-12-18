use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Connection::Table)
                    .add_column(ColumnDef::new(Connection::Params).binary().not_null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Connection::Table)
                    .drop_column(Connection::Params)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Connection {
    Table,
    Params,
}
