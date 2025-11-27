use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add refresh_state column as string, default to "IDLE"
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .add_column(ColumnDef::new(TableEnum::RefreshState).string())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .drop_column(TableEnum::RefreshState)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum TableEnum {
    #[sea_orm(iden = "table")]
    Table,
    RefreshState,
}
