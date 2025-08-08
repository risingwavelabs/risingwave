use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add refresh_state column
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .add_column(
                        ColumnDef::new(TableEnum::RefreshState)
                            .integer()
                            .not_null()
                            .default(0), // REFRESH_STATE_IDLE = 0
                    )
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
