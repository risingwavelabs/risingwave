use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TableRefill::Table)
                    .col(
                        ColumnDef::new(TableRefill::TableId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TableRefill::Mode).string().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_table_refill_table")
                            .from(TableRefill::Table, TableRefill::TableId)
                            .to(TableEnum::Table, TableEnum::TableId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(TableRefill::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum TableRefill {
    #[sea_orm(iden = "table_refill")]
    Table,
    TableId,
    Mode,
}

#[derive(DeriveIden)]
enum TableEnum {
    #[sea_orm(iden = "table")]
    Table,
    TableId,
}
