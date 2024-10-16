use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(HummockGcHistory::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockGcHistory::SstId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockGcHistory::MarkDeleteAt)
                            .date_time()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, HummockGcHistory);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum HummockGcHistory {
    Table,
    SstId,
    MarkDeleteAt,
}
