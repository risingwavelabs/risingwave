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
                        ColumnDef::new(HummockGcHistory::ObjectId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockGcHistory::MarkDeleteAt)
                            .date_time()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .table(HummockGcHistory::Table)
                    .name("idx_hummock_gc_history_mark_delete_at")
                    .col(HummockGcHistory::MarkDeleteAt)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, HummockGcHistory);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum HummockGcHistory {
    Table,
    ObjectId,
    MarkDeleteAt,
}
