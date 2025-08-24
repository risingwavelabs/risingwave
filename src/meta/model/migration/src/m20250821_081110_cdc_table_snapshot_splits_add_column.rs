use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(CdcTableSnapshotSplits::Table)
                    .add_column(
                        ColumnDef::new(CdcTableSnapshotSplits::IsBackfillFinished)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(CdcTableSnapshotSplits::Table)
                    .drop_column(CdcTableSnapshotSplits::IsBackfillFinished)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum CdcTableSnapshotSplits {
    Table,
    IsBackfillFinished,
}
