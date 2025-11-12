use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                sea_orm_migration::prelude::Table::create()
                    .table(RefreshJob::Table)
                    .col(
                        ColumnDef::new(RefreshJob::TableId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(RefreshJob::LastTriggerTime).big_integer())
                    .col(ColumnDef::new(RefreshJob::TriggerIntervalSecs).big_integer())
                    .col(
                        ColumnDef::new(RefreshJob::CurrentStatus)
                            .string()
                            .not_null()
                            .default("IDLE"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_refresh_job_table")
                            .from(RefreshJob::Table, RefreshJob::TableId)
                            .to(TableEnum::Table, TableEnum::TableId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        // revert changes from `src/meta/model/migration/src/m20250916_120000_add_refresh_fields.rs`
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .drop_column(TableEnum::RefreshState)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                sea_orm_migration::prelude::Table::drop()
                    .table(RefreshJob::Table)
                    .to_owned(),
            )
            .await?;

        // revert changes from `src/meta/model/migration/src/m20250916_120000_add_refresh_fields.rs`
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .add_column(ColumnDef::new(TableEnum::RefreshState).string())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum RefreshJob {
    #[sea_orm(iden = "refresh_job")]
    Table,
    TableId,
    LastTriggerTime,
    TriggerIntervalSecs,
    CurrentStatus,
}

#[derive(DeriveIden)]
enum TableEnum {
    #[sea_orm(iden = "table")]
    Table,
    TableId,
    RefreshState,
}
