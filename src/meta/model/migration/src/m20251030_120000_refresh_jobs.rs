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
                    .col(
                        ColumnDef::new(RefreshJob::JobCreateTime)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(ColumnDef::new(RefreshJob::LastTriggerTime).date_time())
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
                            .to(Table::Table, Table::TableId)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                sea_orm_migration::prelude::Table::drop()
                    .table(RefreshJob::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum RefreshJob {
    #[sea_orm(iden = "refresh_job")]
    Table,
    TableId,
    JobCreateTime,
    LastTriggerTime,
    TriggerIntervalSecs,
    CurrentStatus,
}

#[derive(DeriveIden)]
enum Table {
    #[sea_orm(iden = "table")]
    Table,
    TableId,
}
