use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(BoundedJob::Table)
                    .col(ColumnDef::new(BoundedJob::JobId).integer().not_null())
                    .col(ColumnDef::new(BoundedJob::SourceId).integer().not_null())
                    .col(ColumnDef::new(BoundedJob::CompletedAt).big_integer())
                    .primary_key(
                        Index::create()
                            .col(BoundedJob::JobId)
                            .col(BoundedJob::SourceId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_bounded_job_job_id")
                            .from(BoundedJob::Table, BoundedJob::JobId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_bounded_job_source_id")
                            .from(BoundedJob::Table, BoundedJob::SourceId)
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(BoundedJob::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum BoundedJob {
    Table,
    JobId,
    SourceId,
    CompletedAt,
}

#[derive(DeriveIden)]
enum Object {
    Table,
    Oid,
}
