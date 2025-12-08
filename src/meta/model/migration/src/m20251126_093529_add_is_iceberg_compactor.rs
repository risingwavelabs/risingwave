use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(
                        ColumnDef::new(WorkerProperty::IsIcebergCompactor)
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
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::IsIcebergCompactor)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    IsIcebergCompactor,
}
