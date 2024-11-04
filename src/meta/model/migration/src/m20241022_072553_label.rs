use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(WorkerProperty::Table)
                    .add_column(ColumnDef::new(WorkerProperty::Label).string())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(
                        ColumnDef::new(Table::BackfillType)
                            .string()
                            .default("REGULAR"),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::Label)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Table::BackfillType)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    Label,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    BackfillType,
}
