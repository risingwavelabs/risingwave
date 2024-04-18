use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Source::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .add_column(ColumnDef::new(Source::UdfExpr).binary())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(ColumnDef::new(Table::UdfExpr).binary())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .drop_column(Alias::new(Source::UdfExpr.to_string()))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Alias::new(Table::UdfExpr.to_string()))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Source {
    Table,
    UdfExpr,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    UdfExpr,
}
