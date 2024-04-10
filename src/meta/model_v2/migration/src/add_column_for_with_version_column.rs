use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(VersonColumn::Table)
                    .add_column(ColumnDef::new(VersonColumn::VersionColumnIndex).string())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts

        manager
            .drop_table(Table::drop().table(VersonColumn::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum VersonColumn {
    Table,
    VersionColumnIndex,
}
