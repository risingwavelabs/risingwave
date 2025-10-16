use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Fragment::Table)
                    .add_column(
                        ColumnDef::new(Fragment::Parallelism)
                            .json_binary()
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Fragment::Table)
                    .drop_column(Fragment::Parallelism)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    Parallelism,
}
