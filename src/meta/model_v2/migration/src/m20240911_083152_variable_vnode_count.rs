use sea_orm_migration::prelude::{Table as MigrationTable, *};

const VNODE_COUNT: i32 = 256;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(
                        ColumnDef::new(Table::VnodeCount)
                            .integer()
                            .default(VNODE_COUNT),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Fragment::Table)
                    .add_column(
                        ColumnDef::new(Fragment::VnodeCount)
                            .integer()
                            .default(VNODE_COUNT),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Table::VnodeCount)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Fragment::Table)
                    .drop_column(Fragment::VnodeCount)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    VnodeCount,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    VnodeCount,
}
