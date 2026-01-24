use sea_orm_migration::prelude::{Table as MigrationTable, *};

macro_rules! col {
    ($name:expr) => {
        ColumnDef::new($name).integer().not_null().default(256) // compat vnode count
    };
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(col!(Table::VnodeCount))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Fragment::Table)
                    .add_column(col!(Fragment::VnodeCount))
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(StreamingJob::Table)
                    .add_column(col!(StreamingJob::MaxParallelism))
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
            .await?;

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(StreamingJob::Table)
                    .drop_column(StreamingJob::MaxParallelism)
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

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    MaxParallelism,
}
