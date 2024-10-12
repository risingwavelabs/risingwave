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

        // Fill vnode count with 1 for singleton tables.
        manager
            .exec_stmt(
                UpdateStatement::new()
                    .table(Table::Table)
                    .values([(Table::VnodeCount, Expr::value(1))])
                    .and_where(Expr::col(Table::DistributionKey).eq(Expr::value("[]")))
                    .and_where(Expr::col(Table::DistKeyInPk).eq(Expr::value("[]")))
                    .and_where(Expr::col(Table::VnodeColIndex).is_null())
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

        // Fill vnode count with 1 for singleton fragments.
        manager
            .exec_stmt(
                UpdateStatement::new()
                    .table(Fragment::Table)
                    .values([(Fragment::VnodeCount, Expr::value(1))])
                    .and_where(Expr::col(Fragment::DistributionType).eq(Expr::value("SINGLE")))
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
    DistributionType,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    VnodeCount,
    DistributionKey,
    DistKeyInPk,
    VnodeColIndex,
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    MaxParallelism,
}
