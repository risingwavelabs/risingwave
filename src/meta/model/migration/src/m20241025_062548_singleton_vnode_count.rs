use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
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

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "cannot rollback singleton vnode count migration".to_owned(),
        ))?
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
