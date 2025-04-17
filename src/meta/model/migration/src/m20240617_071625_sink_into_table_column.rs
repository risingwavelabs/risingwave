use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::SubQueryStatement::SelectStatement;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .add_column(ColumnDef::new(Sink::OriginalTargetColumns).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        let stmt = Query::update()
            .table(Sink::Table)
            .value(
                Sink::OriginalTargetColumns,
                SimpleExpr::SubQuery(
                    None,
                    Box::new(SelectStatement(
                        Query::select()
                            .column((Table::Table, Table::Columns))
                            .from(Table::Table)
                            .and_where(
                                Expr::col((Table::Table, Table::TableId))
                                    .equals((Sink::Table, Sink::TargetTable)),
                            )
                            .to_owned(),
                    )),
                ),
            )
            .and_where(Expr::col((Sink::Table, Sink::TargetTable)).is_not_null())
            .to_owned();

        manager.exec_stmt(stmt).await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .drop_column(Sink::OriginalTargetColumns)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    TargetTable,
    OriginalTargetColumns,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    TableId,
    Columns,
}
