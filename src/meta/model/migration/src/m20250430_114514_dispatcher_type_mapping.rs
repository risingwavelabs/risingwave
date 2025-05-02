use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(FragmentRelation::Table)
                    .add_column(
                        ColumnDef::new(FragmentRelation::OutputTypeMapping)
                            .rw_binary(manager)
                            .not_null()
                            .default(Expr::value(b"".to_vec())),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(FragmentRelation::Table)
                    .drop_column(FragmentRelation::OutputTypeMapping)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum FragmentRelation {
    Table,
    OutputTypeMapping,
}
