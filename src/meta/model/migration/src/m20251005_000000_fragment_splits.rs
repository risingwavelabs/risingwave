use sea_orm_migration::prelude::*;

use crate::drop_tables;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(FragmentSplits::Table)
                    .col(
                        ColumnDef::new(FragmentSplits::FragmentId)
                            .integer()
                            .primary_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(FragmentSplits::Splits).rw_binary(manager))
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_fragment_splits_fragment_oid")
                            .from(FragmentSplits::Table, FragmentSplits::FragmentId)
                            .to(Fragment::Table, Fragment::FragmentId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, FragmentSplits);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum FragmentSplits {
    Table,
    FragmentId,
    Splits,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
}
