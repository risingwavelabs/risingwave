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
                    .table(SourceSplits::Table)
                    .col(
                        ColumnDef::new(SourceSplits::SourceId)
                            .integer()
                            .primary_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(SourceSplits::Splits).rw_binary(manager))
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_splits_source_oid")
                            .from(SourceSplits::Table, SourceSplits::SourceId)
                            .to(Source::Table, Source::SourceId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, SourceSplits);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum SourceSplits {
    Table,
    SourceId,
    Splits,
}

#[derive(DeriveIden)]
enum Source {
    Table,
    SourceId,
}
