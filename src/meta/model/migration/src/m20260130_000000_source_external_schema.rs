use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::m20230908_072257_init::Source;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                MigrationTable::create()
                    .table(SourceExternalSchema::Table)
                    .col(
                        ColumnDef::new(SourceExternalSchema::SourceId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(SourceExternalSchema::Version)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(SourceExternalSchema::Content)
                            .string()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_source_external_schema_source_id")
                            .from(
                                SourceExternalSchema::Table,
                                SourceExternalSchema::SourceId,
                            )
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
        manager
            .drop_table(
                MigrationTable::drop()
                    .table(SourceExternalSchema::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum SourceExternalSchema {
    Table,
    SourceId,
    Version,
    Content,
}
