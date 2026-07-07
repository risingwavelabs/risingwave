use sea_orm_migration::prelude::*;

use crate::drop_tables;
use crate::m20230908_072257_init::Object;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

const FK_NAME: &str = "FK_iceberg_pk_index_pending_remap_object_id";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Durable record of a pk-index compaction remap that has committed its overwrite but whose
        // writer-side pk-index remap may not have been applied yet. Persisted before the remap
        // barrier mutation is enqueued, so a crash between the overwrite commit and the mutation
        // delivery is recoverable: on meta startup every un-satisfied row re-triggers its mutation.
        // Composite PK `(sink_id, remap_id)` supports more than one concurrent pending remap per
        // sink (two compactions on disjoint input file sets).
        manager
            .create_table(
                Table::create()
                    .table(IcebergPkIndexPendingRemap::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IcebergPkIndexPendingRemap::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergPkIndexPendingRemap::RemapId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergPkIndexPendingRemap::MappingPaths)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergPkIndexPendingRemap::InputFiles)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(IcebergPkIndexPendingRemap::SinkId)
                            .col(IcebergPkIndexPendingRemap::RemapId),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name(FK_NAME)
                            .from_tbl(IcebergPkIndexPendingRemap::Table)
                            .from_col(IcebergPkIndexPendingRemap::SinkId)
                            .to_tbl(Object::Table)
                            .to_col(Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, IcebergPkIndexPendingRemap);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum IcebergPkIndexPendingRemap {
    Table,
    SinkId,
    RemapId,
    MappingPaths,
    InputFiles,
}
