use sea_orm_migration::prelude::*;

use crate::m20230908_072257_init::Object;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(CdcTableSnapshotSplits::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(CdcTableSnapshotSplits::TableId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CdcTableSnapshotSplits::SplitId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CdcTableSnapshotSplits::Left)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CdcTableSnapshotSplits::Right)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(CdcTableSnapshotSplits::TableId)
                            .col(CdcTableSnapshotSplits::SplitId),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_cdc_table_snapshot_splits_table_id")
                            .from(
                                CdcTableSnapshotSplits::Table,
                                CdcTableSnapshotSplits::TableId,
                            )
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(CdcTableSnapshotSplits::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum CdcTableSnapshotSplits {
    Table,
    TableId,
    SplitId,
    Left,
    Right,
}
