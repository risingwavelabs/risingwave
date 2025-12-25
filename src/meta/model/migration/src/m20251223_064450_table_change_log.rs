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
                    .table(HummockTableChangeLog::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockTableChangeLog::TableId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTableChangeLog::CheckpointEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTableChangeLog::NonCheckpointEpochs)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTableChangeLog::NewValueSst)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTableChangeLog::OldValueSst)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(HummockTableChangeLog::TableId)
                            .col(HummockTableChangeLog::CheckpointEpoch),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_hummock_table_change_log_table_id")
                            .from(HummockTableChangeLog::Table, HummockTableChangeLog::TableId)
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
            .drop_table(Table::drop().table(HummockTableChangeLog::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum HummockTableChangeLog {
    Table,
    TableId,
    CheckpointEpoch,
    NonCheckpointEpochs,
    NewValueSst,
    OldValueSst,
}
