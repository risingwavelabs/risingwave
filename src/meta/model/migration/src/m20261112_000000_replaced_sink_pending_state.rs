use sea_orm_migration::prelude::*;

use crate::drop_tables;
use crate::m20230908_072257_init::Object;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Exactly-once epochs that a sink removed by `REPLACE SINK` had pre-committed but not yet
        // committed to the external system. Each row belongs to the successor sink's object, so
        // dropping the successor discards them.
        manager
            .create_table(
                Table::create()
                    .table(ReplacedSinkPendingState::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::Epoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::SuccessorSinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::Sink)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::Metadata)
                            .rw_binary(manager)
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ReplacedSinkPendingState::SchemaChange)
                            .rw_binary(manager)
                            .null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(ReplacedSinkPendingState::SinkId)
                            .col(ReplacedSinkPendingState::Epoch),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("FK_replaced_sink_pending_state_successor_object_id")
                            .from(
                                ReplacedSinkPendingState::Table,
                                ReplacedSinkPendingState::SuccessorSinkId,
                            )
                            .to(Object::Table, Object::Oid)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, ReplacedSinkPendingState);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum ReplacedSinkPendingState {
    Table,
    SinkId,
    Epoch,
    SuccessorSinkId,
    Sink,
    Metadata,
    SchemaChange,
}
