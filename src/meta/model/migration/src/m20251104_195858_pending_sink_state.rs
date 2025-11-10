use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(PendingSinkState::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(PendingSinkState::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::Epoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::SinkState)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(PendingSinkState::Metadata)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(PendingSinkState::SinkId)
                            .col(PendingSinkState::Epoch),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, PendingSinkState);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum PendingSinkState {
    Table,
    SinkId,
    Epoch,
    SinkState,
    Metadata,
}
