use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PendingSinkState::Table)
                    .add_column(
                        ColumnDef::new(PendingSinkState::SchemaChange)
                            .rw_binary(manager)
                            .null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(PendingSinkState::Table)
                    .drop_column(PendingSinkState::SchemaChange)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum PendingSinkState {
    Table,
    SchemaChange,
}
