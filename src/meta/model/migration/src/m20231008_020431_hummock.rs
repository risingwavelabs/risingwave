use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;
use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(
            manager,
            CompactionTask,
            CompactionConfig,
            CompactionStatus,
            HummockPinnedVersion,
            HummockPinnedSnapshot,
            HummockVersionDelta,
            HummockVersionStats,
            HummockSequence
        );

        manager
            .create_table(
                Table::create()
                    .table(CompactionTask::Table)
                    .col(
                        ColumnDef::new(CompactionTask::Id)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(CompactionTask::Task)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(CompactionTask::ContextId)
                            .integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(CompactionConfig::Table)
                    .col(
                        ColumnDef::new(CompactionConfig::CompactionGroupId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(CompactionConfig::Config).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(CompactionStatus::Table)
                    .col(
                        ColumnDef::new(CompactionStatus::CompactionGroupId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(CompactionStatus::Status).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockPinnedVersion::Table)
                    .col(
                        ColumnDef::new(HummockPinnedVersion::ContextId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockPinnedVersion::MinPinnedId)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockPinnedSnapshot::Table)
                    .col(
                        ColumnDef::new(HummockPinnedSnapshot::ContextId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockPinnedSnapshot::MinPinnedSnapshot)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockVersionDelta::Table)
                    .col(
                        ColumnDef::new(HummockVersionDelta::Id)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockVersionDelta::PrevId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockVersionDelta::MaxCommittedEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockVersionDelta::SafeEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockVersionDelta::TrivialMove)
                            .boolean()
                            .not_null(),
                    )
                    .col(ColumnDef::new(HummockVersionDelta::FullVersionDelta).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockVersionStats::Table)
                    .col(
                        ColumnDef::new(HummockVersionStats::Id)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockVersionStats::Stats)
                            .json_binary()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(HummockSequence::Table)
                    .col(
                        ColumnDef::new(HummockSequence::Name)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockSequence::Seq)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(
            manager,
            CompactionTask,
            CompactionConfig,
            CompactionStatus,
            HummockPinnedVersion,
            HummockPinnedSnapshot,
            HummockVersionDelta,
            HummockVersionStats,
            HummockSequence
        );
        Ok(())
    }
}

#[derive(DeriveIden)]
enum CompactionTask {
    Table,
    Id,
    Task,
    ContextId,
}

#[derive(DeriveIden)]
enum CompactionConfig {
    Table,
    CompactionGroupId,
    Config,
}

#[derive(DeriveIden)]
enum CompactionStatus {
    Table,
    CompactionGroupId,
    Status,
}

#[derive(DeriveIden)]
enum HummockPinnedVersion {
    Table,
    ContextId,
    MinPinnedId,
}

#[derive(DeriveIden)]
enum HummockPinnedSnapshot {
    Table,
    ContextId,
    MinPinnedSnapshot,
}

#[derive(DeriveIden)]
enum HummockVersionDelta {
    Table,
    Id,
    PrevId,
    MaxCommittedEpoch,
    SafeEpoch,
    TrivialMove,
    FullVersionDelta,
}

#[derive(DeriveIden)]
enum HummockVersionStats {
    Table,
    Id,
    Stats,
}

#[derive(DeriveIden)]
enum HummockSequence {
    Table,
    Name,
    Seq,
}
