use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        macro_rules! assert_not_has_tables {
            ($manager:expr, $( $table:ident ),+) => {
                $(
                    assert!(
                        !$manager
                            .has_table($table::Table.to_string())
                            .await?
                    );
                )+
            };
        }
        assert_not_has_tables!(
            manager,
            CompactionTask,
            CompactionConfig,
            CompactionStatus,
            HummockPinnedVersion,
            HummockPinnedSnapshot,
            HummockVersionDelta,
            HummockVersionStats
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
                            .json_binary()
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
                    .col(ColumnDef::new(CompactionConfig::Config).json_binary())
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
                    .col(ColumnDef::new(CompactionStatus::Status).json_binary())
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
                    .col(ColumnDef::new(HummockVersionDelta::GroupDeltas).json_binary())
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
                    .col(ColumnDef::new(HummockVersionDelta::GcObjectIds).json_binary())
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

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        macro_rules! drop_tables {
            ($manager:expr, $( $table:ident ),+) => {
                $(
                    $manager
                        .drop_table(
                            Table::drop()
                                .table($table::Table)
                                .if_exists()
                                .cascade()
                                .to_owned(),
                        )
                        .await?;
                )+
            };
        }
        drop_tables!(
            manager,
            CompactionTask,
            CompactionConfig,
            CompactionStatus,
            HummockPinnedVersion,
            HummockPinnedSnapshot,
            HummockVersionDelta,
            HummockVersionStats
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
    GroupDeltas,
    MaxCommittedEpoch,
    SafeEpoch,
    TrivialMove,
    GcObjectIds,
}

#[derive(DeriveIden)]
enum HummockVersionStats {
    Table,
    Id,
    Stats,
}
