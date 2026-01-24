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
                    .table(HummockSstableInfo::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockSstableInfo::SstId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockSstableInfo::ObjectId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockSstableInfo::SstableInfo)
                            .rw_binary(manager)
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockTimeTravelVersion::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockTimeTravelVersion::VersionId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelVersion::Version)
                            .rw_binary(manager)
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockTimeTravelDelta::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockTimeTravelDelta::VersionId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelDelta::VersionDelta)
                            .rw_binary(manager)
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockEpochToVersion::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockEpochToVersion::Epoch)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockEpochToVersion::VersionId)
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
            HummockSstableInfo,
            HummockTimeTravelVersion,
            HummockTimeTravelDelta,
            HummockEpochToVersion
        );
        Ok(())
    }
}

#[derive(DeriveIden)]
pub(crate) enum HummockSstableInfo {
    Table,
    SstId,
    ObjectId,
    SstableInfo,
}

#[derive(DeriveIden)]
enum HummockTimeTravelVersion {
    Table,
    VersionId,
    Version,
}

#[derive(DeriveIden)]
enum HummockTimeTravelDelta {
    Table,
    VersionId,
    VersionDelta,
}

#[derive(DeriveIden)]
enum HummockEpochToVersion {
    Table,
    Epoch,
    VersionId,
}
