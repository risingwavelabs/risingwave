use sea_orm_migration::prelude::*;

use crate::m20240701_060504_hummock_time_travel::HummockSstableInfo;

#[derive(DeriveMigrationName)]
pub struct Migration;

const IDX_HUMMOCK_SSTABLE_INFO_OBJECT_ID: &str = "idx_hummock_sstable_info_object_id";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_index(
                Index::create()
                    .table(HummockSstableInfo::Table)
                    .name(IDX_HUMMOCK_SSTABLE_INFO_OBJECT_ID)
                    .col(HummockSstableInfo::ObjectId)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .table(HummockSstableInfo::Table)
                    .name(IDX_HUMMOCK_SSTABLE_INFO_OBJECT_ID)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
