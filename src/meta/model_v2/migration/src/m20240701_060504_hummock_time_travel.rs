use sea_orm_migration::prelude::*;

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
                            .blob(BlobSize::Long)
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(HummockTimeTravelArchive::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockTimeTravelArchive::FirstVersionId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelArchive::LastVersionId)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelArchive::FirstVersion)
                            .blob(BlobSize::Long)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelArchive::VersionDeltas)
                            .blob(BlobSize::Long)
                            .not_null(),
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
        manager
            .drop_table(Table::drop().table(HummockEpochToVersion::Table).to_owned())
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(HummockTimeTravelArchive::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(HummockSstableInfo::Table).to_owned())
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum HummockSstableInfo {
    Table,
    SstId,
    ObjectId,
    SstableInfo,
}

#[derive(DeriveIden)]
enum HummockTimeTravelArchive {
    Table,
    FirstVersionId,
    LastVersionId,
    FirstVersion,
    VersionDeltas,
}

#[derive(DeriveIden)]
enum HummockEpochToVersion {
    Table,
    Epoch,
    VersionId,
}
