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
                    .table(ExactlyOnceIcebergSinkMetadata::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ExactlyOnceIcebergSinkMetadata::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExactlyOnceIcebergSinkMetadata::EndEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExactlyOnceIcebergSinkMetadata::StartEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(ExactlyOnceIcebergSinkMetadata::Metadata)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, ExactlyOnceIcebergSinkMetadata);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum ExactlyOnceIcebergSinkMetadata {
    Table,
    SinkId,
    EndEpoch,
    StartEpoch,
    Metadata,
}
