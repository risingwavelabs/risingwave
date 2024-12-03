use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::*;

#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(IcebergSinkMetadata::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IcebergSinkMetadata::EndEpoch)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(IcebergSinkMetadata::StartEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergSinkMetadata::Metadata)
                            .blob()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(IcebergSinkMetadata::Table)
                    .name("idx_iceberg_sink_metadata_start_epoch")
                    .col(IcebergSinkMetadata::StartEpoch)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, IcebergSinkMetadata);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum IcebergSinkMetadata {
    Table,
    EndEpoch,
    StartEpoch,
    Metadata,
}
