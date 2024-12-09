use sea_orm_migration::prelude::*;

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
                        ColumnDef::new(IcebergSinkMetadata::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergSinkMetadata::EndEpoch)
                            .big_integer()
                            .not_null(),
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
    SinkId,
    EndEpoch,
    StartEpoch,
    Metadata,
}
