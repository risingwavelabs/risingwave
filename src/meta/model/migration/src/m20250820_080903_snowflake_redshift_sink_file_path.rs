use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(SnowflakeRedshiftSinkFilePath::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(SnowflakeRedshiftSinkFilePath::SinkId)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(SnowflakeRedshiftSinkFilePath::EndEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(SnowflakeRedshiftSinkFilePath::FilePaths)
                            .json_binary()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(SnowflakeRedshiftSinkFilePath::SinkId)
                            .col(SnowflakeRedshiftSinkFilePath::EndEpoch),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, SnowflakeRedshiftSinkFilePath);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum SnowflakeRedshiftSinkFilePath {
    Table,
    SinkId,
    EndEpoch,
    FilePaths,
}
