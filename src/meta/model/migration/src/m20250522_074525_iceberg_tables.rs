use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(IcebergTables::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IcebergTables::CatalogName)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergTables::TableNamespace)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergTables::TableName)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergTables::MetadataLocation)
                            .string_len(1000)
                            .null(),
                    )
                    .col(
                        ColumnDef::new(IcebergTables::PreviousMetadataLocation)
                            .string_len(1000)
                            .null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(IcebergTables::CatalogName)
                            .col(IcebergTables::TableNamespace)
                            .col(IcebergTables::TableName),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(IcebergTables::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum IcebergTables {
    Table,
    CatalogName,
    TableNamespace,
    TableName,
    MetadataLocation,
    PreviousMetadataLocation,
}
