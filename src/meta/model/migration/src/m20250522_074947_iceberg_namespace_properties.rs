use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(IcebergNamespaceProperties::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IcebergNamespaceProperties::CatalogName)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergNamespaceProperties::Namespace)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergNamespaceProperties::PropertyKey)
                            .string_len(255)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(IcebergNamespaceProperties::PropertyValue)
                            .string_len(1000)
                            .null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(IcebergNamespaceProperties::CatalogName)
                            .col(IcebergNamespaceProperties::Namespace)
                            .col(IcebergNamespaceProperties::PropertyKey),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(IcebergNamespaceProperties::Table)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum IcebergNamespaceProperties {
    Table,
    CatalogName,
    Namespace,
    PropertyKey,
    PropertyValue,
}
