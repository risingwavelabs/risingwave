use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .add_column(
                        ColumnDef::new(TableEnum::CdcSchemaChangeFailurePolicy)
                            .string()
                            .null(), // Nullable, only CDC tables have this field
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(TableEnum::Table)
                    .drop_column(TableEnum::CdcSchemaChangeFailurePolicy)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum TableEnum {
    #[sea_orm(iden = "table")]
    Table,
    CdcSchemaChangeFailurePolicy,
}
