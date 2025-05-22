use sea_orm::EnumIter;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(AllowAlterConnectorProps::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(AllowAlterConnectorProps::ConnectorName)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(AllowAlterConnectorProps::ObjectType)
                            .string()
                            .not_null()
                            .primary_key()
                            .check(
                                Expr::col(AllowAlterConnectorProps::ObjectType)
                                    .is_in(vec!["SINK", "SOURCE"]),
                            ),
                    )
                    .col(
                        ColumnDef::new(AllowAlterConnectorProps::AllowAlterProps)
                            .array(ColumnType::String(StringLen::N(255)))
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        crate::drop_tables!(manager, AllowAlterConnectorProps);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum AllowAlterConnectorProps {
    Table,
    ConnectorName,
    ObjectType,
    AllowAlterProps,
}

#[derive(DeriveIden, EnumIter)]
enum AlterPropsObjectType {
    Sink,
    Source,
}
