use sea_orm_migration::prelude::*;

use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(manager, SessionParameter);
        manager
            .create_table(
                Table::create()
                    .table(SessionParameter::Table)
                    .col(
                        ColumnDef::new(SessionParameter::Name)
                            .string()
                            .primary_key()
                            .not_null(),
                    )
                    .col(ColumnDef::new(SessionParameter::Value).string().not_null())
                    .col(ColumnDef::new(SessionParameter::Description).text())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.
        drop_tables!(manager, SessionParameter);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum SessionParameter {
    Table,
    Name,
    Value,
    Description,
}
