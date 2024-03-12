use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(manager, Subscription);
        manager
            .create_table(
                MigrationTable::create()
                    .table(Subscription::Table)
                    .col(
                        ColumnDef::new(Subscription::SubscriptionId)
                            .integer()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Subscription::Name).string().not_null())
                    .col(
                        ColumnDef::new(Subscription::Columns)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::PlanPk)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::DistributionKey)
                            .json_binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::Properties)
                            .json_binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Subscription::Definition).string().not_null())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.
        drop_tables!(manager, Subscription);
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Subscription {
    Table,
    SubscriptionId,
    Name,
    Columns,
    PlanPk,
    DistributionKey,
    Properties,
    Definition,
}
