use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::drop_tables;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.
        drop_tables!(manager, Subscription);
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
                        ColumnDef::new(Subscription::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::RetentionSeconds)
                            .string()
                            .big_integer(),
                    )
                    .col(
                        ColumnDef::new(Subscription::SubscriptionState)
                            .string()
                            .integer(),
                    )
                    .col(
                        ColumnDef::new(Subscription::DependentTableId)
                            .integer()
                            .not_null(),
                    )
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_subscription_object_id")
                            .from(Subscription::Table, Subscription::SubscriptionId)
                            .to(
                                crate::m20230908_072257_init::Object::Table,
                                crate::m20230908_072257_init::Object::Oid,
                            )
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // drop tables cascade.
        drop_tables!(manager, Subscription);
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
                        ColumnDef::new(Subscription::Definition)
                            .rw_long_text(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::Columns)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Subscription::PlanPk)
                            .rw_binary(manager)
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
                    .col(
                        ColumnDef::new(Subscription::SubscriptionFromName)
                            .string()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Subscription::SubscriptionInternalTableName).string())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_subscription_object_id")
                            .from(Subscription::Table, Subscription::SubscriptionId)
                            .to(
                                crate::m20230908_072257_init::Object::Table,
                                crate::m20230908_072257_init::Object::Oid,
                            )
                            .on_delete(ForeignKeyAction::Cascade)
                            .to_owned(),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Subscription {
    Table,
    // common
    SubscriptionId,
    Name,
    Definition,
    // before
    Columns,
    PlanPk,
    DistributionKey,
    Properties,
    SubscriptionFromName,
    SubscriptionInternalTableName,
    // after
    RetentionSeconds,
    SubscriptionState,
    DependentTableId,
}
