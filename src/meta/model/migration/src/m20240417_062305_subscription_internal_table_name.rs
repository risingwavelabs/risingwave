use sea_orm_migration::prelude::{Table as MigrationTable, *};
#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Subscription::Table)
                    .add_column(
                        ColumnDef::new(Subscription::SubscriptionInternalTableName).string(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Subscription::Table)
                    .drop_column(Alias::new(
                        Subscription::SubscriptionInternalTableName.to_string(),
                    ))
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Subscription {
    Table,
    SubscriptionInternalTableName,
}
