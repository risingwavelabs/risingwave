use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const TABLE_NAME: &str = "hummock_time_travel_version_epoch_summary";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(HummockTimeTravelVersionEpochSummary::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(HummockTimeTravelVersionEpochSummary::VersionId)
                            .big_integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(HummockTimeTravelVersionEpochSummary::MaxEpoch)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .get_connection()
            .execute(sea_orm::Statement::from_string(
                manager.get_database_backend(),
                format!(
                    "INSERT INTO {TABLE_NAME} (version_id, max_epoch) \
                     SELECT version_id, MAX(epoch) FROM hummock_epoch_to_version GROUP BY version_id"
                ),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(HummockTimeTravelVersionEpochSummary::Table)
                    .if_exists()
                    .cascade()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum HummockTimeTravelVersionEpochSummary {
    Table,
    VersionId,
    MaxEpoch,
}
