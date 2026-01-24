use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const TABLE_NAME: &str = "hummock_epoch_to_version";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // modify PK
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::MySql => {
                manager
                    .alter_table(
                        Table::alter()
                            .table(HummockEpochToVersion::Table)
                            .add_column(
                                ColumnDef::new(HummockEpochToVersion::TableId).big_integer(),
                            )
                            .to_owned(),
                    )
                    .await?;
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::MySql,
                        format!("ALTER TABLE {TABLE_NAME} DROP PRIMARY KEY, ADD PRIMARY KEY (epoch, table_id)"),
                    ))
                    .await?;
            }
            sea_orm::DatabaseBackend::Postgres => {
                manager
                    .alter_table(
                        Table::alter()
                            .table(HummockEpochToVersion::Table)
                            .add_column(
                                ColumnDef::new(HummockEpochToVersion::TableId).big_integer(),
                            )
                            .to_owned(),
                    )
                    .await?;
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::Postgres,
                        format!("ALTER TABLE {TABLE_NAME} DROP CONSTRAINT {TABLE_NAME}_pkey"),
                    ))
                    .await?;
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::Postgres,
                        format!("ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (epoch, table_id)"),
                    ))
                    .await?;
            }
            sea_orm::DatabaseBackend::Sqlite => {
                // sqlite is not for prod usage, so recreating the table is fine.
                manager
                    .drop_table(
                        sea_orm_migration::prelude::Table::drop()
                            .table(HummockEpochToVersion::Table)
                            .if_exists()
                            .cascade()
                            .to_owned(),
                    )
                    .await?;

                manager
                    .create_table(
                        Table::create()
                            .table(HummockEpochToVersion::Table)
                            .if_not_exists()
                            .col(
                                ColumnDef::new(HummockEpochToVersion::Epoch)
                                    .big_integer()
                                    .not_null(),
                            )
                            .col(
                                ColumnDef::new(HummockEpochToVersion::TableId)
                                    .big_integer()
                                    .not_null(),
                            )
                            .col(
                                ColumnDef::new(HummockEpochToVersion::VersionId)
                                    .big_integer()
                                    .not_null(),
                            )
                            .primary_key(
                                Index::create()
                                    .col(HummockEpochToVersion::Epoch)
                                    .col(HummockEpochToVersion::TableId),
                            )
                            .to_owned(),
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // The downgrade for MySql and Postgres may not work due to PK confliction.
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::MySql => {
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::MySql,
                        format!("ALTER TABLE {TABLE_NAME} DROP PRIMARY KEY"),
                    ))
                    .await?;
                manager
                    .alter_table(
                        Table::alter()
                            .table(HummockEpochToVersion::Table)
                            .drop_column(HummockEpochToVersion::TableId)
                            .to_owned(),
                    )
                    .await?;
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::MySql,
                        format!("ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (epoch)"),
                    ))
                    .await?;
            }
            sea_orm::DatabaseBackend::Postgres => {
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::Postgres,
                        format!("ALTER TABLE {TABLE_NAME} DROP CONSTRAINT {TABLE_NAME}_pkey"),
                    ))
                    .await?;
                manager
                    .alter_table(
                        Table::alter()
                            .table(HummockEpochToVersion::Table)
                            .drop_column(HummockEpochToVersion::TableId)
                            .to_owned(),
                    )
                    .await?;
                manager
                    .get_connection()
                    .execute(sea_orm::Statement::from_string(
                        sea_orm::DatabaseBackend::Postgres,
                        format!("ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (epoch)"),
                    ))
                    .await?;
            }
            sea_orm::DatabaseBackend::Sqlite => {
                manager
                    .drop_table(
                        sea_orm_migration::prelude::Table::drop()
                            .table(HummockEpochToVersion::Table)
                            .if_exists()
                            .cascade()
                            .to_owned(),
                    )
                    .await?;

                manager
                    .create_table(
                        Table::create()
                            .table(HummockEpochToVersion::Table)
                            .if_not_exists()
                            .col(
                                ColumnDef::new(HummockEpochToVersion::Epoch)
                                    .big_integer()
                                    .not_null()
                                    .primary_key(),
                            )
                            .col(
                                ColumnDef::new(HummockEpochToVersion::VersionId)
                                    .big_integer()
                                    .not_null(),
                            )
                            .to_owned(),
                    )
                    .await?;
            }
        }

        Ok(())
    }
}

#[derive(DeriveIden)]
enum HummockEpochToVersion {
    Table,
    Epoch,
    TableId,
    VersionId,
}
