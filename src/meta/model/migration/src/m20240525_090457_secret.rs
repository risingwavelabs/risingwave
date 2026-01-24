use sea_orm_migration::prelude::{Table as MigrationTable, *};

use crate::utils::ColumnDefExt;
use crate::{assert_not_has_tables, drop_tables};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_not_has_tables!(manager, Secret);
        manager
            .create_table(
                MigrationTable::create()
                    .table(Secret::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Secret::SecretId)
                            .integer()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Secret::Name).string().not_null())
                    .col(ColumnDef::new(Secret::Value).rw_binary(manager).not_null())
                    .foreign_key(
                        &mut ForeignKey::create()
                            .name("FK_secret_object_id")
                            .from(Secret::Table, Secret::SecretId)
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

        // Add a new column to the `sink` table
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .add_column(ColumnDef::new(Sink::SecretRef).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        // Add a new column to the `source` table
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .add_column(ColumnDef::new(Source::SecretRef).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        drop_tables!(manager, Secret);
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .drop_column(Sink::SecretRef)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Source::Table)
                    .drop_column(Source::SecretRef)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum Secret {
    Table,
    SecretId,
    Name,
    Value,
}

#[derive(DeriveIden)]
enum Sink {
    Table,
    SecretRef,
}

#[derive(DeriveIden)]
enum Source {
    Table,
    SecretRef,
}
