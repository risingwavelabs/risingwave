use sea_orm::Statement;
use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Sink::Table)
                    .add_column(
                        ColumnDef::new(Sink::IgnoreDelete)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        // Normalize deprecated FORCE_APPEND_ONLY rows to APPEND_ONLY + ignore_delete = true.
        manager
            .get_connection()
            .execute(Statement::from_string(
                manager.get_database_backend(),
                "UPDATE sink SET ignore_delete = TRUE, sink_type = 'APPEND_ONLY' WHERE sink_type = 'FORCE_APPEND_ONLY'",
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}

#[derive(DeriveIden)]
#[allow(clippy::enum_variant_names)]
enum Sink {
    Table,
    IgnoreDelete,
}
