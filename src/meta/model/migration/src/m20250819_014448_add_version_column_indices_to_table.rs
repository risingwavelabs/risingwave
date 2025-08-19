use sea_orm::{FromJsonQueryResult, FromQueryResult, Statement};
use sea_orm_migration::prelude::{Table as MigrationTable, *};
use serde::{Deserialize, Serialize};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add the new version_column_indices column
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column(
                        ColumnDef::new(Table::VersionColumnIndices)
                            .json_binary()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        // Migrate data from version_column_index to version_column_indices using I32Array serialization
        let connection = manager.get_connection();
        let database_backend = connection.get_database_backend();

        // First, get all tables that have a version_column_index
        let (sql, values) = Query::select()
            .columns([Table::TableId, Table::VersionColumnIndex])
            .from(Table::Table)
            .and_where(Expr::col(Table::VersionColumnIndex).is_not_null())
            .build_any(&*database_backend.get_query_builder());

        let query_result = connection
            .query_all(Statement::from_sql_and_values(
                database_backend,
                sql,
                values,
            ))
            .await?;

        // For each table with a version_column_index, create the proper I32Array JSON
        for row in query_result {
            let table_entity = TableEntity::from_query_result(&row, "")?;

            // Create I32Array with single element
            let i32_array = I32Array(vec![table_entity.version_column_index.unwrap()]);

            // Update with the I32Array
            manager
                .exec_stmt(
                    Query::update()
                        .table(Table::Table)
                        .value(Table::VersionColumnIndices, i32_array)
                        .and_where(Expr::col(Table::TableId).eq(table_entity.table_id))
                        .to_owned(),
                )
                .await?;
        }

        // Handle tables with NULL version_column_index (set to empty array)
        let empty_array = I32Array(vec![]);
        manager
            .exec_stmt(
                Query::update()
                    .table(Table::Table)
                    .value(Table::VersionColumnIndices, empty_array)
                    .and_where(Expr::col(Table::VersionColumnIndex).is_null())
                    .to_owned(),
            )
            .await?;

        // Keep the old version_column_index column for safety
        // It can be manually dropped later if needed
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Remove the new version_column_indices column
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .drop_column(Table::VersionColumnIndices)
                    .to_owned(),
            )
            .await?;

        // Restore the old version_column_index column if not exist
        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Table::Table)
                    .add_column_if_not_exists(ColumnDef::new(Table::VersionColumnIndex).integer())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct I32Array(pub Vec<i32>);

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Table")]
pub struct TableEntity {
    table_id: i32,
    version_column_index: Option<i32>,
}

#[derive(DeriveIden)]
enum Table {
    Table,
    TableId,
    VersionColumnIndex,
    VersionColumnIndices,
}
