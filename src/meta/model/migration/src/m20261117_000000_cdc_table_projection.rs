use sea_orm_migration::prelude::*;

use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                sea_query::Table::alter()
                    .table(Table::Table)
                    .add_column(ColumnDef::new(Table::CdcTableProjection).rw_binary(manager))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                sea_query::Table::alter()
                    .table(Table::Table)
                    .drop_column(Table::CdcTableProjection)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum Table {
    Table,
    CdcTableProjection,
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use risingwave_pb::catalog::CdcTableProjection;
    use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};

    use super::*;

    #[tokio::test]
    async fn test_cdc_projection_migration_preserves_unknown_and_empty() -> Result<(), DbErr> {
        let db = Database::connect("sqlite::memory:").await?;
        db.execute_unprepared("CREATE TABLE \"table\" (id INTEGER PRIMARY KEY)")
            .await?;
        db.execute_unprepared("INSERT INTO \"table\" (id) VALUES (1)")
            .await?;
        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await?;
        // Legacy unknown projections must remain distinct from known full-table projections,
        // whose protobuf encoding is an empty blob.
        let full = CdcTableProjection::default();
        let subset = CdcTableProjection {
            excluded_column_names: vec!["email".into()],
        };
        for (id, projection) in [(2, &full), (3, &subset)] {
            db.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO \"table\" (id, cdc_table_projection) VALUES (?, ?)",
                [id.into(), projection.encode_to_vec().into()],
            ))
            .await?;
        }
        let rows = db
            .query_all(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT cdc_table_projection FROM \"table\" ORDER BY id",
            ))
            .await?;
        let projections = rows
            .iter()
            .map(|row| {
                row.try_get::<Option<Vec<u8>>>("", "cdc_table_projection")
                    .map(|bytes| {
                        bytes.map(|bytes| CdcTableProjection::decode(bytes.as_slice()).unwrap())
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(projections, vec![None, Some(full), Some(subset)]);
        Migration.down(&manager).await?;
        Ok(())
    }
}
