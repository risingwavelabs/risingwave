use sea_orm_migration::prelude::*;

use crate::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let connection = manager.get_connection();

        // Moving two same-named subscriptions into one schema would make the frontend catalog
        // ambiguous. Stop the upgrade instead of silently renaming or deleting either object.
        let conflicts = connection
            .query_one(Statement::from_string(
                backend,
                "SELECT COUNT(*) FROM (\
                    SELECT s.name, upstream.database_id, upstream.schema_id \
                    FROM subscription AS s \
                    JOIN object AS upstream ON upstream.oid = s.dependent_table_id \
                    GROUP BY s.name, upstream.database_id, upstream.schema_id \
                    HAVING COUNT(*) > 1\
                ) AS conflicts",
            ))
            .await?
            .expect("COUNT always returns one row");
        let conflict_count: i64 = conflicts.try_get_by(0)?;
        if conflict_count != 0 {
            return Err(DbErr::Custom(
                "cannot move subscriptions to their dependent tables' schemas because duplicate subscription names would result; rename or drop the conflicting subscriptions before upgrading"
                    .to_owned(),
            ));
        }

        // Subscriptions also share a namespace with relations at the SQL layer. Although the
        // catalog stores their names in separate tables, do not create an ambiguous namespace by
        // moving a subscription on top of an existing relation.
        let quote = match backend {
            DatabaseBackend::MySql => '`',
            DatabaseBackend::Postgres | DatabaseBackend::Sqlite => '"',
        };
        let relation_names = ["table", "source", "sink", "index", "view"]
            .map(|relation| {
                format!(
                    "SELECT relation.name, relation_object.database_id, \
                     relation_object.schema_id \
                     FROM {quote}{relation}{quote} AS relation \
                     JOIN {quote}object{quote} AS relation_object \
                       ON relation_object.oid = relation.{relation}_id"
                )
            })
            .join(" UNION ALL ");
        let relation_conflicts = connection
            .query_one(Statement::from_string(
                backend,
                format!(
                    "SELECT COUNT(*) \
                     FROM subscription AS s \
                     JOIN object AS upstream ON upstream.oid = s.dependent_table_id \
                     JOIN ({relation_names}) AS relation \
                       ON relation.name = s.name \
                      AND relation.database_id = upstream.database_id \
                      AND relation.schema_id = upstream.schema_id"
                ),
            ))
            .await?
            .expect("COUNT always returns one row");
        let relation_conflict_count: i64 = relation_conflicts.try_get_by(0)?;
        if relation_conflict_count != 0 {
            return Err(DbErr::Custom(
                "cannot move subscriptions to their dependent tables' schemas because relation name conflicts would result; rename or drop the conflicting objects before upgrading"
                    .to_owned(),
            ));
        }

        let sql = match backend {
            DatabaseBackend::MySql => {
                "UPDATE object AS subscription_object \
                 JOIN subscription AS s ON s.subscription_id = subscription_object.oid \
                 JOIN object AS upstream ON upstream.oid = s.dependent_table_id \
                 SET subscription_object.schema_id = upstream.schema_id"
            }
            DatabaseBackend::Postgres => {
                "UPDATE object AS subscription_object \
                 SET schema_id = upstream.schema_id \
                 FROM subscription AS s \
                 JOIN object AS upstream ON upstream.oid = s.dependent_table_id \
                 WHERE s.subscription_id = subscription_object.oid"
            }
            DatabaseBackend::Sqlite => {
                "UPDATE object \
                 SET schema_id = (\
                     SELECT upstream.schema_id \
                     FROM subscription AS s \
                     JOIN object AS upstream ON upstream.oid = s.dependent_table_id \
                     WHERE s.subscription_id = object.oid\
                 ) \
                 WHERE oid IN (SELECT subscription_id FROM subscription)"
            }
        };
        connection
            .execute(Statement::from_string(backend, sql))
            .await?;

        // Object privileges and ownership reference the object ID, so changing the containing
        // schema neither grants privileges to new users nor drops existing object-level grants.
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // The previous schema was not stored, and restoring the invalid layout would violate the
        // catalog invariant enforced by the corresponding release.
        Ok(())
    }
}
