use std::collections::BTreeSet;

use prost::Message;
use risingwave_pb::catalog::subscription::SubscriptionState;
use risingwave_pb::id::{JobId, TableId};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbStreamNode, PbStreamScanType};
use sea_orm::ActiveValue::{NotSet, Set};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter,
    Statement, TransactionTrait,
};
use sea_orm_migration::prelude::*;

// Keep in sync with `FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan`.
const CROSS_DB_SNAPSHOT_BACKFILL_STREAM_SCAN_FLAG: i32 = 1 << 13;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if !manager
            .has_column("subscription", "cross_db_downstream_job_id")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Subscription::Table)
                        .add_column(
                            ColumnDef::new(Subscription::CrossDbDownstreamJobId)
                                .integer()
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;
        }

        backfill_cross_db_subscriptions(manager).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let subscription_ids = Query::select()
            .column(Subscription::SubscriptionId)
            .from(Subscription::Table)
            .and_where(Expr::col(Subscription::CrossDbDownstreamJobId).is_not_null())
            .to_owned();
        manager
            .exec_stmt(
                Query::delete()
                    .from_table(Object::Table)
                    .and_where(Expr::col(Object::Oid).in_subquery(subscription_ids))
                    .to_owned(),
            )
            .await?;
        // The object FK normally cascades this deletion. Keep the explicit cleanup so the
        // migration also behaves correctly on catalogs created before that FK existed.
        manager
            .exec_stmt(
                Query::delete()
                    .from_table(Subscription::Table)
                    .and_where(Expr::col(Subscription::CrossDbDownstreamJobId).is_not_null())
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(Subscription::Table)
                    .drop_column(Subscription::CrossDbDownstreamJobId)
                    .to_owned(),
            )
            .await
    }
}

async fn backfill_cross_db_subscriptions(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let txn = manager.get_connection().begin().await?;
    let backend = txn.get_database_backend();
    let (sql, values) = Query::select()
        .columns([Fragment::JobId, Fragment::StreamNode])
        .from(Fragment::Table)
        .and_where(
            Expr::col(Fragment::FragmentTypeMask)
                .bit_and(Expr::value(CROSS_DB_SNAPSHOT_BACKFILL_STREAM_SCAN_FLAG))
                .ne(0),
        )
        .build_any(&*backend.get_query_builder());
    let fragments = txn
        .query_all(Statement::from_sql_and_values(backend, sql, values))
        .await?;

    let mut cross_db_dependencies = BTreeSet::new();
    for row in fragments {
        let fragment = FragmentRow::from_query_result(&row, "")?;
        let stream_node = PbStreamNode::decode(fragment.stream_node.as_slice()).map_err(|err| {
            DbErr::Custom(format!(
                "failed to decode stream node for cross-database subscription migration: {err}"
            ))
        })?;
        collect_cross_db_dependencies(fragment.job_id, &stream_node, &mut cross_db_dependencies)?;
    }

    for (downstream_job_id, upstream_table_id) in cross_db_dependencies {
        let already_exists = subscription_entity::Entity::find()
            .filter(subscription_entity::Column::CrossDbDownstreamJobId.eq(downstream_job_id))
            .filter(subscription_entity::Column::DependentTableId.eq(upstream_table_id))
            .one(&txn)
            .await?
            .is_some();
        if already_exists {
            continue;
        }

        let downstream_object = object_entity::Entity::find_by_id(downstream_job_id.as_i32_id())
            .one(&txn)
            .await?
            .ok_or_else(|| {
                DbErr::Custom(format!(
                    "cross-database downstream job object {downstream_job_id} does not exist"
                ))
            })?;
        let upstream_object = object_entity::Entity::find_by_id(upstream_table_id.as_i32_id())
            .one(&txn)
            .await?
            .ok_or_else(|| {
                DbErr::Custom(format!(
                    "cross-database upstream table object {upstream_table_id} does not exist"
                ))
            })?;
        if upstream_object.obj_type != "TABLE" {
            return Err(DbErr::Custom(format!(
                "cross-database upstream object {upstream_table_id} is not a table"
            )));
        }
        let upstream_database_id = upstream_object.database_id.ok_or_else(|| {
            DbErr::Custom(format!(
                "cross-database upstream table {upstream_table_id} has no database"
            ))
        })?;
        let downstream_database_id = downstream_object.database_id.ok_or_else(|| {
            DbErr::Custom(format!(
                "cross-database downstream job {downstream_job_id} has no database"
            ))
        })?;
        let downstream_schema_id = downstream_object.schema_id.ok_or_else(|| {
            DbErr::Custom(format!(
                "cross-database downstream job {downstream_job_id} has no schema"
            ))
        })?;
        if upstream_database_id == downstream_database_id {
            return Err(DbErr::Custom(format!(
                "cross-database scan from job {downstream_job_id} to table {upstream_table_id} is in one database"
            )));
        }

        let subscription_object = object_entity::ActiveModel {
            oid: NotSet,
            obj_type: Set("SUBSCRIPTION".to_owned()),
            owner_id: Set(downstream_object.owner_id),
            schema_id: Set(Some(downstream_schema_id)),
            database_id: Set(Some(downstream_database_id)),
            belong_to_oid: Set(Some(downstream_job_id)),
        }
        .insert(&txn)
        .await?;

        subscription_entity::ActiveModel {
            subscription_id: Set(subscription_object.oid),
            name: Set(format!(
                "__cross_db_subscription_{downstream_job_id}_{upstream_table_id}"
            )),
            retention_seconds: Set(None),
            definition: Set(String::new()),
            subscription_state: Set(SubscriptionState::Created as i32),
            dependent_table_id: Set(upstream_table_id),
            cross_db_downstream_job_id: Set(Some(downstream_job_id)),
        }
        .insert(&txn)
        .await?;
    }

    txn.commit().await
}

fn collect_cross_db_dependencies(
    downstream_job_id: JobId,
    node: &PbStreamNode,
    dependencies: &mut BTreeSet<(JobId, TableId)>,
) -> Result<(), DbErr> {
    if let Some(NodeBody::StreamScan(stream_scan)) = &node.node_body
        && stream_scan.stream_scan_type == PbStreamScanType::CrossDbSnapshotBackfill as i32
    {
        dependencies.insert((downstream_job_id, stream_scan.table_id));
    }
    for input in &node.input {
        collect_cross_db_dependencies(downstream_job_id, input, dependencies)?;
    }
    Ok(())
}

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
struct FragmentRow {
    job_id: JobId,
    stream_node: Vec<u8>,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    JobId,
    FragmentTypeMask,
    StreamNode,
}

#[derive(DeriveIden)]
enum Object {
    Table,
    Oid,
}

#[derive(DeriveIden)]
enum Subscription {
    Table,
    SubscriptionId,
    CrossDbDownstreamJobId,
}

mod object_entity {
    use risingwave_pb::id::JobId;
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "object")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub oid: i32,
        pub obj_type: String,
        pub owner_id: i32,
        pub schema_id: Option<i32>,
        pub database_id: Option<i32>,
        pub belong_to_oid: Option<JobId>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

mod subscription_entity {
    use risingwave_pb::id::{JobId, TableId};
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "subscription")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub subscription_id: i32,
        pub name: String,
        pub retention_seconds: Option<i64>,
        pub definition: String,
        pub subscription_state: i32,
        pub dependent_table_id: TableId,
        pub cross_db_downstream_job_id: Option<JobId>,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::{PbStreamScanNode, PbStreamScanType};
    use sea_orm::{Database, DatabaseBackend, TryGetable};

    use super::*;

    #[tokio::test]
    async fn test_backfill_existing_cross_db_job() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        for sql in [
            r#"CREATE TABLE "object" ("oid" INTEGER PRIMARY KEY AUTOINCREMENT, "obj_type" TEXT NOT NULL, "owner_id" INTEGER NOT NULL, "schema_id" INTEGER, "database_id" INTEGER, "belong_to_oid" INTEGER)"#,
            r#"CREATE TABLE "subscription" ("subscription_id" INTEGER PRIMARY KEY, "name" TEXT NOT NULL, "retention_seconds" BIGINT, "definition" TEXT NOT NULL, "subscription_state" INTEGER, "dependent_table_id" INTEGER NOT NULL)"#,
            r#"CREATE TABLE "fragment" ("job_id" INTEGER NOT NULL, "fragment_type_mask" INTEGER NOT NULL, "stream_node" BLOB NOT NULL)"#,
            r#"INSERT INTO "object" ("oid", "obj_type", "owner_id", "schema_id", "database_id", "belong_to_oid") VALUES (10, 'TABLE', 1, 2, 1, 2), (20, 'TABLE', 3, 4, 5, 4)"#,
        ] {
            db.execute(Statement::from_string(DatabaseBackend::Sqlite, sql))
                .await
                .unwrap();
        }

        let stream_node = PbStreamNode {
            node_body: Some(NodeBody::StreamScan(Box::new(PbStreamScanNode {
                table_id: 10.into(),
                stream_scan_type: PbStreamScanType::CrossDbSnapshotBackfill as i32,
                ..Default::default()
            }))),
            ..Default::default()
        };
        for _ in 0..2 {
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                r#"INSERT INTO "fragment" ("job_id", "fragment_type_mask", "stream_node") VALUES (?, ?, ?)"#,
                [
                    20.into(),
                    CROSS_DB_SNAPSHOT_BACKFILL_STREAM_SCAN_FLAG.into(),
                    stream_node.encode_to_vec().into(),
                ],
            ))
            .await
            .unwrap();
        }
        // An unrelated fragment with invalid protobuf must be excluded by the type-mask filter.
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "fragment" ("job_id", "fragment_type_mask", "stream_node") VALUES (?, ?, ?)"#,
            [20.into(), 0.into(), vec![0xff_u8].into()],
        ))
        .await
        .unwrap();

        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap();
        // A retry must not create a duplicate subscription.
        Migration.up(&manager).await.unwrap();

        let row = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT s."name", s."retention_seconds", s."subscription_state", s."dependent_table_id", s."cross_db_downstream_job_id", o."owner_id", o."schema_id", o."database_id", o."belong_to_oid" FROM "subscription" AS s JOIN "object" AS o ON o."oid" = s."subscription_id""#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            String::try_get(&row, "", "name").unwrap(),
            "__cross_db_subscription_20_10"
        );
        assert_eq!(
            Option::<i64>::try_get(&row, "", "retention_seconds").unwrap(),
            None
        );
        assert_eq!(i32::try_get(&row, "", "subscription_state").unwrap(), 2);
        assert_eq!(i32::try_get(&row, "", "dependent_table_id").unwrap(), 10);
        assert_eq!(
            i32::try_get(&row, "", "cross_db_downstream_job_id").unwrap(),
            20
        );
        assert_eq!(i32::try_get(&row, "", "owner_id").unwrap(), 3);
        assert_eq!(i32::try_get(&row, "", "schema_id").unwrap(), 4);
        assert_eq!(i32::try_get(&row, "", "database_id").unwrap(), 5);
        assert_eq!(i32::try_get(&row, "", "belong_to_oid").unwrap(), 20);
    }
}
