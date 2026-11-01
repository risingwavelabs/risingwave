// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::ICEBERG_SINK_PREFIX;
use risingwave_pb::catalog::subscription::PbSubscriptionState;
use risingwave_pb::common::PbObjectType;
use risingwave_pb::ddl_service::{CascadeObject as PbCascadeObject, DropResult, TypeCount};
use risingwave_pb::telemetry::PbTelemetryDatabaseObject;
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, JoinType, ModelTrait, QueryFilter, QuerySelect,
};

#[derive(Debug)]
pub struct DropObjectResult {
    pub release_context: ReleaseContext,
    pub version: NotificationVersion,
    pub drop_result: Option<DropResult>,
}

use super::*;
impl CatalogController {
    // Drop all kinds of objects including databases,
    // schemas, relations, connections, functions, etc.
    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: impl Into<ObjectId>,
        drop_mode: DropMode,
    ) -> MetaResult<DropObjectResult> {
        let object_id = object_id.into();
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        let database_id = if object_type == ObjectType::Database {
            object_id.as_database_id()
        } else {
            obj.database_id
                .ok_or_else(|| anyhow!("dropped object should have database_id"))?
        };

        // Check the cross-db dependency info to see if the subscription can be dropped.
        if obj.obj_type == ObjectType::Subscription {
            validate_subscription_deletion(&txn, object_id.as_subscription_id()).await?;
        }

        let mut removed_objects = match drop_mode {
            DropMode::Cascade => {
                get_referring_objects_cascade(object_id, object_type, &txn).await?
            }
            DropMode::Restrict => match object_type {
                ObjectType::Database => unreachable!("database always be dropped in cascade mode"),
                ObjectType::Schema => {
                    ensure_schema_empty(object_id.as_schema_id(), &txn).await?;
                    Default::default()
                }
                ObjectType::Table => {
                    let objects = validate_restrict_drop_and_collect_owned_objects(
                        object_type,
                        object_id,
                        &txn,
                    )
                    .await?;
                    for obj in objects.iter().filter(|object| {
                        object.obj_type == ObjectType::Source || object.obj_type == ObjectType::Sink
                    }) {
                        report_drop_object(obj.obj_type, obj.oid, &txn).await;
                    }
                    assert!(
                        objects.iter().all(|obj| obj.obj_type == ObjectType::Index
                            || obj.obj_type == ObjectType::Sink),
                        "only index and iceberg sink could be dropped in restrict mode"
                    );
                    for obj in &objects {
                        validate_restrict_drop_and_collect_owned_objects(
                            obj.obj_type,
                            obj.oid,
                            &txn,
                        )
                        .await?;
                    }
                    objects
                }
                object_type @ (ObjectType::Source | ObjectType::Sink) => {
                    validate_restrict_drop_and_collect_owned_objects(object_type, object_id, &txn)
                        .await?;
                    report_drop_object(object_type, object_id, &txn).await;
                    vec![]
                }

                ObjectType::View
                | ObjectType::Index
                | ObjectType::Function
                | ObjectType::Connection
                | ObjectType::Subscription
                | ObjectType::Secret => {
                    validate_restrict_drop_and_collect_owned_objects(object_type, object_id, &txn)
                        .await?;
                    vec![]
                }
            },
        };

        // The dependency query includes incoming sinks. Build notices before adding owned
        // objects, which are implicitly removed with their parent.
        let drop_result = if drop_mode == DropMode::Cascade && object_type != ObjectType::Database {
            Some(build_drop_result_for_notice(object_id, &removed_objects, &txn).await?)
        } else {
            None
        };

        removed_objects.push(obj);
        let mut removed_object_ids: HashSet<_> =
            removed_objects.iter().map(|obj| obj.oid).collect();

        for obj in &removed_objects {
            if obj.obj_type == ObjectType::Sink {
                let sink = Sink::find_by_id(obj.oid.as_sink_id())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", obj.oid))?;

                if let Some(target_table) = sink.target_table
                    && !removed_object_ids.contains(&target_table.as_object_id())
                    && !has_table_been_migrated(&txn, target_table).await?
                {
                    return Err(anyhow::anyhow!(
                        "Dropping sink into table is not allowed for unmigrated table {}. Please migrate it first.",
                        target_table
                    ).into());
                }
            }
        }

        // Load all objects that belong to the dropped objects before deletion. Cascaded rows are
        // still needed for notifications and resource cleanup.
        let root_objects = Object::find()
            .filter(object::Column::Oid.is_in(removed_object_ids.iter().copied()))
            .all(&txn)
            .await?;
        let belonging_objects =
            get_belong_objects_by_ids(&txn, removed_objects.iter().map(|obj| obj.oid)).await?;
        removed_object_ids.extend(belonging_objects.iter().map(|obj| obj.oid));
        let mut objects_to_remove = root_objects.clone();
        objects_to_remove.extend(belonging_objects.iter().cloned());
        let removed_catalog_models = load_object_models(&txn, &objects_to_remove).await?;
        removed_objects.extend(belonging_objects.into_iter().map(|obj| PartialObject {
            oid: obj.oid,
            obj_type: obj.obj_type,
            schema_id: obj.schema_id,
            database_id: obj.database_id,
        }));

        let removed_table_ids = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid.as_table_id());

        let removed_iceberg_table_sinks: Vec<PbSink> = removed_catalog_models
            .iter()
            .filter_map(|object_info| match object_info {
                PbObjectInfo::Sink(sink) if sink.name.starts_with(ICEBERG_SINK_PREFIX) => {
                    Some(sink.clone())
                }
                _ => None,
            })
            .collect();

        // Iceberg sinks can be user-created with arbitrary
        // names, so unlike the iceberg-table cleanup above, identify them by
        // inspecting properties rather than by name prefix.
        let mut removed_iceberg_sink_ids: Vec<SinkId> = Vec::new();
        for object_info in &removed_catalog_models {
            if let PbObjectInfo::Sink(sink) = object_info
                && crate::manager::iceberg_compaction::is_iceberg_sink(&sink.properties)
            {
                removed_iceberg_sink_ids.push(sink.id);
            }
        }

        let removed_streaming_job_ids: Vec<JobId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobId.is_in(removed_object_ids))
            .into_tuple()
            .all(&txn)
            .await?;

        // Check if there are any streaming jobs that are creating.
        if !removed_streaming_job_ids.is_empty() {
            let creating = StreamingJob::find()
                .filter(
                    streaming_job::Column::JobStatus
                        .ne(JobStatus::Created)
                        .and(streaming_job::Column::JobId.is_in(removed_streaming_job_ids.clone())),
                )
                .count(&txn)
                .await?;
            if creating != 0 {
                if creating == 1 && object_type == ObjectType::Sink {
                    info!("dropping creating sink job, it will be cancelled");
                } else {
                    return Err(MetaError::permission_denied(format!(
                        "cannot drop {creating} streaming job(s) that are still being created; please cancel them first"
                    )));
                }
            }
        }

        let removed_state_table_ids: HashSet<_> = removed_table_ids.clone().collect();

        let removed_source_ids: HashSet<_> = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Source)
            .map(|obj| obj.oid.as_source_id())
            .collect();

        let removed_secret_ids = removed_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Secret)
            .map(|obj| obj.oid.as_secret_id())
            .collect();

        let removed_objects: HashMap<_, _> = removed_objects
            .into_iter()
            .map(|obj| (obj.oid, obj))
            .collect();

        // TODO: Support drop cascade for cross-database query.
        for obj in removed_objects.values() {
            if let Some(obj_database_id) = obj.database_id
                && obj_database_id != database_id
            {
                return Err(MetaError::permission_denied(format!(
                    "Referenced by other objects in database {obj_database_id}, please drop them manually"
                )));
            }
        }

        let (removed_source_fragments, removed_sink_fragments, removed_fragments) =
            get_fragments_for_jobs(&txn, removed_streaming_job_ids.clone()).await?;

        let sink_target_fragments = fetch_target_fragments(&txn, removed_sink_fragments).await?;
        let mut removed_sink_fragment_by_targets = HashMap::new();
        for (sink_fragment, target_fragments) in sink_target_fragments {
            assert!(
                target_fragments.len() <= 1,
                "sink should have at most one downstream fragment"
            );
            if let Some(target_fragment) = target_fragments.first()
                && !removed_fragments.contains(target_fragment)
            {
                removed_sink_fragment_by_targets
                    .entry(*target_fragment)
                    .or_insert_with(Vec::new)
                    .push(sink_fragment);
            }
        }

        // Find affect users with privileges on all this objects.
        let updated_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(removed_objects.keys().cloned()))
            .into_tuple()
            .all(&txn)
            .await?;
        let dropped_tables = removed_catalog_models
            .into_iter()
            .filter_map(|object_info| match object_info {
                PbObjectInfo::Table(table) if removed_state_table_ids.contains(&table.id) => {
                    Some(table)
                }
                _ => None,
            });
        // Delete the explicitly selected objects. The self foreign key cascades to every object
        // that belongs to them.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(root_objects.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        let user_infos = list_user_info_by_ids(updated_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        let mut notifications = self.frontend_notification_batch();
        Self::add_users_update_notifications(&mut notifications, user_infos);
        inner
            .dropped_tables
            .extend(dropped_tables.map(|t| (t.id, t)));

        match object_type {
            ObjectType::Database => {
                // TODO: Notify objects in other databases when the cross-database query is supported.
                notifications.add(
                    NotificationOperation::Delete,
                    NotificationInfo::Database(PbDatabase {
                        id: database_id,
                        ..Default::default()
                    }),
                );
            }
            ObjectType::Schema => {
                let (schema_obj, mut to_notify_objs): (Vec<_>, Vec<_>) = removed_objects
                    .into_values()
                    .partition(|obj| obj.obj_type == ObjectType::Schema && obj.oid == object_id);
                let schema_obj = Itertools::exactly_one(schema_obj.into_iter())
                    .expect("schema object not found");
                to_notify_objs.push(schema_obj);

                let relation_group = build_object_group_for_delete(to_notify_objs);
                notifications.add(NotificationOperation::Delete, relation_group);
            }
            _ => {
                // Hummock observers and compactor observers are notified once the corresponding barrier is completed.
                // They only need RelationInfo::Table.
                let relation_group =
                    build_object_group_for_delete(removed_objects.into_values().collect());
                notifications.add(NotificationOperation::Delete, relation_group);
            }
        }
        let version = notifications
            .publish()
            .await
            .expect("the drop notification batch contains the dropped object");

        Ok(DropObjectResult {
            release_context: ReleaseContext {
                database_id,
                removed_streaming_job_ids,
                removed_state_table_ids: removed_state_table_ids.into_iter().collect(),
                removed_source_ids: removed_source_ids.into_iter().collect(),
                removed_secret_ids,
                removed_source_fragments,
                removed_fragments,
                removed_sink_fragment_by_targets,
                removed_iceberg_table_sinks,
                removed_iceberg_sink_ids,
            },
            version,
            drop_result,
        })
    }

    pub async fn try_abort_creating_subscription(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let subscription = Subscription::find_by_id(subscription_id).one(&txn).await?;
        let Some(subscription) = subscription else {
            tracing::warn!(
                %subscription_id,
                "subscription not found when aborting creation, might be cleaned by recovery"
            );
            return Ok(());
        };

        if subscription.subscription_state == PbSubscriptionState::Created as i32 {
            tracing::warn!(
                %subscription_id,
                "subscription is already created when aborting creation"
            );
            return Ok(());
        }

        subscription.delete(&txn).await?;
        txn.commit().await?;
        Ok(())
    }
}

async fn report_drop_object(
    object_type: ObjectType,
    object_id: ObjectId,
    txn: &DatabaseTransaction,
) {
    let connector_name = {
        match object_type {
            ObjectType::Sink => Sink::find_by_id(object_id.as_sink_id())
                .select_only()
                .column(sink::Column::Properties)
                .into_tuple::<Property>()
                .one(txn)
                .await
                .ok()
                .flatten()
                .and_then(|properties| properties.inner_ref().get("connector").cloned()),
            ObjectType::Source => Source::find_by_id(object_id.as_source_id())
                .select_only()
                .column(source::Column::WithProperties)
                .into_tuple::<Property>()
                .one(txn)
                .await
                .ok()
                .flatten()
                .and_then(|properties| properties.inner_ref().get("connector").cloned()),
            _ => unreachable!(),
        }
    };
    if let Some(connector_name) = connector_name {
        report_event(
            PbTelemetryEventStage::DropStreamJob,
            "source",
            object_id.as_raw_id() as _,
            Some(connector_name),
            Some(match object_type {
                ObjectType::Source => PbTelemetryDatabaseObject::Source,
                ObjectType::Sink => PbTelemetryDatabaseObject::Sink,
                _ => unreachable!(),
            }),
            None,
        );
    }
}

/// Maximum number of complete object names included in a cascade notice.
const MAX_CASCADE_NOTICE_OBJECTS: usize = 10;

/// Build complete counts and bounded details from the same dependency set used for deletion.
async fn build_drop_result_for_notice(
    root_object_id: ObjectId,
    objects: &[PartialObject],
    txn: &DatabaseTransaction,
) -> MetaResult<DropResult> {
    let mut seen = HashSet::new();
    let mut reportable_objects = Vec::new();
    let mut table_ids = Vec::new();
    for object in objects {
        if object.oid == root_object_id || !seen.insert(object.oid) {
            continue;
        }
        let object_type = match object.obj_type {
            ObjectType::Table => {
                table_ids.push(object.oid);
                continue;
            }
            ObjectType::Source => PbObjectType::Source,
            ObjectType::Sink => PbObjectType::Sink,
            ObjectType::View => PbObjectType::View,
            ObjectType::Function => PbObjectType::Function,
            ObjectType::Connection => PbObjectType::Connection,
            ObjectType::Subscription => PbObjectType::Subscription,
            ObjectType::Secret => PbObjectType::Secret,
            ObjectType::Schema => PbObjectType::Schema,
            // Indexes are implicitly removed with their primary table.
            ObjectType::Index | ObjectType::Database => continue,
        };
        reportable_objects.push((object_type, object.oid));
    }

    // Classify tables without loading names for the entire dependency graph.
    if !table_ids.is_empty() {
        let tables: Vec<(ObjectId, TableType)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::TableType])
            .filter(table::Column::TableId.is_in(table_ids))
            .into_tuple()
            .all(txn)
            .await?;
        for (oid, table_type) in tables {
            let object_type = match table_type {
                TableType::Table => PbObjectType::Table,
                TableType::MaterializedView => PbObjectType::Mview,
                TableType::Index | TableType::Internal | TableType::VectorIndex => continue,
            };
            reportable_objects.push((object_type, oid));
        }
    }

    let (mut drop_result, detail_objects) = summarize_cascade_objects(reportable_objects);
    let grouped = detail_objects.into_iter().into_group_map();
    let mut result = vec![];

    for (object_type, object_ids) in grouped {
        match object_type {
            PbObjectType::Table | PbObjectType::Mview => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Table.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(table::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: object_type as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Source => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Source.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(source::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Source as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Sink => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Sink.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(sink::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Sink as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::View => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::View.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(view::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::View as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Function => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Function.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(function::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Function as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Connection => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Connection.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(connection::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Connection as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Subscription => {
                let rows: Vec<(ObjectId, String, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Subscription.def())
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(subscription::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Subscription as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Secret => {
                let rows: Vec<(ObjectId, String, String)> = Secret::find()
                    .inner_join(Object)
                    .join(JoinType::InnerJoin, object::Relation::Schema2.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .column(secret::Column::Name)
                    .filter(secret::Column::SecretId.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, schema_name, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Secret as _,
                            schema_name: Some(schema_name),
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Schema => {
                let rows: Vec<(ObjectId, String)> = Object::find()
                    .join(JoinType::InnerJoin, object::Relation::Schema.def())
                    .select_only()
                    .column(object::Column::Oid)
                    .column(schema::Column::Name)
                    .filter(object::Column::Oid.is_in(object_ids))
                    .into_tuple()
                    .all(txn)
                    .await?;
                result.extend(rows.into_iter().map(|(oid, object_name)| {
                    (
                        oid,
                        PbCascadeObject {
                            object_type: PbObjectType::Schema as _,
                            schema_name: None,
                            object_name,
                        },
                    )
                }));
            }
            PbObjectType::Index | PbObjectType::Database | PbObjectType::Unspecified => {
                unreachable!("non-reportable cascade object")
            }
        }
    }

    // Queries and hash-map iteration do not preserve the selected (type, ID) order.
    result.sort_by_key(|(oid, object)| (object.object_type, *oid));
    drop_result.cascade_objects = result.into_iter().map(|(_, object)| object).collect();
    Ok(drop_result)
}

fn summarize_cascade_objects(
    mut objects: Vec<(PbObjectType, ObjectId)>,
) -> (DropResult, Vec<(PbObjectType, ObjectId)>) {
    objects.sort_unstable_by_key(|(object_type, oid)| (*object_type as i32, *oid));
    let counts_by_type = objects
        .chunk_by(|a, b| a.0 == b.0)
        .map(|objects| TypeCount {
            object_type: objects[0].0 as _,
            count: objects.len() as u64,
        })
        .collect();
    let result = DropResult {
        total_count: objects.len() as u64,
        counts_by_type,
        cascade_objects: vec![],
    };
    objects.truncate(MAX_CASCADE_NOTICE_OBJECTS);
    (result, objects)
}
