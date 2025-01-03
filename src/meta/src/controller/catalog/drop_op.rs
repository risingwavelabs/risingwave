// Copyright 2025 RisingWave Labs
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

use super::*;

impl CatalogController {
    // Drop all kinds of objects including databases,
    // schemas, relations, connections, functions, etc.
    pub async fn drop_object(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        tracing::debug!("drop object: {:?} {}", object_type, object_id);
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        // TODO: refactor ReleaseContext when the cross-database query is supported.
        let database_id = if object_type == ObjectType::Database {
            object_id
        } else {
            obj.database_id
                .ok_or_else(|| anyhow!("dropped object should have database_id"))?
        };

        let mut to_drop_objects = match drop_mode {
            DropMode::Cascade => get_referring_objects_cascade(object_id, &txn).await?,
            DropMode::Restrict => match object_type {
                ObjectType::Database => unreachable!("database always be dropped in cascade mode"),
                ObjectType::Schema => {
                    ensure_schema_empty(object_id, &txn).await?;
                    Default::default()
                }
                ObjectType::Table => {
                    ensure_object_not_refer(object_type, object_id, &txn).await?;
                    let indexes = get_referring_objects(object_id, &txn).await?;
                    assert!(
                        indexes.iter().all(|obj| obj.obj_type == ObjectType::Index),
                        "only index could be dropped in restrict mode"
                    );
                    indexes
                }
                _ => {
                    ensure_object_not_refer(object_type, object_id, &txn).await?;
                    vec![]
                }
            },
        };
        to_drop_objects.push(obj);

        // TODO: record dependency info in object_dependency table for sink into table.
        // Special handling for 'sink into table'.
        if object_type != ObjectType::Sink {
            // When dropping a table downstream, all incoming sinks of the table should be dropped as well.
            if object_type == ObjectType::Table {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                let incoming_sinks = table.incoming_sinks.into_inner();

                if !incoming_sinks.is_empty() {
                    let objs: Vec<PartialObject> = Object::find()
                        .filter(object::Column::Oid.is_in(incoming_sinks))
                        .into_partial_model()
                        .all(&txn)
                        .await?;

                    to_drop_objects.extend(objs);
                }
            }

            let to_drop_object_ids: HashSet<_> =
                to_drop_objects.iter().map(|obj| obj.oid).collect();

            // When there is a table sink in the dependency chain of drop cascade, an error message needs to be returned currently to manually drop the sink.
            for obj in &to_drop_objects {
                if obj.obj_type == ObjectType::Sink {
                    let sink = Sink::find_by_id(obj.oid)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("sink", obj.oid))?;

                    // Since dropping the sink into the table requires the frontend to handle some of the logic (regenerating the plan), it’s not compatible with the current cascade dropping.
                    if let Some(target_table) = sink.target_table
                        && !to_drop_object_ids.contains(&target_table)
                    {
                        bail!(
                            "Found sink into table with sink id {} in dependency, please drop them manually",
                            obj.oid,
                        );
                    }
                }
            }
        }

        let to_drop_table_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid);
        let mut to_drop_streaming_jobs = to_drop_objects
            .iter()
            .filter(|obj| {
                obj.obj_type == ObjectType::Table
                    || obj.obj_type == ObjectType::Sink
                    || obj.obj_type == ObjectType::Index
            })
            .map(|obj| obj.oid)
            .collect_vec();

        // source streaming job.
        if object_type == ObjectType::Source {
            let source_info: Option<StreamSourceInfo> = Source::find_by_id(object_id)
                .select_only()
                .column(source::Column::SourceInfo)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
            if let Some(source_info) = source_info
                && source_info.to_protobuf().is_shared()
            {
                to_drop_streaming_jobs.push(object_id);
            }
        }

        // Check if there are any streaming jobs that are creating.
        if !to_drop_streaming_jobs.is_empty() {
            let creating = StreamingJob::find()
                .filter(
                    streaming_job::Column::JobStatus
                        .ne(JobStatus::Created)
                        .and(streaming_job::Column::JobId.is_in(to_drop_streaming_jobs.clone())),
                )
                .count(&txn)
                .await?;
            if creating != 0 {
                return Err(MetaError::permission_denied(format!(
                    "can not drop {creating} creating streaming job, please cancel them firstly"
                )));
            }
        }

        let mut to_drop_state_table_ids: HashSet<_> = to_drop_table_ids.clone().collect();

        // Add associated sources.
        let mut to_drop_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(to_drop_table_ids)
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        let to_drop_source_objs: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(to_drop_source_ids.clone()))
            .into_partial_model()
            .all(&txn)
            .await?;
        to_drop_objects.extend(to_drop_source_objs);
        if object_type == ObjectType::Source {
            to_drop_source_ids.push(object_id);
        }

        let to_drop_secret_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Secret)
            .map(|obj| obj.oid)
            .collect_vec();

        if !to_drop_streaming_jobs.is_empty() {
            let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.is_in(to_drop_streaming_jobs.clone()))
                .into_partial_model()
                .all(&txn)
                .await?;

            to_drop_state_table_ids.extend(to_drop_internal_table_objs.iter().map(|obj| obj.oid));
            to_drop_objects.extend(to_drop_internal_table_objs);
        }

        let to_drop_objects: HashMap<_, _> = to_drop_objects
            .into_iter()
            .map(|obj| (obj.oid, obj))
            .collect();

        // TODO: Remove this assertion when the cross-database query is supported.
        to_drop_objects.values().for_each(|obj| {
            if let Some(obj_database_id) = obj.database_id {
                assert_eq!(
                    database_id, obj_database_id,
                    "dropped objects not in the same database: {:?}",
                    obj
                );
            }
        });

        let (source_fragments, removed_actors, removed_fragments) =
            resolve_source_register_info_for_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.keys().cloned()))
            .into_tuple()
            .all(&txn)
            .await?;

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.keys().cloned()))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        self.notify_users_update(user_infos).await;

        let version = match object_type {
            ObjectType::Database => {
                // TODO: Notify objects in other databases when the cross-database query is supported.
                self.notify_frontend(
                    NotificationOperation::Delete,
                    NotificationInfo::Database(PbDatabase {
                        id: database_id as _,
                        ..Default::default()
                    }),
                )
                .await
            }
            ObjectType::Schema => {
                let (schema_obj, mut to_notify_objs): (Vec<_>, Vec<_>) = to_drop_objects
                    .into_values()
                    .partition(|obj| obj.obj_type == ObjectType::Schema && obj.oid == object_id);

                let schema_obj = schema_obj
                    .into_iter()
                    .exactly_one()
                    .expect("schema object not found");
                to_notify_objs.push(schema_obj);

                let relation_group = build_object_group_for_delete(to_notify_objs);
                self.notify_frontend(NotificationOperation::Delete, relation_group)
                    .await
            }
            _ => {
                let relation_group =
                    build_object_group_for_delete(to_drop_objects.into_values().collect());
                self.notify_frontend(NotificationOperation::Delete, relation_group)
                    .await
            }
        };

        let fragment_mappings = removed_fragments
            .iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: *fragment_id as _,
                mapping: None,
            })
            .collect();

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;

        Ok((
            ReleaseContext {
                database_id,
                streaming_job_ids: to_drop_streaming_jobs,
                state_table_ids: to_drop_state_table_ids.into_iter().collect(),
                source_ids: to_drop_source_ids,
                secret_ids: to_drop_secret_ids,
                source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
    }
}
