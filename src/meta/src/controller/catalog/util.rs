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

use risingwave_common::catalog::FragmentTypeMask;

use super::*;
use crate::controller::fragment::FragmentTypeMaskExt;

pub(crate) async fn update_internal_tables(
    txn: &DatabaseTransaction,
    object_id: i32,
    column: object::Column,
    new_value: Value,
    objects_to_notify: &mut Vec<PbObjectInfo>,
) -> MetaResult<()> {
    let internal_tables = get_internal_tables_by_id(JobId::new(object_id as _), txn).await?;

    if !internal_tables.is_empty() {
        Object::update_many()
            .col_expr(column, SimpleExpr::Value(new_value))
            .filter(object::Column::Oid.is_in(internal_tables.clone()))
            .exec(txn)
            .await?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(internal_tables))
            .all(txn)
            .await?;
        for (table, table_obj) in table_objs {
            objects_to_notify.push(PbObjectInfo::Table(
                ObjectModel(table, table_obj.unwrap()).into(),
            ));
        }
    }
    Ok(())
}

impl CatalogController {
    pub(crate) async fn init(&self) -> MetaResult<()> {
        self.table_catalog_cdc_table_id_update().await?;
        Ok(())
    }

    /// Fill in the `cdc_table_id` field for Table with empty `cdc_table_id` and parent Source job.
    /// NOTES: We assume Table with a parent Source job is a CDC table
    pub(crate) async fn table_catalog_cdc_table_id_update(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // select Tables which cdc_table_id is empty and has a parent Source job
        let table_and_source_id: Vec<(TableId, String, SourceId)> = Table::find()
            .join(JoinType::InnerJoin, table::Relation::ObjectDependency.def())
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Source.def(),
            )
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns([source::Column::SourceId])
            .filter(
                table::Column::TableType.eq(TableType::Table).and(
                    table::Column::CdcTableId
                        .is_null()
                        .or(table::Column::CdcTableId.eq("")),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // return directly if the result set is empty.
        if table_and_source_id.is_empty() {
            return Ok(());
        }

        info!(table_and_source_id = ?table_and_source_id, "cdc table with empty cdc_table_id");

        let mut cdc_table_ids = HashMap::new();
        for (table_id, definition, source_id) in table_and_source_id {
            match extract_external_table_name_from_definition(&definition) {
                None => {
                    tracing::warn!(
                        %table_id,
                        definition,
                        "failed to extract cdc table name from table definition.",
                    )
                }
                Some(external_table_name) => {
                    cdc_table_ids.insert(
                        table_id,
                        build_cdc_table_id(source_id as u32, &external_table_name),
                    );
                }
            }
        }

        for (table_id, cdc_table_id) in cdc_table_ids {
            table::ActiveModel {
                table_id: Set(table_id as _),
                cdc_table_id: Set(Some(cdc_table_id)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn list_object_dependencies(
        &self,
        include_creating: bool,
    ) -> MetaResult<Vec<PbObjectDependencies>> {
        let inner = self.inner.read().await;

        let dependencies: Vec<(ObjectId, ObjectId)> = {
            let filter = if include_creating {
                Expr::value(true)
            } else {
                streaming_job::Column::JobStatus.eq(JobStatus::Created)
            };
            ObjectDependency::find()
                .select_only()
                .columns([
                    object_dependency::Column::Oid,
                    object_dependency::Column::UsedBy,
                ])
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        let mut obj_dependencies = dependencies
            .into_iter()
            .map(|(oid, used_by)| PbObjectDependencies {
                object_id: used_by as _,
                referenced_object_id: oid as _,
            })
            .collect_vec();

        let view_dependencies: Vec<(ObjectId, ObjectId)> = ObjectDependency::find()
            .select_only()
            .columns([
                object_dependency::Column::Oid,
                object_dependency::Column::UsedBy,
            ])
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::View.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        obj_dependencies.extend(view_dependencies.into_iter().map(|(view_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: view_id as _,
            }
        }));

        let sink_dependencies: Vec<(SinkId, TableId)> = {
            let filter = if include_creating {
                sink::Column::TargetTable.is_not_null()
            } else {
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .and(sink::Column::TargetTable.is_not_null())
            };
            Sink::find()
                .select_only()
                .columns([sink::Column::SinkId, sink::Column::TargetTable])
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(sink_dependencies.into_iter().map(|(sink_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id.as_raw_id(),
                referenced_object_id: sink_id as _,
            }
        }));

        let subscription_dependencies: Vec<(SubscriptionId, TableId)> = {
            let filter = if include_creating {
                subscription::Column::DependentTableId.is_not_null()
            } else {
                subscription::Column::SubscriptionState
                    .eq(Into::<i32>::into(SubscriptionState::Created))
                    .and(subscription::Column::DependentTableId.is_not_null())
            };
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::DependentTableId,
                ])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(subscription_dependencies.into_iter().map(
            |(subscription_id, table_id)| PbObjectDependencies {
                object_id: subscription_id as _,
                referenced_object_id: table_id.as_raw_id(),
            },
        ));

        Ok(obj_dependencies)
    }

    pub(crate) async fn log_cleaned_dirty_jobs(
        &self,
        dirty_objs: &[PartialObject],
        txn: &DatabaseTransaction,
    ) -> MetaResult<()> {
        // Record cleaned streaming jobs in event logs.
        let mut dirty_table_ids = vec![];
        let mut dirty_source_ids = vec![];
        let mut dirty_sink_ids = vec![];
        for dirty_job_obj in dirty_objs {
            let job_id = dirty_job_obj.oid;
            let job_type = dirty_job_obj.obj_type;
            match job_type {
                ObjectType::Table | ObjectType::Index => dirty_table_ids.push(job_id),
                ObjectType::Source => dirty_source_ids.push(job_id),
                ObjectType::Sink => dirty_sink_ids.push(job_id),
                _ => unreachable!("unexpected streaming job type"),
            }
        }

        let mut event_logs = vec![];
        if !dirty_table_ids.is_empty() {
            let table_info: Vec<(TableId, String, String)> = Table::find()
                .select_only()
                .columns([
                    table::Column::TableId,
                    table::Column::Name,
                    table::Column::Definition,
                ])
                .filter(table::Column::TableId.is_in(dirty_table_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (table_id, name, definition) in table_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: table_id.as_raw_id(),
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_source_ids.is_empty() {
            let source_info: Vec<(SourceId, String, String)> = Source::find()
                .select_only()
                .columns([
                    source::Column::SourceId,
                    source::Column::Name,
                    source::Column::Definition,
                ])
                .filter(source::Column::SourceId.is_in(dirty_source_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (source_id, name, definition) in source_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: source_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_sink_ids.is_empty() {
            let sink_info: Vec<(SinkId, String, String)> = Sink::find()
                .select_only()
                .columns([
                    sink::Column::SinkId,
                    sink::Column::Name,
                    sink::Column::Definition,
                ])
                .filter(sink::Column::SinkId.is_in(dirty_sink_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (sink_id, name, definition) in sink_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: sink_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        self.env.event_log_manager_ref().add_event_logs(event_logs);
        Ok(())
    }

    pub(crate) async fn clean_dirty_sink_downstreams(txn: &DatabaseTransaction) -> MetaResult<()> {
        // clean incoming sink from (table)
        // clean upstream fragment ids from (fragment)
        // clean stream node from (fragment)
        // clean upstream actor ids from (actor)

        // The cleanup of fragment and StreamNode is to maintain compatibility with old versions of data. For the
        // current sink-into-table implementation, there is no need to restore the contents of StreamNode, because the
        // `UpstreamSinkUnion` operator does not persist any data, but relies on refill during recovery.

        let all_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_ids: HashSet<_> = all_fragment_ids.into_iter().collect();

        let all_sink_into_tables: Vec<Option<TableId>> = Sink::find()
            .select_only()
            .column(sink::Column::TargetTable)
            .filter(sink::Column::TargetTable.is_not_null())
            .into_tuple()
            .all(txn)
            .await?;

        let mut table_with_incoming_sinks: HashSet<TableId> = HashSet::new();
        for target_table_id in all_sink_into_tables {
            table_with_incoming_sinks.insert(target_table_id.expect("filter by non null"));
        }

        // no need to update, returning
        if table_with_incoming_sinks.is_empty() {
            return Ok(());
        }

        for table_id in table_with_incoming_sinks {
            tracing::info!("cleaning dirty table sink downstream table {}", table_id);

            let fragments: Vec<(FragmentId, StreamNode)> = Fragment::find()
                .select_only()
                .columns(vec![
                    fragment::Column::FragmentId,
                    fragment::Column::StreamNode,
                ])
                .filter(fragment::Column::JobId.eq(table_id).and(
                    // dirty downstream should be materialize fragment of table
                    FragmentTypeMask::intersects(FragmentTypeFlag::Mview),
                ))
                .into_tuple()
                .all(txn)
                .await?;

            for (fragment_id, stream_node) in fragments {
                {
                    let mut dirty_upstream_fragment_ids = HashSet::new();

                    let mut pb_stream_node = stream_node.to_protobuf();

                    visit_stream_node_cont_mut(&mut pb_stream_node, |node| {
                        if let Some(NodeBody::Union(_)) = node.node_body {
                            node.input.retain_mut(|input| match &mut input.node_body {
                                Some(NodeBody::Project(_)) => {
                                    let body = input.input.iter().exactly_one().unwrap();
                                    let Some(NodeBody::Merge(merge_node)) = &body.node_body else {
                                        unreachable!("expect merge node");
                                    };
                                    if all_fragment_ids
                                        .contains(&(merge_node.upstream_fragment_id as i32))
                                    {
                                        true
                                    } else {
                                        dirty_upstream_fragment_ids
                                            .insert(merge_node.upstream_fragment_id);
                                        false
                                    }
                                }
                                Some(NodeBody::Merge(merge_node)) => {
                                    if all_fragment_ids
                                        .contains(&(merge_node.upstream_fragment_id as i32))
                                    {
                                        true
                                    } else {
                                        dirty_upstream_fragment_ids
                                            .insert(merge_node.upstream_fragment_id);
                                        false
                                    }
                                }
                                _ => false,
                            });
                        }
                        true
                    });

                    tracing::info!(
                        "cleaning dirty table sink fragment {:?} from downstream fragment {}",
                        dirty_upstream_fragment_ids,
                        fragment_id
                    );

                    if !dirty_upstream_fragment_ids.is_empty() {
                        tracing::info!(
                            "fixing dirty stream node in downstream fragment {}",
                            fragment_id
                        );
                        Fragment::update_many()
                            .col_expr(
                                fragment::Column::StreamNode,
                                StreamNode::from(&pb_stream_node).into(),
                            )
                            .filter(fragment::Column::FragmentId.eq(fragment_id))
                            .exec(txn)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_subscriptions: Vec<(ObjectId, String, DatabaseId, SchemaId)> =
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::Name,
                ])
                .columns([object::Column::DatabaseId, object::Column::SchemaId])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                .into_tuple()
                .all(&inner.db)
                .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks.into_iter())
            .chain(creating_subscriptions.into_iter())
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| {
                job_mapping.remove(&(info.database_id as _, info.schema_id as _, info.name))
            })
            .collect())
    }
}
