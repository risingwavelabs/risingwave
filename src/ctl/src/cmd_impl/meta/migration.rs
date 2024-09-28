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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use chrono::DateTime;
use etcd_client::ConnectOptions;
use itertools::Itertools;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_tables;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_meta::controller::catalog::CatalogController;
use risingwave_meta::controller::system_param::system_params_to_model;
use risingwave_meta::controller::SqlMetaStore;
use risingwave_meta::hummock::compaction::CompactStatus;
use risingwave_meta::hummock::model::CompactionGroup;
use risingwave_meta::manager::model::SystemParamsModel;
use risingwave_meta::model;
use risingwave_meta::model::{ClusterId, MetadataModel, NotificationVersion, TableParallelism};
use risingwave_meta::storage::{
    EtcdMetaStore, MetaStore, MetaStoreBoxExt, MetaStoreError, MetaStoreRef,
    WrappedEtcdClient as EtcdClient, DEFAULT_COLUMN_FAMILY,
};
use risingwave_meta::stream::TableRevision;
use risingwave_meta_model_migration::{Migrator, MigratorTrait};
use risingwave_meta_model_v2::catalog_version::VersionCategory;
use risingwave_meta_model_v2::compaction_status::LevelHandlers;
use risingwave_meta_model_v2::hummock_sequence::{
    COMPACTION_GROUP_ID, COMPACTION_TASK_ID, META_BACKUP_ID, SSTABLE_OBJECT_ID,
};
use risingwave_meta_model_v2::hummock_version_stats::TableStats;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{
    Actor, ActorDispatcher, CatalogVersion, Cluster, Connection, Database, Fragment, Function,
    Index, Object, ObjectDependency, Schema, Secret, Sink, Source, StreamingJob, Subscription,
    SystemParameter, Table, User, UserPrivilege, View, Worker, WorkerProperty,
};
use risingwave_meta_model_v2::{
    catalog_version, cluster, compaction_config, compaction_status, compaction_task, connection,
    database, function, hummock_pinned_snapshot, hummock_pinned_version, hummock_sequence,
    hummock_version_delta, hummock_version_stats, index, object, object_dependency, schema, secret,
    sink, source, streaming_job, subscription, table, user, user_privilege, view, worker,
    worker_property, CreateType, JobStatus, ObjectId, StreamingParallelism,
};
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::{
    PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::{
    CompactTaskAssignment, HummockPinnedSnapshot, HummockPinnedVersion, HummockVersionStats,
};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::PbSystemParams;
use risingwave_pb::user::grant_privilege::PbObject as GrantObject;
use risingwave_pb::user::PbUserInfo;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DbBackend, EntityTrait, IntoActiveModel, NotSet,
    QueryFilter, QuerySelect, Statement,
};
use thiserror_ext::AsReport;
use uuid::Uuid;

pub struct EtcdBackend {
    pub(crate) endpoints: Vec<String>,
    pub(crate) credentials: Option<(String, String)>,
}

pub async fn migrate(from: EtcdBackend, target: String, force_clean: bool) -> anyhow::Result<()> {
    // 1. init etcd meta store.
    let mut options =
        ConnectOptions::default().with_keep_alive(Duration::from_secs(3), Duration::from_secs(5));
    if let Some((username, password)) = &from.credentials {
        options = options.with_user(username, password)
    }
    let auth_enabled = from.credentials.is_some();
    let client = EtcdClient::connect(from.endpoints.clone(), Some(options.clone()), auth_enabled)
        .await
        .context("failed to connect etcd")?;
    let meta_store = EtcdMetaStore::new(client).into_ref();

    // 2. init sql meta store.
    let mut options = sea_orm::ConnectOptions::new(target);
    options
        .max_connections(10)
        .connect_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(30));
    let conn = sea_orm::Database::connect(options).await?;
    let meta_store_sql = SqlMetaStore::new(conn);

    if force_clean {
        Migrator::down(&meta_store_sql.conn, None)
            .await
            .expect("failed to clean sql backend");
    }
    Migrator::up(&meta_store_sql.conn, None)
        .await
        .expect("failed to init sql backend");

    // cluster Id.
    let cluster_id: Uuid = ClusterId::from_meta_store(&meta_store)
        .await?
        .expect("cluster id not found")
        .parse()?;

    let generated_cluster_id: Uuid = Cluster::find()
        .select_only()
        .column(cluster::Column::ClusterId)
        .into_tuple()
        .one(&meta_store_sql.conn)
        .await?
        .expect("cluster id not found");

    Cluster::update_many()
        .col_expr(cluster::Column::ClusterId, cluster_id.into())
        .filter(cluster::Column::ClusterId.eq(generated_cluster_id))
        .exec(&meta_store_sql.conn)
        .await?;
    println!("cluster id updated to {}", cluster_id);

    // system parameters.
    let system_parameters = PbSystemParams::get(&meta_store)
        .await?
        .expect("system parameters not found");
    SystemParameter::insert_many(system_params_to_model(&system_parameters)?)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("system parameters migrated");

    // workers.
    let workers = model::Worker::list(&meta_store).await?;
    let next_worker_id = workers
        .iter()
        .map(|w| w.worker_node.id + 1)
        .max()
        .unwrap_or(1);
    for worker in workers {
        Worker::insert(worker::ActiveModel::from(&worker.worker_node))
            .exec(&meta_store_sql.conn)
            .await?;
        if worker.worker_type() == WorkerType::ComputeNode
            || worker.worker_type() == WorkerType::Frontend
        {
            let pb_property = worker.worker_node.property.as_ref().unwrap();
            let property = worker_property::ActiveModel {
                worker_id: Set(worker.worker_id() as _),
                is_streaming: Set(pb_property.is_streaming),
                is_serving: Set(pb_property.is_serving),
                is_unschedulable: Set(pb_property.is_unschedulable),
                parallelism: Set(worker.worker_node.parallelism() as _),
                internal_rpc_host_addr: Set(Some(pb_property.internal_rpc_host_addr.clone())),
            };
            WorkerProperty::insert(property)
                .exec(&meta_store_sql.conn)
                .await?;
        }
    }
    println!("worker nodes migrated");

    // catalogs.
    let databases = PbDatabase::list(&meta_store).await?;
    let schemas = PbSchema::list(&meta_store).await?;
    let users = PbUserInfo::list(&meta_store).await?;
    let tables = PbTable::list(&meta_store).await?;
    let sources = PbSource::list(&meta_store).await?;
    let sinks = PbSink::list(&meta_store).await?;
    let indexes = PbIndex::list(&meta_store).await?;
    let views = PbView::list(&meta_store).await?;
    let functions = PbFunction::list(&meta_store).await?;
    let connections = PbConnection::list(&meta_store).await?;
    let subscriptions = PbSubscription::list(&meta_store).await?;
    let secrets = PbSecret::list(&meta_store).await?;

    // inuse object ids.
    let mut inuse_obj_ids = tables
        .iter()
        .map(|t| t.id)
        .chain(sources.iter().map(|s| s.id))
        .chain(sinks.iter().map(|s| s.id))
        .chain(indexes.iter().map(|i| i.id))
        .chain(views.iter().map(|v| v.id))
        .chain(subscriptions.iter().map(|s| s.id))
        .collect::<BTreeSet<_>>();

    // Helper function to get next available id.
    let mut next_available_id = || -> u32 {
        let id = inuse_obj_ids
            .iter()
            .enumerate()
            .find(|(i, id)| i + 1 != **id as usize)
            .map(|(i, _)| i + 1)
            .unwrap_or(inuse_obj_ids.len() + 1) as u32;
        inuse_obj_ids.insert(id);
        id
    };

    // simply truncate all objects.
    Object::delete_many()
        .filter(object::Column::Oid.ne(0))
        .exec(&meta_store_sql.conn)
        .await?;
    User::delete_many()
        .filter(user::Column::UserId.ne(0))
        .exec(&meta_store_sql.conn)
        .await?;

    // user
    let user_models = users
        .iter()
        .map(|u| user::ActiveModel::from(u.clone()))
        .collect_vec();
    User::insert_many(user_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("users migrated");

    // database
    let mut db_rewrite = HashMap::new();
    for mut db in databases {
        let id = next_available_id();
        db_rewrite.insert(db.id, id);
        db.id = id as _;

        let obj = object::ActiveModel {
            oid: Set(id as _),
            obj_type: Set(ObjectType::Database),
            owner_id: Set(db.owner as _),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
        Database::insert(database::ActiveModel::from(db))
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("databases migrated");

    // schema
    let mut schema_rewrite = HashMap::new();
    for mut schema in schemas {
        let id = next_available_id();
        schema_rewrite.insert(schema.id, id);
        schema.id = id as _;

        let obj = object::ActiveModel {
            oid: Set(id as _),
            obj_type: Set(ObjectType::Schema),
            owner_id: Set(schema.owner as _),
            database_id: Set(Some(*db_rewrite.get(&schema.database_id).unwrap() as _)),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
        Schema::insert(schema::ActiveModel::from(schema))
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("schemas migrated");

    // function
    let mut function_rewrite = HashMap::new();
    for mut function in functions {
        let id = next_available_id();
        function_rewrite.insert(function.id, id);
        function.id = id as _;

        let obj = object::ActiveModel {
            oid: Set(id as _),
            obj_type: Set(ObjectType::Function),
            owner_id: Set(function.owner as _),
            database_id: Set(Some(*db_rewrite.get(&function.database_id).unwrap() as _)),
            schema_id: Set(Some(*schema_rewrite.get(&function.schema_id).unwrap() as _)),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
        Function::insert(function::ActiveModel::from(function))
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("functions migrated");

    // connection mapping
    let mut connection_rewrite = HashMap::new();
    for mut connection in connections {
        let id = next_available_id();
        connection_rewrite.insert(connection.id, id);
        connection.id = id as _;

        let obj = object::ActiveModel {
            oid: Set(id as _),
            obj_type: Set(ObjectType::Connection),
            owner_id: Set(connection.owner as _),
            database_id: Set(Some(*db_rewrite.get(&connection.database_id).unwrap() as _)),
            schema_id: Set(Some(
                *schema_rewrite.get(&connection.schema_id).unwrap() as _
            )),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
        Connection::insert(connection::ActiveModel::from(connection))
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("connections migrated");

    // secret mapping
    let mut secret_rewrite = HashMap::new();
    for mut secret in secrets {
        let id = next_available_id();
        secret_rewrite.insert(secret.id, id);
        secret.id = id as _;

        let obj = object::ActiveModel {
            oid: Set(id as _),
            obj_type: Set(ObjectType::Secret),
            owner_id: Set(secret.owner as _),
            database_id: Set(Some(*db_rewrite.get(&secret.database_id).unwrap() as _)),
            schema_id: Set(Some(*schema_rewrite.get(&secret.schema_id).unwrap() as _)),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
        Secret::insert(secret::ActiveModel::from(secret))
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("secrets migrated");

    // add object: table, source, sink, index, view, subscription.
    macro_rules! insert_objects {
        ($objects:expr, $object_type:expr) => {
            for object in &$objects {
                let mut obj = object::ActiveModel {
                    oid: Set(object.id as _),
                    obj_type: Set($object_type),
                    owner_id: Set(object.owner as _),
                    database_id: Set(Some(*db_rewrite.get(&object.database_id).unwrap() as _)),
                    schema_id: Set(Some(*schema_rewrite.get(&object.schema_id).unwrap() as _)),
                    initialized_at_cluster_version: Set(object
                        .initialized_at_cluster_version
                        .clone()),
                    created_at_cluster_version: Set(object.created_at_cluster_version.clone()),
                    ..Default::default()
                };
                if let Some(epoch) = object.initialized_at_epoch.map(Epoch::from) {
                    obj.initialized_at =
                        Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _)
                            .unwrap()
                            .naive_utc());
                }
                if let Some(epoch) = object.created_at_epoch.map(Epoch::from) {
                    obj.created_at =
                        Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _)
                            .unwrap()
                            .naive_utc());
                }
                Object::insert(obj).exec(&meta_store_sql.conn).await?;
            }
        };
    }
    insert_objects!(sources, ObjectType::Source);
    insert_objects!(sinks, ObjectType::Sink);
    insert_objects!(indexes, ObjectType::Index);
    insert_objects!(subscriptions, ObjectType::Subscription);
    for table in &tables {
        if table.table_type() == PbTableType::Index {
            // we only store index object.
            continue;
        }
        let mut obj = object::ActiveModel {
            oid: Set(table.id as _),
            obj_type: Set(ObjectType::Table),
            owner_id: Set(table.owner as _),
            database_id: Set(Some(*db_rewrite.get(&table.database_id).unwrap() as _)),
            schema_id: Set(Some(*schema_rewrite.get(&table.schema_id).unwrap() as _)),
            initialized_at_cluster_version: Set(table.initialized_at_cluster_version.clone()),
            created_at_cluster_version: Set(table.created_at_cluster_version.clone()),
            ..Default::default()
        };
        if let Some(epoch) = table.initialized_at_epoch.map(Epoch::from) {
            obj.initialized_at = Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _)
                .unwrap()
                .naive_utc());
        }
        if let Some(epoch) = table.created_at_epoch.map(Epoch::from) {
            obj.created_at = Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _)
                .unwrap()
                .naive_utc());
        }
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
    }
    for view in &views {
        let obj = object::ActiveModel {
            oid: Set(view.id as _),
            obj_type: Set(ObjectType::View),
            owner_id: Set(view.owner as _),
            database_id: Set(Some(*db_rewrite.get(&view.database_id).unwrap() as _)),
            schema_id: Set(Some(*schema_rewrite.get(&view.schema_id).unwrap() as _)),
            ..Default::default()
        };
        Object::insert(obj).exec(&meta_store_sql.conn).await?;
    }

    // table fragments.
    let table_fragments = model::TableFragments::list(&meta_store).await?;
    let mut fragment_job_map = HashMap::new();
    let mut fragments = vec![];
    let mut actors = vec![];
    let mut actor_dispatchers = vec![];

    for table_fragment in table_fragments {
        let streaming_parallelism = match &table_fragment.assigned_parallelism {
            TableParallelism::Adaptive => StreamingParallelism::Adaptive,
            TableParallelism::Fixed(n) => StreamingParallelism::Fixed(*n),
            TableParallelism::Custom => StreamingParallelism::Custom,
        };
        let status = match table_fragment.state() {
            State::Unspecified => unreachable!(),
            State::Initial => JobStatus::Initial,
            State::Creating => JobStatus::Creating,
            State::Created => JobStatus::Created,
        };
        StreamingJob::insert(streaming_job::ActiveModel {
            job_id: Set(table_fragment.table_id().table_id as _),
            job_status: Set(status),
            create_type: Set(CreateType::Foreground),
            timezone: Set(table_fragment.timezone()),
            parallelism: Set(streaming_parallelism),
        })
        .exec(&meta_store_sql.conn)
        .await?;

        let fragment_actors = CatalogController::extract_fragment_and_actors_from_table_fragments(
            table_fragment.to_protobuf(),
        )
        .unwrap();
        for (fragment, a, ad) in fragment_actors {
            fragment_job_map.insert(
                fragment.fragment_id as u32,
                table_fragment.table_id().table_id as ObjectId,
            );
            fragments.push(fragment);
            actors.extend(a);
            actor_dispatchers.extend(ad);
        }
    }
    for fragment in fragments {
        // rewrite conflict ids.
        let mut stream_node = fragment.stream_node.to_protobuf();
        visit_stream_node_tables(&mut stream_node, |table, _| {
            table.database_id = *db_rewrite.get(&table.database_id).unwrap();
            table.schema_id = *schema_rewrite.get(&table.schema_id).unwrap();
        });
        let mut fragment = fragment.into_active_model();
        fragment.stream_node = Set((&stream_node).into());
        Fragment::insert(fragment)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    // Add actors and actor dispatchers.
    for actor in actors {
        let actor = actor.into_active_model();
        Actor::insert(actor).exec(&meta_store_sql.conn).await?;
    }
    for (_, actor_dispatchers) in actor_dispatchers {
        for actor_dispatcher in actor_dispatchers {
            let mut actor_dispatcher = actor_dispatcher.into_active_model();
            actor_dispatcher.id = NotSet;
            ActorDispatcher::insert(actor_dispatcher)
                .exec(&meta_store_sql.conn)
                .await?;
        }
    }
    println!("table fragments migrated");

    let mut object_dependencies = vec![];

    // catalogs.
    // source
    if !sources.is_empty() {
        let source_models: Vec<source::ActiveModel> = sources
            .into_iter()
            .map(|mut src| {
                let mut dependent_secret_ids = HashSet::new();
                if let Some(id) = src.connection_id.as_mut() {
                    *id = *connection_rewrite.get(id).unwrap();
                }
                for secret_ref in src.secret_refs.values_mut() {
                    secret_ref.secret_id = *secret_rewrite.get(&secret_ref.secret_id).unwrap();
                    dependent_secret_ids.insert(secret_ref.secret_id);
                }
                if let Some(info) = &mut src.info {
                    for secret_ref in info.format_encode_secret_refs.values_mut() {
                        secret_ref.secret_id = *secret_rewrite.get(&secret_ref.secret_id).unwrap();
                        dependent_secret_ids.insert(secret_ref.secret_id);
                    }
                }
                object_dependencies.extend(dependent_secret_ids.into_iter().map(|secret_id| {
                    object_dependency::ActiveModel {
                        id: NotSet,
                        oid: Set(secret_id as _),
                        used_by: Set(src.id as _),
                    }
                }));
                src.into()
            })
            .collect();
        Source::insert_many(source_models)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("sources migrated");

    // table
    for table in tables {
        let job_id = if table.table_type() == PbTableType::Internal {
            Set(Some(*fragment_job_map.get(&table.fragment_id).unwrap()))
        } else {
            NotSet
        };
        object_dependencies.extend(table.dependent_relations.iter().map(|id| {
            object_dependency::ActiveModel {
                id: NotSet,
                oid: Set(*id as _),
                used_by: Set(table.id as _),
            }
        }));
        let mut t: table::ActiveModel = table.into();
        t.belongs_to_job_id = job_id;
        Table::insert(t).exec(&meta_store_sql.conn).await?;
    }
    println!("tables migrated");

    // index
    if !indexes.is_empty() {
        let index_models: Vec<index::ActiveModel> =
            indexes.into_iter().map(|i| i.into()).collect_vec();
        Index::insert_many(index_models)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("indexes migrated");

    // sink
    if !sinks.is_empty() {
        let sink_models: Vec<sink::ActiveModel> = sinks
            .into_iter()
            .map(|mut s| {
                object_dependencies.extend(s.dependent_relations.iter().map(|id| {
                    object_dependency::ActiveModel {
                        id: NotSet,
                        oid: Set(*id as _),
                        used_by: Set(s.id as _),
                    }
                }));
                if let Some(id) = s.connection_id.as_mut() {
                    *id = *connection_rewrite.get(id).unwrap();
                }
                let mut dependent_secret_ids = HashSet::new();
                for secret_ref in s.secret_refs.values_mut() {
                    secret_ref.secret_id = *secret_rewrite.get(&secret_ref.secret_id).unwrap();
                    dependent_secret_ids.insert(secret_ref.secret_id);
                }
                if let Some(desc) = &mut s.format_desc {
                    for secret_ref in desc.secret_refs.values_mut() {
                        secret_ref.secret_id = *secret_rewrite.get(&secret_ref.secret_id).unwrap();
                        dependent_secret_ids.insert(secret_ref.secret_id);
                    }
                }
                object_dependencies.extend(dependent_secret_ids.into_iter().map(|secret_id| {
                    object_dependency::ActiveModel {
                        id: NotSet,
                        oid: Set(secret_id as _),
                        used_by: Set(s.id as _),
                    }
                }));
                s.into()
            })
            .collect();
        Sink::insert_many(sink_models)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("sinks migrated");

    // view
    if !views.is_empty() {
        let view_models: Vec<view::ActiveModel> = views
            .into_iter()
            .map(|v| {
                object_dependencies.extend(v.dependent_relations.iter().map(|id| {
                    object_dependency::ActiveModel {
                        id: NotSet,
                        oid: Set(*id as _),
                        used_by: Set(v.id as _),
                    }
                }));
                v.into()
            })
            .collect();
        View::insert_many(view_models)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("views migrated");

    // subscriptions
    if !subscriptions.is_empty() {
        let subscription_models: Vec<subscription::ActiveModel> = subscriptions
            .into_iter()
            .map(|s| {
                object_dependencies.push(object_dependency::ActiveModel {
                    id: NotSet,
                    oid: Set(s.dependent_table_id as _),
                    used_by: Set(s.id as _),
                });
                s.into()
            })
            .collect();
        Subscription::insert_many(subscription_models)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("subscriptions migrated");

    // object_dependency
    if !object_dependencies.is_empty() {
        ObjectDependency::insert_many(object_dependencies)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("object dependencies migrated");

    // user privilege
    let mut privileges = vec![];
    assert!(!users.is_empty());
    let next_user_id = users.iter().map(|u| u.id + 1).max().unwrap();
    for user in users {
        for gp in user.grant_privileges {
            let id = match gp.get_object()? {
                GrantObject::DatabaseId(id) => *db_rewrite.get(id).unwrap(),
                GrantObject::SchemaId(id) => *schema_rewrite.get(id).unwrap(),
                GrantObject::FunctionId(id) => *function_rewrite.get(id).unwrap(),
                GrantObject::TableId(id)
                | GrantObject::SourceId(id)
                | GrantObject::SinkId(id)
                | GrantObject::ViewId(id)
                | GrantObject::SubscriptionId(id) => *id,
                ty => unreachable!("invalid object type: {:?}", ty),
            };
            for action_with_opt in &gp.action_with_opts {
                privileges.push(user_privilege::ActiveModel {
                    user_id: Set(user.id as _),
                    oid: Set(id as _),
                    granted_by: Set(action_with_opt.granted_by as _),
                    action: Set(action_with_opt.get_action()?.into()),
                    with_grant_option: Set(action_with_opt.with_grant_option),
                    ..Default::default()
                });
            }
        }
    }
    if !privileges.is_empty() {
        UserPrivilege::insert_many(privileges)
            .exec(&meta_store_sql.conn)
            .await?;
    }
    println!("user privileges migrated");

    // notification.
    let notification_version = NotificationVersion::new(&meta_store).await;
    CatalogVersion::insert(catalog_version::ActiveModel {
        name: Set(VersionCategory::Notification),
        version: Set(notification_version.version() as _),
    })
    .exec(&meta_store_sql.conn)
    .await?;
    println!("notification version migrated");

    // table revision.
    let table_revision = TableRevision::get(&meta_store).await?;
    CatalogVersion::insert(catalog_version::ActiveModel {
        name: Set(VersionCategory::TableRevision),
        version: Set(table_revision.inner() as _),
    })
    .exec(&meta_store_sql.conn)
    .await?;
    println!("table revision migrated");

    // hummock.
    // hummock pinned snapshots
    let pinned_snapshots = HummockPinnedSnapshot::list(&meta_store).await?;
    if !pinned_snapshots.is_empty() {
        hummock_pinned_snapshot::Entity::insert_many(
            pinned_snapshots
                .into_iter()
                .map(|ps| hummock_pinned_snapshot::ActiveModel {
                    context_id: Set(ps.context_id as _),
                    min_pinned_snapshot: Set(ps.minimal_pinned_snapshot as _),
                })
                .collect_vec(),
        )
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("hummock pinned snapshots migrated");

    // hummock pinned version
    let pinned_version = HummockPinnedVersion::list(&meta_store).await?;
    if !pinned_version.is_empty() {
        hummock_pinned_version::Entity::insert_many(
            pinned_version
                .into_iter()
                .map(|pv| hummock_pinned_version::ActiveModel {
                    context_id: Set(pv.context_id as _),
                    min_pinned_id: Set(pv.min_pinned_id as _),
                })
                .collect_vec(),
        )
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("hummock pinned version migrated");

    // hummock version delta
    let version_delta = HummockVersionDelta::list(&meta_store).await?;
    if !version_delta.is_empty() {
        hummock_version_delta::Entity::insert_many(
            version_delta
                .into_iter()
                .map(|vd| hummock_version_delta::ActiveModel {
                    id: Set(vd.id.to_u64() as _),
                    prev_id: Set(vd.prev_id.to_u64() as _),
                    max_committed_epoch: Set(vd.visible_table_committed_epoch() as _),
                    safe_epoch: Set(0 as _),
                    trivial_move: Set(vd.trivial_move),
                    full_version_delta: Set((&vd.to_protobuf()).into()),
                })
                .collect_vec(),
        )
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("hummock version delta migrated");

    // hummock version stat
    let version_stats = HummockVersionStats::list(&meta_store)
        .await?
        .into_iter()
        .next();
    if let Some(version_stats) = version_stats {
        hummock_version_stats::Entity::insert(hummock_version_stats::ActiveModel {
            id: Set(version_stats.hummock_version_id as _),
            stats: Set(TableStats(version_stats.table_stats)),
        })
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("hummock version stats migrated");

    // compaction
    // compaction config
    let compaction_groups = CompactionGroup::list(&meta_store).await?;
    if !compaction_groups.is_empty() {
        compaction_config::Entity::insert_many(
            compaction_groups
                .into_iter()
                .map(|cg| compaction_config::ActiveModel {
                    compaction_group_id: Set(cg.group_id as _),
                    config: Set((&*cg.compaction_config).into()),
                })
                .collect_vec(),
        )
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("compaction config migrated");

    // compaction status
    let compaction_statuses = CompactStatus::list(&meta_store).await?;
    if !compaction_statuses.is_empty() {
        compaction_status::Entity::insert_many(
            compaction_statuses
                .into_iter()
                .map(|cs| compaction_status::ActiveModel {
                    compaction_group_id: Set(cs.compaction_group_id as _),
                    status: Set(LevelHandlers::from(
                        cs.level_handlers.iter().map_into().collect_vec(),
                    )),
                })
                .collect_vec(),
        )
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("compaction status migrated");

    // compaction task
    let compaction_tasks = CompactTaskAssignment::list(&meta_store).await?;
    if !compaction_tasks.is_empty() {
        compaction_task::Entity::insert_many(compaction_tasks.into_iter().map(|ct| {
            let context_id = ct.context_id;
            let task = ct.compact_task.unwrap();
            compaction_task::ActiveModel {
                id: Set(task.task_id as _),
                context_id: Set(context_id as _),
                task: Set((&task).into()),
            }
        }))
        .exec(&meta_store_sql.conn)
        .await?;
    }
    println!("compaction task migrated");

    // hummock sequence
    let sst_obj_id = load_current_id(&meta_store, "hummock_ss_table_id", Some(1)).await;
    let compaction_task_id = load_current_id(&meta_store, "hummock_compaction_task", Some(1)).await;
    let compaction_group_id = load_current_id(
        &meta_store,
        "compaction_group",
        Some(StaticCompactionGroupId::End as u64 + 1),
    )
    .await;
    let backup_id = load_current_id(&meta_store, "backup", Some(1)).await;
    hummock_sequence::Entity::insert_many(vec![
        hummock_sequence::ActiveModel {
            name: Set(SSTABLE_OBJECT_ID.into()),
            seq: Set(sst_obj_id as _),
        },
        hummock_sequence::ActiveModel {
            name: Set(COMPACTION_TASK_ID.into()),
            seq: Set(compaction_task_id as _),
        },
        hummock_sequence::ActiveModel {
            name: Set(COMPACTION_GROUP_ID.into()),
            seq: Set(compaction_group_id as _),
        },
        hummock_sequence::ActiveModel {
            name: Set(META_BACKUP_ID.into()),
            seq: Set(backup_id as _),
        },
    ])
    .exec(&meta_store_sql.conn)
    .await?;
    println!("hummock sequence migrated");

    // Rest sequence for object and user.
    match meta_store_sql.conn.get_database_backend() {
        DbBackend::MySql => {
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE worker AUTO_INCREMENT = {next_worker_id};"),
                ))
                .await?;
            let next_object_id = next_available_id();
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE object AUTO_INCREMENT = {next_object_id};"),
                ))
                .await?;
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE user AUTO_INCREMENT = {next_user_id};"),
                ))
                .await?;
        }
        DbBackend::Postgres => {
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT setval('worker_worker_id_seq', (SELECT MAX(worker_id) FROM worker));",
                ))
                .await?;
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT setval('object_oid_seq', (SELECT MAX(oid) FROM object));",
                ))
                .await?;
            meta_store_sql
                .conn
                .execute(Statement::from_string(
                    DatabaseBackend::Postgres,
                    "SELECT setval('user_user_id_seq', (SELECT MAX(user_id) FROM \"user\"));",
                ))
                .await?;
        }
        DbBackend::Sqlite => {}
    }

    Ok(())
}

async fn load_current_id(meta_store: &MetaStoreRef, category: &str, start: Option<u64>) -> u64 {
    let category_gen_key = format!("{}_id_next_generator", category);
    let res = meta_store
        .get_cf(DEFAULT_COLUMN_FAMILY, category_gen_key.as_bytes())
        .await;
    match res {
        Ok(value) => memcomparable::from_slice(&value).unwrap(),
        Err(MetaStoreError::ItemNotFound(_)) => start.unwrap_or(0),
        Err(e) => panic!("{}", e.as_report()),
    }
}
