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

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use anyhow::Context;
use etcd_client::ConnectOptions;
use itertools::Itertools;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_tables;
use risingwave_meta::controller::catalog::CatalogController;
use risingwave_meta::controller::system_param::system_params_to_model;
use risingwave_meta::controller::SqlMetaStore;
use risingwave_meta::manager::model::SystemParamsModel;
use risingwave_meta::model;
use risingwave_meta::model::{ClusterId, MetadataModel, TableParallelism};
use risingwave_meta::storage::{EtcdMetaStore, MetaStoreBoxExt, WrappedEtcdClient as EtcdClient};
use risingwave_meta_model_migration::{Migrator, MigratorTrait};
use risingwave_meta_model_v2::fragment::StreamNode;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{
    Actor, ActorDispatcher, Cluster, Connection, Database, Fragment, Function, Index, Object,
    ObjectDependency, Schema, Sink, Source, StreamingJob, Subscription, SystemParameter, Table,
    User, UserPrivilege, View, Worker, WorkerProperty,
};
use risingwave_meta_model_v2::{
    cluster, connection, database, function, index, object, object_dependency, schema, sink,
    source, streaming_job, subscription, table, user, user_privilege, view, worker,
    worker_property, CreateType, JobStatus, StreamingParallelism,
};
use risingwave_pb::catalog::{
    PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbSubscription,
    PbTable, PbView,
};
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::PbSystemParams;
use risingwave_pb::user::grant_privilege::PbObject as GrantObject;
use risingwave_pb::user::PbUserInfo;
use sea_orm::prelude::DateTime;
use sea_orm::ActiveValue::Set;
use sea_orm::{ColumnTrait, EntityTrait, IntoActiveModel, NotSet, QueryFilter, QuerySelect};
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
    for worker in workers {
        Worker::insert(worker::ActiveModel::from(&worker.worker_node))
            .exec(&meta_store_sql.conn)
            .await?;
        if worker.worker_type() == WorkerType::ComputeNode {
            let pb_property = worker.worker_node.property.as_ref().unwrap();
            let parallel_unit_ids = worker
                .worker_node
                .parallel_units
                .iter()
                .map(|pu| pu.id as i32)
                .collect_vec();
            let property = worker_property::ActiveModel {
                worker_id: Set(worker.worker_id() as _),
                parallel_unit_ids: Set(parallel_unit_ids.into()),
                is_streaming: Set(pb_property.is_streaming),
                is_serving: Set(pb_property.is_serving),
                is_unschedulable: Set(pb_property.is_unschedulable),
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
    // FIXME: add user_privileges.
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
    for db in databases {
        let id = next_available_id();
        db_rewrite.insert(db.id, id);

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
    for schema in schemas {
        let id = next_available_id();
        schema_rewrite.insert(schema.id, id);

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
    for function in functions {
        let id = next_available_id();
        function_rewrite.insert(function.id, id);

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
    for connection in connections {
        let id = next_available_id();
        connection_rewrite.insert(connection.id, id);

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
                    ..Default::default()
                };
                if let Some(epoch) = object.initialized_at_epoch.map(Epoch::from) {
                    obj.initialized_at =
                        Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _).unwrap());
                }
                if let Some(epoch) = object.created_at_epoch.map(Epoch::from) {
                    obj.created_at =
                        Set(DateTime::from_timestamp_millis(epoch.as_unix_millis() as _).unwrap());
                }
                Object::insert(obj).exec(&meta_store_sql.conn).await?;
            }
        };
    }
    insert_objects!(tables, ObjectType::Table);
    insert_objects!(sources, ObjectType::Source);
    insert_objects!(sinks, ObjectType::Sink);
    insert_objects!(indexes, ObjectType::Index);
    insert_objects!(subscriptions, ObjectType::Subscription);
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
    for table_fragment in table_fragments {
        let streaming_parallelism = match &table_fragment.assigned_parallelism {
            TableParallelism::Adaptive => StreamingParallelism::Adaptive,
            TableParallelism::Fixed(n) => StreamingParallelism::Fixed(*n),
            TableParallelism::Custom => todo!("custom parallelism"),
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
        let (fragments, actor_with_dispatchers): (Vec<_>, Vec<_>) = fragment_actors
            .into_iter()
            .map(|(fragment, actors, actor_dispatchers)| (fragment, (actors, actor_dispatchers)))
            .unzip();
        for fragment in fragments {
            // rewrite conflict ids.
            let mut stream_node = fragment.stream_node.to_protobuf();
            visit_stream_node_tables(&mut stream_node, |table, _| {
                table.database_id = *db_rewrite.get(&table.database_id).unwrap();
                table.schema_id = *schema_rewrite.get(&table.schema_id).unwrap();
            });
            // Question: Do we need to rewrite the function ids?
            let mut fragment = fragment.into_active_model();
            fragment.stream_node = Set(StreamNode::from_protobuf(&stream_node));
            Fragment::insert(fragment)
                .exec(&meta_store_sql.conn)
                .await?;
        }
        // Add actors and actor dispatchers.
        for (actors, actor_dispatchers) in actor_with_dispatchers {
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
        }
    }
    println!("table fragments migrated");

    // catalogs.
    // source
    let source_models: Vec<source::ActiveModel> =
        sources.into_iter().map(|src| src.into()).collect();
    Source::insert_many(source_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("sources migrated");

    let mut object_dependencies = vec![];

    // table
    let table_models: Vec<table::ActiveModel> = tables
        .into_iter()
        .map(|t| {
            object_dependencies.extend(t.dependent_relations.iter().map(|id| {
                object_dependency::ActiveModel {
                    id: NotSet,
                    oid: Set(*id as _),
                    used_by: Set(t.id as _),
                }
            }));
            t.into()
        })
        .collect_vec();
    Table::insert_many(table_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("tables migrated");

    // index
    let index_models: Vec<index::ActiveModel> = indexes.into_iter().map(|i| i.into()).collect_vec();
    Index::insert_many(index_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("indexes migrated");

    // sink
    let sink_models: Vec<sink::ActiveModel> = sinks
        .into_iter()
        .map(|s| {
            object_dependencies.extend(s.dependent_relations.iter().map(|id| {
                object_dependency::ActiveModel {
                    id: NotSet,
                    oid: Set(*id as _),
                    used_by: Set(s.id as _),
                }
            }));
            s.into()
        })
        .collect();
    Sink::insert_many(sink_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("sinks migrated");

    // view
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
    println!("views migrated");

    // subscriptions
    let subscription_models: Vec<subscription::ActiveModel> = subscriptions
        .into_iter()
        .map(|s| {
            object_dependencies.extend(s.dependent_relations.iter().map(|id| {
                object_dependency::ActiveModel {
                    id: NotSet,
                    oid: Set(*id as _),
                    used_by: Set(s.id as _),
                }
            }));
            s.into()
        })
        .collect();
    Subscription::insert_many(subscription_models)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("subscriptions migrated");

    // object_dependency
    ObjectDependency::insert_many(object_dependencies)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("object dependencies migrated");

    // user privilege
    let mut privileges = vec![];
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
                ty @ _ => unreachable!("invalid object type: {:?}", ty),
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
    UserPrivilege::insert_many(privileges)
        .exec(&meta_store_sql.conn)
        .await?;
    println!("user privileges migrated");

    // notification.

    // hummock.

    // Rest sequence for all tables.

    Ok(())
}
