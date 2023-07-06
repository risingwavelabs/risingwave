// Copyright 2023 RisingWave Labs
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

mod rw_actors;
mod rw_connections;
mod rw_databases;
mod rw_ddl_progress;
mod rw_fragments;
mod rw_functions;
mod rw_indexes;
mod rw_materialized_views;
mod rw_meta_snapshot;
mod rw_parallel_units;
mod rw_relation_info;
mod rw_schemas;
mod rw_sinks;
mod rw_sources;
mod rw_table_fragments;
mod rw_table_stats;
mod rw_tables;
mod rw_users;
mod rw_views;
mod rw_worker_nodes;

use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{ScalarImpl, Timestamp};
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::user::grant_privilege::Object;
pub use rw_actors::*;
pub use rw_connections::*;
pub use rw_databases::*;
pub use rw_ddl_progress::*;
pub use rw_fragments::*;
pub use rw_functions::*;
pub use rw_indexes::*;
pub use rw_materialized_views::*;
pub use rw_meta_snapshot::*;
pub use rw_parallel_units::*;
pub use rw_relation_info::*;
pub use rw_schemas::*;
pub use rw_sinks::*;
pub use rw_sources::*;
pub use rw_table_fragments::*;
pub use rw_table_stats::*;
pub use rw_tables::*;
pub use rw_users::*;
pub use rw_views::*;
pub use rw_worker_nodes::*;
use serde_json::json;

use super::SysCatalogReaderImpl;
use crate::catalog::system_catalog::get_acl_items;
use crate::handler::create_source::UPSTREAM_SOURCE_KEY;

impl SysCatalogReaderImpl {
    pub(super) async fn read_meta_snapshot(&self) -> Result<Vec<OwnedRow>> {
        let try_get_date_time = |epoch: u64| {
            if epoch == 0 {
                return None;
            }
            let time_millis = Epoch::from(epoch).as_unix_millis();
            Timestamp::with_secs_nsecs(
                (time_millis / 1000) as i64,
                (time_millis % 1000 * 1_000_000) as u32,
            )
            .map(ScalarImpl::Timestamp)
            .ok()
        };
        let meta_snapshots = self
            .meta_client
            .list_meta_snapshots()
            .await?
            .into_iter()
            .map(|s| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(s.id as i64)),
                    Some(ScalarImpl::Int64(s.hummock_version_id as i64)),
                    Some(ScalarImpl::Int64(s.safe_epoch as i64)),
                    try_get_date_time(s.safe_epoch),
                    Some(ScalarImpl::Int64(s.max_committed_epoch as i64)),
                    try_get_date_time(s.max_committed_epoch),
                ])
            })
            .collect_vec();
        Ok(meta_snapshots)
    }

    pub(super) async fn read_ddl_progress(&self) -> Result<Vec<OwnedRow>> {
        let ddl_grogress = self
            .meta_client
            .list_ddl_progress()
            .await?
            .into_iter()
            .map(|s| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(s.id as i64)),
                    Some(ScalarImpl::Utf8(s.statement.into())),
                    Some(ScalarImpl::Utf8(s.progress.into())),
                ])
            })
            .collect_vec();
        Ok(ddl_grogress)
    }

    pub(super) async fn read_relation_info(&self) -> Result<Vec<OwnedRow>> {
        let mut table_ids = Vec::new();
        {
            let reader = self.catalog_reader.read_guard();
            let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
            for schema in &schemas {
                let schema_catalog =
                    reader.get_schema_by_name(&self.auth_context.database, schema)?;

                schema_catalog.iter_mv().for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

                schema_catalog.iter_table().for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

                schema_catalog.iter_sink().for_each(|t| {
                    table_ids.push(t.id.sink_id);
                });

                schema_catalog.iter_index().for_each(|t| {
                    table_ids.push(t.index_table.id.table_id);
                });
            }
        }

        let table_fragments = self.meta_client.list_table_fragments(&table_ids).await?;
        let mut rows = Vec::new();
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
        for schema in &schemas {
            let schema_catalog = reader.get_schema_by_name(&self.auth_context.database, schema)?;
            schema_catalog.iter_mv().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("MATERIALIZED VIEW".into())),
                        Some(ScalarImpl::Int32(t.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                    ]));
                }
            });

            schema_catalog.iter_table().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("TABLE".into())),
                        Some(ScalarImpl::Int32(t.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                    ]));
                }
            });

            schema_catalog.iter_sink().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.sink_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner.user_id as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("SINK".into())),
                        Some(ScalarImpl::Int32(t.id.sink_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                    ]));
                }
            });

            schema_catalog.iter_index().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.index_table.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.index_table.owner as i32)),
                        Some(ScalarImpl::Utf8(t.index_table.definition.clone().into())),
                        Some(ScalarImpl::Utf8("INDEX".into())),
                        Some(ScalarImpl::Int32(t.index_table.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                    ]));
                }
            });

            // Sources have no fragments.
            schema_catalog.iter_source().for_each(|t| {
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(schema.clone().into())),
                    Some(ScalarImpl::Utf8(t.name.clone().into())),
                    Some(ScalarImpl::Int32(t.owner as i32)),
                    Some(ScalarImpl::Utf8(t.definition.clone().into())),
                    Some(ScalarImpl::Utf8("SOURCE".into())),
                    Some(ScalarImpl::Int32(t.id as i32)),
                    Some(ScalarImpl::Utf8("".into())),
                    None,
                ]));
            });
        }

        Ok(rows)
    }

    pub(super) fn read_rw_database_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(reader
            .iter_databases()
            .map(|db| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(db.id() as i32)),
                    Some(ScalarImpl::Utf8(db.name().into())),
                    Some(ScalarImpl::Int32(db.owner() as i32)),
                    Some(ScalarImpl::Utf8(
                        get_acl_items(&Object::DatabaseId(db.id()), &users, username_map).into(),
                    )),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_rw_schema_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .map(|schema| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(schema.id() as i32)),
                    Some(ScalarImpl::Utf8(schema.name().into())),
                    Some(ScalarImpl::Int32(schema.owner() as i32)),
                    Some(ScalarImpl::Utf8(
                        get_acl_items(&Object::SchemaId(schema.id()), &users, username_map).into(),
                    )),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_rw_user_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();

        Ok(users
            .into_iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.into())),
                    Some(ScalarImpl::Bool(user.is_super)),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.can_create_user)),
                    Some(ScalarImpl::Bool(user.can_login)),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_rw_table_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_table().map(|table| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(table.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(table.name().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(table.owner as i32)),
                        Some(ScalarImpl::Utf8(table.create_sql().into())),
                        Some(ScalarImpl::Utf8(
                            get_acl_items(
                                &Object::TableId(table.id.table_id),
                                &users,
                                username_map,
                            )
                            .into(),
                        )),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_mview_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_mv().map(|table| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(table.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(table.name().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(table.owner as i32)),
                        Some(ScalarImpl::Utf8(table.create_sql().into())),
                        Some(ScalarImpl::Utf8(
                            get_acl_items(
                                &Object::TableId(table.id.table_id),
                                &users,
                                username_map,
                            )
                            .into(),
                        )),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_indexes_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_index().map(|index| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(index.id.index_id as i32)),
                        Some(ScalarImpl::Utf8(index.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(index.index_table.owner as i32)),
                        Some(ScalarImpl::Utf8(index.index_table.create_sql().into())),
                        Some(ScalarImpl::Utf8("".into())),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_views_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_view().map(|view| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(view.id as i32)),
                        Some(ScalarImpl::Utf8(view.name().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(view.owner as i32)),
                        Some(ScalarImpl::Utf8(view.create_sql().into())),
                        Some(ScalarImpl::Utf8(
                            get_acl_items(&Object::ViewId(view.id), &users, username_map).into(),
                        )),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_sources_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema
                    .iter_source()
                    .filter(|s| s.associated_table_id.is_none())
                    .map(|source| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(source.id as i32)),
                            Some(ScalarImpl::Utf8(source.name.clone().into())),
                            Some(ScalarImpl::Int32(schema.id() as i32)),
                            Some(ScalarImpl::Int32(source.owner as i32)),
                            Some(ScalarImpl::Utf8(
                                source
                                    .properties
                                    .get(UPSTREAM_SOURCE_KEY)
                                    .cloned()
                                    .unwrap_or("".to_string())
                                    .to_uppercase()
                                    .into(),
                            )),
                            Some(ScalarImpl::List(ListValue::new(
                                source
                                    .columns
                                    .iter()
                                    .map(|c| Some(ScalarImpl::Utf8(c.name().into())))
                                    .collect_vec(),
                            ))),
                            Some(ScalarImpl::Utf8(
                                source.info.get_row_format().unwrap().as_str_name().into(),
                            )),
                            Some(ScalarImpl::Bool(source.append_only)),
                            source.connection_id.map(|id| ScalarImpl::Int32(id as i32)),
                            Some(ScalarImpl::Utf8(source.create_sql().into())),
                            Some(
                                get_acl_items(&Object::SourceId(source.id), &users, username_map)
                                    .into(),
                            ),
                        ])
                    })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_sinks_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_sink().map(|sink| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(sink.id.sink_id as i32)),
                        Some(ScalarImpl::Utf8(sink.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(sink.owner.user_id as i32)),
                        Some(ScalarImpl::Utf8(
                            sink.properties
                                .get(UPSTREAM_SOURCE_KEY)
                                .cloned()
                                .unwrap_or("".to_string())
                                .to_uppercase()
                                .into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            sink.sink_type.to_proto().as_str_name().into(),
                        )),
                        sink.connection_id
                            .map(|id| ScalarImpl::Int32(id.connection_id() as i32)),
                        Some(ScalarImpl::Utf8(sink.create_sql().into())),
                        Some(
                            get_acl_items(&Object::SinkId(sink.id.sink_id), &users, username_map)
                                .into(),
                        ),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_connections_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_connections().map(|conn| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(conn.id as i32)),
                        Some(ScalarImpl::Utf8(conn.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(conn.owner as i32)),
                        Some(ScalarImpl::Utf8(conn.connection_type().into())),
                        Some(ScalarImpl::Utf8(conn.provider().into())),
                        Some(ScalarImpl::Utf8("".into())),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_functions_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_function().map(|function| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(function.id.function_id() as i32)),
                        Some(ScalarImpl::Utf8(function.name.clone().into())),
                        Some(ScalarImpl::Int32(schema.id() as i32)),
                        Some(ScalarImpl::Int32(function.owner as i32)),
                        Some(ScalarImpl::Utf8(function.kind.to_string().into())),
                        Some(ScalarImpl::List(ListValue::new(
                            function
                                .arg_types
                                .iter()
                                .map(|t| Some(ScalarImpl::Int32(t.to_oid())))
                                .collect_vec(),
                        ))),
                        Some(ScalarImpl::Int32(function.return_type.to_oid())),
                        Some(ScalarImpl::Utf8(function.language.clone().into())),
                        Some(ScalarImpl::Utf8(function.link.clone().into())),
                        Some(ScalarImpl::Utf8(
                            get_acl_items(
                                &Object::FunctionId(function.id.function_id()),
                                &users,
                                username_map,
                            )
                            .into(),
                        )),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_rw_worker_nodes_info(&self) -> Result<Vec<OwnedRow>> {
        let workers = self.worker_node_manager.list_worker_nodes();

        Ok(workers
            .into_iter()
            .map(|worker| {
                let host = worker.host.as_ref().unwrap();
                let property = worker.property.as_ref().unwrap();
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(worker.id as i32)),
                    Some(ScalarImpl::Utf8(host.host.clone().into())),
                    Some(ScalarImpl::Utf8(host.port.to_string().into())),
                    Some(ScalarImpl::Utf8(
                        worker.get_type().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Utf8(
                        worker.get_state().unwrap().as_str_name().into(),
                    )),
                    Some(ScalarImpl::Int32(worker.parallel_units.len() as i32)),
                    Some(ScalarImpl::Bool(property.is_streaming)),
                    Some(ScalarImpl::Bool(property.is_serving)),
                    Some(ScalarImpl::Bool(property.is_unschedulable)),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_rw_parallel_units_info(&self) -> Result<Vec<OwnedRow>> {
        let workers = self.worker_node_manager.list_worker_nodes();

        Ok(workers
            .into_iter()
            .flat_map(|worker| {
                worker.parallel_units.into_iter().map(|unit| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(unit.id as i32)),
                        Some(ScalarImpl::Int32(unit.worker_node_id as i32)),
                    ])
                })
            })
            .collect_vec())
    }

    /// FIXME: we need to introduce revision snapshot read on meta to avoid any inconsistency when
    /// we are trying to join any table fragments related system tables.
    pub(super) async fn read_rw_table_fragments_info(&self) -> Result<Vec<OwnedRow>> {
        let states = self.meta_client.list_table_fragment_states().await?;

        Ok(states
            .into_iter()
            .map(|state| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(state.table_id as i32)),
                    Some(ScalarImpl::Utf8(state.state().as_str_name().into())),
                ])
            })
            .collect_vec())
    }

    pub(super) async fn read_rw_fragment_distributions_info(&self) -> Result<Vec<OwnedRow>> {
        let distributions = self.meta_client.list_fragment_distribution().await?;

        Ok(distributions
            .into_iter()
            .map(|distribution| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(distribution.fragment_id as i32)),
                    Some(ScalarImpl::Int32(distribution.table_id as i32)),
                    Some(ScalarImpl::Utf8(
                        distribution.distribution_type().as_str_name().into(),
                    )),
                    Some(ScalarImpl::List(ListValue::new(
                        distribution
                            .state_table_ids
                            .into_iter()
                            .map(|id| Some(ScalarImpl::Int32(id as i32)))
                            .collect_vec(),
                    ))),
                    Some(ScalarImpl::List(ListValue::new(
                        distribution
                            .upstream_fragment_ids
                            .into_iter()
                            .map(|id| Some(ScalarImpl::Int32(id as i32)))
                            .collect_vec(),
                    ))),
                ])
            })
            .collect_vec())
    }

    pub(super) async fn read_rw_actor_states_info(&self) -> Result<Vec<OwnedRow>> {
        let states = self.meta_client.list_actor_states().await?;

        Ok(states
            .into_iter()
            .map(|state| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(state.actor_id as i32)),
                    Some(ScalarImpl::Int32(state.fragment_id as i32)),
                    Some(ScalarImpl::Int32(state.parallel_unit_id as i32)),
                    Some(ScalarImpl::Utf8(state.state().as_str_name().into())),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_table_stats(&self) -> Result<Vec<OwnedRow>> {
        let catalog = self.catalog_reader.read_guard();
        let table_stats = catalog.table_stats();
        let mut rows = vec![];
        for (id, stats) in &table_stats.table_stats {
            rows.push(OwnedRow::new(vec![
                Some(ScalarImpl::Int32(*id as i32)),
                Some(ScalarImpl::Int64(stats.total_key_count)),
                Some(ScalarImpl::Int64(stats.total_key_size)),
                Some(ScalarImpl::Int64(stats.total_value_size)),
            ]));
        }
        Ok(rows)
    }
}
