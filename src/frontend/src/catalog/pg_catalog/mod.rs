// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod pg_cast;
pub mod pg_class;
pub mod pg_matviews_info;
pub mod pg_namespace;
pub mod pg_type;
pub mod pg_user;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, SysCatalogReader, TableId, DEFAULT_SUPER_USER_ID};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_pb::user::UserInfo;
use serde_json::json;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::pg_catalog::pg_cast::*;
use crate::catalog::pg_catalog::pg_class::*;
use crate::catalog::pg_catalog::pg_matviews_info::*;
use crate::catalog::pg_catalog::pg_namespace::*;
use crate::catalog::pg_catalog::pg_type::*;
use crate::catalog::pg_catalog::pg_user::*;
use crate::catalog::system_catalog::SystemCatalog;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::session::AuthContext;
use crate::user::user_privilege::available_prost_privilege;
use crate::user::user_service::UserInfoReader;
use crate::user::UserId;

#[expect(dead_code)]
pub struct SysCatalogReaderImpl {
    // Read catalog info: database/schema/source/table.
    catalog_reader: CatalogReader,
    // Read user info.
    user_info_reader: UserInfoReader,
    // Read cluster info.
    worker_node_manager: WorkerNodeManagerRef,
    // Read from meta.
    meta_client: Arc<dyn FrontendMetaClient>,
    auth_context: Arc<AuthContext>,
}

impl SysCatalogReaderImpl {
    pub fn new(
        catalog_reader: CatalogReader,
        user_info_reader: UserInfoReader,
        worker_node_manager: WorkerNodeManagerRef,
        meta_client: Arc<dyn FrontendMetaClient>,
        auth_context: Arc<AuthContext>,
    ) -> Self {
        Self {
            catalog_reader,
            user_info_reader,
            worker_node_manager,
            meta_client,
            auth_context,
        }
    }
}

#[async_trait]
impl SysCatalogReader for SysCatalogReaderImpl {
    async fn read_table(&self, table_name: &str) -> Result<Vec<Row>> {
        match table_name {
            PG_TYPE_TABLE_NAME => Ok(PG_TYPE_DATA_ROWS.clone()),
            PG_CAST_TABLE_NAME => Ok(PG_CAST_DATA_ROWS.clone()),
            PG_NAMESPACE_TABLE_NAME => self.read_namespace(),
            PG_MATVIEWS_INFO_TABLE_NAME => self.read_mviews_info().await,
            PG_USER_TABLE_NAME => self.read_user_info(),
            PG_CLASS_TABLE_NAME => self.read_class_info(),
            _ => {
                Err(ErrorCode::ItemNotFound(format!("Invalid system table: {}", table_name)).into())
            }
        }
    }
}

/// get acl items of `object` in string, ignore public.
fn get_acl_items(
    object: &Object,
    users: &Vec<UserInfo>,
    username_map: &HashMap<UserId, String>,
) -> String {
    let mut res = String::from("{");
    let mut empty_flag = true;
    let super_privilege = available_prost_privilege(object.clone());
    for user in users {
        let privileges = if user.get_is_super() {
            vec![&super_privilege]
        } else {
            user.get_grant_privileges()
                .iter()
                .filter(|&privilege| privilege.object.as_ref().unwrap() == object)
                .collect_vec()
        };
        if privileges.is_empty() {
            continue;
        };
        let mut grantor_map = HashMap::new();
        privileges.iter().for_each(|&privilege| {
            privilege.action_with_opts.iter().for_each(|ao| {
                grantor_map.entry(ao.granted_by).or_insert_with(Vec::new);
                grantor_map
                    .get_mut(&ao.granted_by)
                    .unwrap()
                    .push((ao.action, ao.with_grant_option));
            })
        });
        for key in grantor_map.keys() {
            if empty_flag {
                empty_flag = false;
            } else {
                res.push(',');
            }
            res.push_str(user.get_name());
            res.push('=');
            grantor_map
                .get(key)
                .unwrap()
                .iter()
                .for_each(|(action, option)| {
                    let str = match Action::from_i32(*action).unwrap() {
                        Action::Select => "r",
                        Action::Insert => "a",
                        Action::Update => "w",
                        Action::Delete => "d",
                        Action::Create => "C",
                        Action::Connect => "c",
                        _ => unreachable!(),
                    };
                    res.push_str(str);
                    if *option {
                        res.push('*');
                    }
                });
            res.push('/');
            // should be able to query grantor's name
            res.push_str(username_map.get(key).as_ref().unwrap());
        }
    }
    res.push('}');
    res
}
impl SysCatalogReaderImpl {
    fn read_namespace(&self) -> Result<Vec<Row>> {
        let schemas = self
            .catalog_reader
            .read_guard()
            .get_all_schema_info(&self.auth_context.database)?;
        let user_reader = self.user_info_reader.read_guard();
        let users = user_reader.get_all_users();
        let username_map = user_reader.get_user_name_map();
        Ok(schemas
            .iter()
            .map(|schema| {
                Row::new(vec![
                    Some(ScalarImpl::Int32(schema.id as i32)),
                    Some(ScalarImpl::Utf8(schema.name.clone())),
                    Some(ScalarImpl::Int32(schema.owner as i32)),
                    Some(ScalarImpl::Utf8(get_acl_items(
                        &Object::SchemaId(schema.id),
                        &users,
                        username_map,
                    ))),
                ])
            })
            .collect_vec())
    }

    fn read_user_info(&self) -> Result<Vec<Row>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                Row::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.clone())),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.is_super)),
                    // compatible with PG.
                    Some(ScalarImpl::Utf8("********".to_string())),
                ])
            })
            .collect_vec())
    }

    fn read_class_info(&self) -> Result<Vec<Row>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let schema_infos = reader.get_all_schema_info(&self.auth_context.database)?;

        Ok(schemas
            .zip_eq(schema_infos.iter())
            .flat_map(|(schema, schema_info)| {
                let rows = schema
                    .iter_table()
                    .map(|table| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("table".to_string())),
                        ])
                    })
                    .collect_vec();

                let mvs = schema
                    .iter_mv()
                    .map(|mv| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(mv.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(mv.name.clone())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(mv.owner as i32)),
                            Some(ScalarImpl::Utf8("materialized view".to_string())),
                        ])
                    })
                    .collect_vec();

                let indexes = schema
                    .iter_index()
                    .map(|index| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(index.index_table.id.table_id as i32)),
                            Some(ScalarImpl::Utf8(index.name.clone())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(index.index_table.owner as i32)),
                            Some(ScalarImpl::Utf8("index".to_string())),
                        ])
                    })
                    .collect_vec();

                let sources = schema
                    .iter_source()
                    .map(|source| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(source.id as i32)),
                            Some(ScalarImpl::Utf8(source.name.clone())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(source.owner as i32)),
                            Some(ScalarImpl::Utf8("source".to_string())),
                        ])
                    })
                    .collect_vec();

                let sys_tables = schema
                    .iter_system_tables()
                    .map(|table| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("system table".to_string())),
                        ])
                    })
                    .collect_vec();

                rows.into_iter()
                    .chain(mvs.into_iter())
                    .chain(indexes.into_iter())
                    .chain(sources.into_iter())
                    .chain(sys_tables.into_iter())
                    .collect_vec()
            })
            .collect_vec())
    }

    async fn read_mviews_info(&self) -> Result<Vec<Row>> {
        let mut table_ids = Vec::new();
        {
            let reader = self.catalog_reader.read_guard();
            let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
            for schema in &schemas {
                reader
                    .get_schema_by_name(&self.auth_context.database, schema)?
                    .iter_mv()
                    .for_each(|t| {
                        table_ids.push(t.id.table_id);
                    });
            }
        }

        let table_fragments = self.meta_client.list_table_fragments(&table_ids).await?;
        let mut rows = Vec::new();
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
        for schema in &schemas {
            reader
                .get_schema_by_name(&self.auth_context.database, schema)?
                .iter_mv()
                .for_each(|t| {
                    if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                        rows.push(Row::new(vec![
                            Some(ScalarImpl::Int32(t.id.table_id as i32)),
                            Some(ScalarImpl::Utf8(t.name.clone())),
                            Some(ScalarImpl::Utf8(schema.clone())),
                            Some(ScalarImpl::Int32(t.owner as i32)),
                            Some(ScalarImpl::Utf8(json!(fragments).to_string())),
                        ]));
                    }
                });
        }

        Ok(rows)
    }
}

// TODO: support struct column and type name when necessary.
type PgCatalogColumnsDef<'a> = (DataType, &'a str);

/// `def_sys_catalog` defines a table with given id, name and columns.
macro_rules! def_sys_catalog {
    ($id:expr, $name:ident, $columns:expr) => {
        SystemCatalog {
            id: TableId::new($id),
            name: $name.to_string(),
            columns: $columns
                .iter()
                .enumerate()
                .map(|(idx, col)| ColumnCatalog {
                    column_desc: ColumnDesc {
                        column_id: (idx as i32).into(),
                        data_type: col.0.clone(),
                        name: col.1.to_string(),
                        field_descs: vec![],
                        type_name: "".to_string(),
                    },
                    is_hidden: false,
                })
                .collect::<Vec<_>>(),
            pk: vec![0], // change this when multi-column pk is needed in some system table.
            owner: DEFAULT_SUPER_USER_ID,
        }
    };
}

lazy_static::lazy_static! {
    /// `PG_CATALOG_MAP` includes all system catalogs. If you added a new system catalog, be
    /// sure to add a corresponding entry here.
    pub(crate) static ref PG_CATALOG_MAP: HashMap<String, SystemCatalog> =
        [
            (PG_TYPE_TABLE_NAME.to_string(), def_sys_catalog!(1, PG_TYPE_TABLE_NAME, PG_TYPE_COLUMNS)),
            (PG_NAMESPACE_TABLE_NAME.to_string(), def_sys_catalog!(2, PG_NAMESPACE_TABLE_NAME, PG_NAMESPACE_COLUMNS)),
            (PG_CAST_TABLE_NAME.to_string(), def_sys_catalog!(3, PG_CAST_TABLE_NAME, PG_CAST_COLUMNS)),
            (PG_MATVIEWS_INFO_TABLE_NAME.to_string(), def_sys_catalog!(4, PG_MATVIEWS_INFO_TABLE_NAME, PG_MATVIEWS_INFO_COLUMNS)),
            (PG_USER_TABLE_NAME.to_string(), def_sys_catalog!(5, PG_USER_TABLE_NAME, PG_USER_COLUMNS)),
            (PG_CLASS_TABLE_NAME.to_string(), def_sys_catalog!(6, PG_CLASS_TABLE_NAME, PG_CLASS_COLUMNS))
        ].into();
}

pub fn get_all_pg_catalogs() -> Vec<SystemCatalog> {
    PG_CATALOG_MAP.values().cloned().collect()
}
