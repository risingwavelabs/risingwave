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

pub mod pg_am;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_collation;
pub mod pg_index;
pub mod pg_matviews_info;
pub mod pg_namespace;
pub mod pg_opclass;
pub mod pg_operator;
pub mod pg_type;
pub mod pg_user;
pub mod pg_views;

use std::collections::HashMap;

use itertools::Itertools;
pub use pg_am::*;
pub use pg_cast::*;
pub use pg_class::*;
pub use pg_collation::*;
pub use pg_index::*;
pub use pg_matviews_info::*;
pub use pg_namespace::*;
pub use pg_opclass::*;
pub use pg_operator::*;
pub use pg_type::*;
pub use pg_user::*;
pub use pg_views::*;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::ScalarImpl;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_pb::user::UserInfo;
use serde_json::json;

use super::SysCatalogReaderImpl;
use crate::user::user_privilege::available_prost_privilege;
use crate::user::UserId;

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
    pub(super) fn read_types(&self) -> Result<Vec<Row>> {
        Ok(PG_TYPE_DATA_ROWS.clone())
    }

    pub(super) fn read_cast(&self) -> Result<Vec<Row>> {
        Ok(PG_CAST_DATA_ROWS.clone())
    }

    pub(super) fn read_namespace(&self) -> Result<Vec<Row>> {
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

    pub(super) fn read_user_info(&self) -> Result<Vec<Row>> {
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

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_opclass_info(&self) -> Result<Vec<Row>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_operator_info(&self) -> Result<Vec<Row>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_am_info(&self) -> Result<Vec<Row>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_collation_info(&self) -> Result<Vec<Row>> {
        Ok(vec![])
    }

    pub(super) fn read_class_info(&self) -> Result<Vec<Row>> {
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
                            Some(ScalarImpl::Utf8("r".to_string())),
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
                            Some(ScalarImpl::Utf8("m".to_string())),
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
                            Some(ScalarImpl::Utf8("i".to_string())),
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
                            Some(ScalarImpl::Utf8("x".to_string())),
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
                            Some(ScalarImpl::Utf8("r".to_string())),
                        ])
                    })
                    .collect_vec();

                let views = schema
                    .iter_view()
                    .map(|view| {
                        Row::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(view.name().to_string())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(view.owner as i32)),
                            Some(ScalarImpl::Utf8("v".to_string())),
                        ])
                    })
                    .collect_vec();

                rows.into_iter()
                    .chain(mvs.into_iter())
                    .chain(indexes.into_iter())
                    .chain(sources.into_iter())
                    .chain(sys_tables.into_iter())
                    .chain(views.into_iter())
                    .collect_vec()
            })
            .collect_vec())
    }

    pub(super) fn read_index_info(&self) -> Result<Vec<Row>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_index().map(|index| {
                    Row::new(vec![
                        Some(ScalarImpl::Int32(index.id.index_id() as i32)),
                        Some(ScalarImpl::Int32(index.primary_table.id.table_id() as i32)),
                        Some(ScalarImpl::Int16(index.index_item.len() as i16)),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) async fn read_mviews_info(&self) -> Result<Vec<Row>> {
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
                            Some(ScalarImpl::Utf8(t.definition.clone())),
                        ]));
                    }
                });
        }

        Ok(rows)
    }

    pub(super) fn read_views_info(&self) -> Result<Vec<Row>> {
        // TODO(zehua): solve the deadlock problem.
        // Get two read locks. The order must be the same as
        // `FrontendObserverNode::handle_initialization_notification`.
        let catalog_reader = self.catalog_reader.read_guard();
        let user_info_reader = self.user_info_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_view().map(|view| {
                    Row::new(vec![
                        Some(ScalarImpl::Utf8(schema.name())),
                        Some(ScalarImpl::Utf8(view.name().to_string())),
                        // TODO(zehua): after fix issue #6080, there must be Some.
                        user_info_reader
                            .get_user_name_by_id(view.owner)
                            .map(ScalarImpl::Utf8),
                        // TODO(zehua): may be not same as postgresql's "definition" column.
                        Some(ScalarImpl::Utf8(view.sql.clone())),
                    ])
                })
            })
            .collect_vec())
    }
}
