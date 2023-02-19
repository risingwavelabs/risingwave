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

pub mod pg_am;
pub mod pg_attrdef;
pub mod pg_attribute;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_collation;
pub mod pg_conversion;
pub mod pg_database;
pub mod pg_description;
pub mod pg_enum;
pub mod pg_index;
pub mod pg_keywords;
pub mod pg_matviews;
pub mod pg_namespace;
pub mod pg_opclass;
pub mod pg_operator;
pub mod pg_roles;
pub mod pg_settings;
pub mod pg_shdescription;
pub mod pg_stat_activity;
pub mod pg_tablespace;
pub mod pg_type;
pub mod pg_user;
pub mod pg_views;

use std::collections::HashMap;

use itertools::Itertools;
pub use pg_am::*;
pub use pg_attrdef::*;
pub use pg_attribute::*;
pub use pg_cast::*;
pub use pg_class::*;
pub use pg_collation::*;
pub use pg_conversion::*;
pub use pg_database::*;
pub use pg_description::*;
pub use pg_enum::*;
pub use pg_index::*;
pub use pg_keywords::*;
pub use pg_matviews::*;
pub use pg_namespace::*;
pub use pg_opclass::*;
pub use pg_operator::*;
pub use pg_roles::*;
pub use pg_settings::*;
pub use pg_shdescription::*;
pub use pg_stat_activity::*;
pub use pg_tablespace::*;
pub use pg_type::*;
pub use pg_user::*;
pub use pg_views::*;
use risingwave_common::array::ListValue;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{NaiveDateTimeWrapper, ScalarImpl};
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::iter_util::ZipEqDebug;
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
    pub(super) fn read_types(&self) -> Result<Vec<OwnedRow>> {
        Ok(PG_TYPE_DATA_ROWS.clone())
    }

    pub(super) fn read_cast(&self) -> Result<Vec<OwnedRow>> {
        Ok(PG_CAST_DATA_ROWS.clone())
    }

    pub(super) fn read_namespace(&self) -> Result<Vec<OwnedRow>> {
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
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(schema.id as i32)),
                    Some(ScalarImpl::Utf8(schema.name.clone().into())),
                    Some(ScalarImpl::Int32(schema.owner as i32)),
                    Some(ScalarImpl::Utf8(
                        get_acl_items(&Object::SchemaId(schema.id), &users, username_map).into(),
                    )),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_user_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.clone().into())),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.is_super)),
                    // compatible with PG.
                    Some(ScalarImpl::Utf8("********".into())),
                ])
            })
            .collect_vec())
    }

    pub(super) async fn read_meta_snapshot(&self) -> Result<Vec<OwnedRow>> {
        let try_get_date_time = |epoch: u64| {
            if epoch == 0 {
                return None;
            }
            let time_millis = Epoch::from(epoch).as_unix_millis();
            NaiveDateTimeWrapper::with_secs_nsecs(
                (time_millis / 1000) as i64,
                (time_millis % 1000 * 1_000_000) as u32,
            )
            .map(ScalarImpl::NaiveDateTime)
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

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_opclass_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_operator_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_am_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    // FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
    pub(super) fn read_collation_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    pub(super) fn read_attrdef_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    pub(crate) fn read_shdescription_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    pub(crate) fn read_enum_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    pub(super) fn read_roles_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.user_info_reader.read_guard();
        let users = reader.get_all_users();
        Ok(users
            .iter()
            .map(|user| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(user.id as i32)),
                    Some(ScalarImpl::Utf8(user.name.clone().into())),
                    Some(ScalarImpl::Bool(user.is_super)),
                    Some(ScalarImpl::Bool(true)),
                    Some(ScalarImpl::Bool(user.can_create_user)),
                    Some(ScalarImpl::Bool(user.can_create_db)),
                    Some(ScalarImpl::Bool(user.can_login)),
                    Some(ScalarImpl::Utf8("********".into())),
                ])
            })
            .collect_vec())
    }

    pub(super) fn read_class_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let schema_infos = reader.get_all_schema_info(&self.auth_context.database)?;

        Ok(schemas
            .zip_eq_debug(schema_infos.iter())
            .flat_map(|(schema, schema_info)| {
                // !!! If we need to add more class types, remember to update
                // Catalog::get_id_by_class_name_inner accordingly.

                let rows = schema
                    .iter_table()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("r".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let mvs = schema
                    .iter_mv()
                    .map(|mv| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(mv.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(mv.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(mv.owner as i32)),
                            Some(ScalarImpl::Utf8("m".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let indexes = schema
                    .iter_index()
                    .map(|index| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(index.index_table.id.table_id as i32)),
                            Some(ScalarImpl::Utf8(index.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(index.index_table.owner as i32)),
                            Some(ScalarImpl::Utf8("i".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let sources = schema
                    .iter_source()
                    .map(|source| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(source.id as i32)),
                            Some(ScalarImpl::Utf8(source.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(source.owner as i32)),
                            Some(ScalarImpl::Utf8("x".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let sys_tables = schema
                    .iter_system_tables()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("r".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let views = schema
                    .iter_view()
                    .map(|view| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(view.name().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(view.owner as i32)),
                            Some(ScalarImpl::Utf8("v".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
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

    pub(super) fn read_index_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_index().map(|index| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(index.id.index_id() as i32)),
                        Some(ScalarImpl::Int32(index.primary_table.id.table_id() as i32)),
                        Some(ScalarImpl::Int16(index.original_columns.len() as i16)),
                        Some(ScalarImpl::List(ListValue::new(
                            index
                                .original_columns
                                .iter()
                                .map(|index| Some(ScalarImpl::Int16(index.get_id() as i16 + 1)))
                                .collect_vec(),
                        ))),
                        None,
                        None,
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) async fn read_mviews_info(&self) -> Result<Vec<OwnedRow>> {
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
                        rows.push(OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(schema.clone().into())),
                            Some(ScalarImpl::Utf8(t.name.clone().into())),
                            Some(ScalarImpl::Int32(t.owner as i32)),
                            Some(ScalarImpl::Utf8(t.definition.clone().into())),
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
        }

        Ok(rows)
    }

    pub(super) fn read_views_info(&self) -> Result<Vec<OwnedRow>> {
        // TODO(zehua): solve the deadlock problem.
        // Get two read locks. The order must be the same as
        // `FrontendObserverNode::handle_initialization_notification`.
        let catalog_reader = self.catalog_reader.read_guard();
        let user_info_reader = self.user_info_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_view().map(|view| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(view.name().into())),
                        Some(ScalarImpl::Utf8(
                            user_info_reader
                                .get_user_name_by_id(view.owner)
                                .unwrap()
                                .into(),
                        )),
                        // TODO(zehua): may be not same as postgresql's "definition" column.
                        Some(ScalarImpl::Utf8(view.sql.clone().into())),
                    ])
                })
            })
            .collect_vec())
    }

    pub(super) fn read_pg_attribute(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let view_rows = schema.iter_view().flat_map(|view| {
                    view.columns.iter().enumerate().map(|(index, column)| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(column.name.clone().into())),
                            Some(ScalarImpl::Int32(column.data_type().to_oid())),
                            Some(ScalarImpl::Int16(column.data_type().type_len())),
                            Some(ScalarImpl::Int16(index as i16 + 1)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Bool(false)),
                        ])
                    })
                });

                schema
                    .iter_valid_table()
                    .flat_map(|table| {
                        table
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| !column.is_hidden())
                            .map(|(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    Some(ScalarImpl::Int32(column.data_type().to_oid())),
                                    Some(ScalarImpl::Int16(column.data_type().type_len())),
                                    Some(ScalarImpl::Int16(index as i16 + 1)),
                                    Some(ScalarImpl::Bool(false)),
                                    Some(ScalarImpl::Bool(false)),
                                ])
                            })
                    })
                    .chain(view_rows)
            })
            .collect_vec())
    }

    pub(super) fn read_database_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let databases = reader.get_all_database_names();

        Ok(databases
            .iter()
            .map(|db| new_pg_database_row(reader.get_database_by_name(db).unwrap().id(), db))
            .collect_vec())
    }

    pub(super) fn read_description_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let rows = schema
                    .iter_table()
                    .map(|table| new_pg_description_row(table.id().table_id))
                    .collect_vec();

                let mvs = schema
                    .iter_mv()
                    .map(|mv| new_pg_description_row(mv.id().table_id))
                    .collect_vec();

                let indexes = schema
                    .iter_index()
                    .map(|index| new_pg_description_row(index.id.index_id()))
                    .collect_vec();

                let sources = schema
                    .iter_source()
                    .map(|source| new_pg_description_row(source.id))
                    .collect_vec();

                let sys_tables = schema
                    .iter_system_tables()
                    .map(|table| new_pg_description_row(table.id().table_id))
                    .collect_vec();

                let views = schema
                    .iter_view()
                    .map(|view| new_pg_description_row(view.id))
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

    pub(super) fn read_settings_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(PG_SETTINGS_DATA_ROWS.clone())
    }

    pub(super) fn read_keywords_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(PG_KEYWORDS_DATA_ROWS.clone())
    }

    pub(super) fn read_tablespace_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(PG_TABLESPACE_DATA_ROWS.clone())
    }

    pub(crate) fn read_conversion_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }

    pub(super) fn read_stat_activity(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }
}
