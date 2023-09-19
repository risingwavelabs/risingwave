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

pub mod information_schema;
pub mod pg_catalog;
pub mod rw_catalog;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, Field, SysCatalogReader, TableDesc, TableId, DEFAULT_SUPER_USER_ID,
    NON_RESERVED_SYS_CATALOG_ID,
};
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_pb::user::UserInfo;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::system_catalog::information_schema::*;
use crate::catalog::system_catalog::pg_catalog::*;
use crate::catalog::system_catalog::rw_catalog::*;
use crate::catalog::view_catalog::ViewCatalog;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::session::AuthContext;
use crate::user::user_privilege::available_prost_privilege;
use crate::user::user_service::UserInfoReader;
use crate::user::UserId;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemTableCatalog {
    pub id: TableId,

    pub name: String,

    // All columns in this table.
    pub columns: Vec<ColumnCatalog>,

    /// Primary key columns indices.
    pub pk: Vec<usize>,

    // owner of table, should always be default super user, keep it for compatibility.
    pub owner: u32,
}

impl SystemTableCatalog {
    /// Get a reference to the system catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn with_id(mut self, id: TableId) -> Self {
        self.id = id;
        self
    }

    /// Get a reference to the system catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a [`TableDesc`] of the system table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            stream_key: self.pk.clone(),
            ..Default::default()
        }
    }

    /// Get a reference to the system catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

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

pub struct BuiltinTable {
    name: &'static str,
    schema: &'static str,
    columns: &'static [SystemCatalogColumnsDef<'static>],
    pk: &'static [usize],
}

pub struct BuiltinView {
    name: &'static str,
    schema: &'static str,
    columns: &'static [SystemCatalogColumnsDef<'static>],
    sql: String,
}

pub enum BuiltinCatalog {
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
}

impl BuiltinCatalog {
    fn name(&self) -> &'static str {
        match self {
            BuiltinCatalog::Table(t) => t.name,
            BuiltinCatalog::View(v) => v.name,
        }
    }
}

impl From<&BuiltinTable> for SystemTableCatalog {
    fn from(val: &BuiltinTable) -> Self {
        SystemTableCatalog {
            id: TableId::placeholder(),
            name: val.name.to_string(),
            columns: val
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| ColumnCatalog {
                    column_desc: ColumnDesc::new_atomic(c.0.clone(), c.1, idx as i32),
                    is_hidden: false,
                })
                .collect(),
            pk: val.pk.to_vec(),
            owner: DEFAULT_SUPER_USER_ID,
        }
    }
}

impl From<&BuiltinView> for ViewCatalog {
    fn from(val: &BuiltinView) -> Self {
        ViewCatalog {
            id: 0,
            name: val.name.to_string(),
            columns: val
                .columns
                .iter()
                .map(|c| Field::with_name(c.0.clone(), c.1.to_string()))
                .collect(),
            sql: val.sql.to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            properties: Default::default(),
        }
    }
}

// TODO: support struct column and type name when necessary.
pub(super) type SystemCatalogColumnsDef<'a> = (DataType, &'a str);

/// `infer_dummy_view_sql` returns a dummy SQL statement for a view with the given columns that
/// returns no rows. For example, with columns `a` and `b`, it returns `SELECT NULL::integer AS a,
/// NULL::varchar AS b WHERE 1 != 1`.
// FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
#[inline(always)]
fn infer_dummy_view_sql(columns: &[SystemCatalogColumnsDef<'_>]) -> String {
    format!(
        "SELECT {} WHERE 1 != 1",
        columns
            .iter()
            .map(|(ty, name)| format!("NULL::{} AS {}", ty, name))
            .join(", ")
    )
}

/// get acl items of `object` in string, ignore public.
fn get_acl_items(
    object: &Object,
    for_dml_table: bool,
    users: &Vec<UserInfo>,
    username_map: &HashMap<UserId, String>,
) -> String {
    let mut res = String::from("{");
    let mut empty_flag = true;
    let super_privilege = available_prost_privilege(object.clone(), for_dml_table);
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

pub struct SystemCatalog {
    table_by_schema_name: HashMap<&'static str, Vec<Arc<SystemTableCatalog>>>,
    table_name_by_id: HashMap<TableId, &'static str>,
    view_by_schema_name: HashMap<&'static str, Vec<Arc<ViewCatalog>>>,
}

pub fn get_sys_tables_in_schema(schema_name: &str) -> Option<Vec<Arc<SystemTableCatalog>>> {
    SYS_CATALOGS
        .table_by_schema_name
        .get(schema_name)
        .map(Clone::clone)
}

pub fn get_sys_views_in_schema(schema_name: &str) -> Option<Vec<Arc<ViewCatalog>>> {
    SYS_CATALOGS
        .view_by_schema_name
        .get(schema_name)
        .map(Clone::clone)
}

macro_rules! prepare_sys_catalog {
    ($( { $builtin_catalog:expr $(, $func:ident $($await:ident)?)? } ),* $(,)?) => {
        pub(crate) static SYS_CATALOGS: LazyLock<SystemCatalog> = LazyLock::new(|| {
            let mut table_by_schema_name = HashMap::new();
            let mut table_name_by_id = HashMap::new();
            let mut view_by_schema_name = HashMap::new();
            $(
                let id = (${index()} + 1) as u32;
                match $builtin_catalog {
                    BuiltinCatalog::Table(table) => {
                        let sys_table: SystemTableCatalog = table.into();
                        table_by_schema_name.entry(table.schema).or_insert(vec![]).push(Arc::new(sys_table.with_id(id.into())));
                        table_name_by_id.insert(id.into(), table.name);
                    },
                    BuiltinCatalog::View(view) => {
                        let sys_view: ViewCatalog = view.into();
                        view_by_schema_name.entry(view.schema).or_insert(vec![]).push(Arc::new(sys_view.with_id(id)));
                    },
                }
            )*
            assert!(table_name_by_id.len() < NON_RESERVED_SYS_CATALOG_ID as usize, "too many system catalogs");

            SystemCatalog {
                table_by_schema_name,
                table_name_by_id,
                view_by_schema_name,
            }
        });

        #[async_trait]
        impl SysCatalogReader for SysCatalogReaderImpl {
            async fn read_table(&self, table_id: &TableId) -> Result<Vec<OwnedRow>> {
                let table_name = SYS_CATALOGS.table_name_by_id.get(table_id).unwrap();
                $(
                    if $builtin_catalog.name() == *table_name {
                        $(
                            let rows = self.$func();
                            $(let rows = rows.$await;)?
                            return rows;
                        )?
                    }
                )*
                unreachable!()
            }
        }
    }
}

// `prepare_sys_catalog!` macro is used to generate all builtin system catalogs.
prepare_sys_catalog! {
    { BuiltinCatalog::View(&PG_TYPE) },
    { BuiltinCatalog::View(&PG_NAMESPACE) },
    { BuiltinCatalog::View(&PG_CAST) },
    { BuiltinCatalog::View(&PG_MATVIEWS) },
    { BuiltinCatalog::View(&PG_USER) },
    { BuiltinCatalog::View(&PG_CLASS) },
    { BuiltinCatalog::View(&PG_INDEX) },
    { BuiltinCatalog::View(&PG_OPCLASS) },
    { BuiltinCatalog::View(&PG_COLLATION) },
    { BuiltinCatalog::View(&PG_AM) },
    { BuiltinCatalog::View(&PG_OPERATOR) },
    { BuiltinCatalog::View(&PG_VIEWS) },
    { BuiltinCatalog::View(&PG_ATTRIBUTE) },
    { BuiltinCatalog::View(&PG_DATABASE) },
    { BuiltinCatalog::View(&PG_DESCRIPTION) },
    { BuiltinCatalog::View(&PG_SETTINGS) },
    { BuiltinCatalog::View(&PG_KEYWORDS) },
    { BuiltinCatalog::View(&PG_ATTRDEF) },
    { BuiltinCatalog::View(&PG_ROLES) },
    { BuiltinCatalog::View(&PG_SHDESCRIPTION) },
    { BuiltinCatalog::View(&PG_TABLESPACE) },
    { BuiltinCatalog::View(&PG_STAT_ACTIVITY) },
    { BuiltinCatalog::View(&PG_ENUM) },
    { BuiltinCatalog::View(&PG_CONVERSION) },
    { BuiltinCatalog::View(&PG_INDEXES) },
    { BuiltinCatalog::View(&PG_INHERITS) },
    { BuiltinCatalog::View(&PG_CONSTRAINT) },
    { BuiltinCatalog::View(&PG_TABLES) },
    { BuiltinCatalog::View(&PG_PROC) },
    { BuiltinCatalog::View(&PG_SHADOW) },
    { BuiltinCatalog::View(&PG_LOCKS) },
    { BuiltinCatalog::View(&PG_EXTENSION) },
    { BuiltinCatalog::View(&PG_DEPEND) },
    { BuiltinCatalog::View(&INFORMATION_SCHEMA_COLUMNS) },
    { BuiltinCatalog::View(&INFORMATION_SCHEMA_TABLES) },
    { BuiltinCatalog::View(&INFORMATION_SCHEMA_VIEWS) },
    { BuiltinCatalog::Table(&RW_DATABASES), read_rw_database_info },
    { BuiltinCatalog::Table(&RW_SCHEMAS), read_rw_schema_info },
    { BuiltinCatalog::Table(&RW_USERS), read_rw_user_info },
    { BuiltinCatalog::Table(&RW_USER_SECRETS), read_rw_user_secrets_info },
    { BuiltinCatalog::Table(&RW_TABLES), read_rw_table_info },
    { BuiltinCatalog::Table(&RW_MATERIALIZED_VIEWS), read_rw_mview_info },
    { BuiltinCatalog::Table(&RW_INDEXES), read_rw_indexes_info },
    { BuiltinCatalog::Table(&RW_SOURCES), read_rw_sources_info },
    { BuiltinCatalog::Table(&RW_SINKS), read_rw_sinks_info },
    { BuiltinCatalog::Table(&RW_CONNECTIONS), read_rw_connections_info },
    { BuiltinCatalog::Table(&RW_FUNCTIONS), read_rw_functions_info },
    { BuiltinCatalog::Table(&RW_VIEWS), read_rw_views_info },
    { BuiltinCatalog::Table(&RW_WORKER_NODES), read_rw_worker_nodes_info },
    { BuiltinCatalog::Table(&RW_PARALLEL_UNITS), read_rw_parallel_units_info },
    { BuiltinCatalog::Table(&RW_TABLE_FRAGMENTS), read_rw_table_fragments_info await },
    { BuiltinCatalog::Table(&RW_FRAGMENTS), read_rw_fragment_distributions_info await },
    { BuiltinCatalog::Table(&RW_ACTORS), read_rw_actor_states_info await },
    { BuiltinCatalog::Table(&RW_META_SNAPSHOT), read_meta_snapshot await },
    { BuiltinCatalog::Table(&RW_DDL_PROGRESS), read_ddl_progress await },
    { BuiltinCatalog::Table(&RW_TABLE_STATS), read_table_stats },
    { BuiltinCatalog::Table(&RW_RELATION_INFO), read_relation_info await },
    { BuiltinCatalog::Table(&RW_SYSTEM_TABLES), read_system_table_info },
    { BuiltinCatalog::View(&RW_RELATIONS) },
    { BuiltinCatalog::Table(&RW_COLUMNS), read_rw_columns_info },
    { BuiltinCatalog::Table(&RW_TYPES), read_rw_types },
    { BuiltinCatalog::Table(&RW_HUMMOCK_PINNED_VERSIONS), read_hummock_pinned_versions await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_PINNED_SNAPSHOTS), read_hummock_pinned_snapshots await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_CURRENT_VERSION), read_hummock_current_version await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_CHECKPOINT_VERSION), read_hummock_checkpoint_version await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_VERSION_DELTAS), read_hummock_version_deltas await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_BRANCHED_OBJECTS), read_hummock_branched_objects await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_COMPACTION_GROUP_CONFIGS), read_hummock_compaction_group_configs await },
    { BuiltinCatalog::Table(&RW_HUMMOCK_META_CONFIGS), read_hummock_meta_configs await},
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::catalog::system_catalog::SYS_CATALOGS;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_builtin_view_definition() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let sqls = SYS_CATALOGS
            .view_by_schema_name
            .values()
            .flat_map(|v| v.iter().map(|v| v.sql.clone()))
            .collect_vec();
        for sql in sqls {
            frontend.query_formatted_result(sql).await;
        }
    }
}
