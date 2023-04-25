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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::iter;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{
    valid_table_name, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER,
    DEFAULT_SUPER_USER_FOR_PG, DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_ID, SYSTEM_SCHEMAS,
};
use risingwave_common::ensure;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{
    Connection, Database, Function, Index, Schema, Sink, Source, Table, View,
};
use risingwave_pb::meta::relation::RelationInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{Relation, RelationGroup};
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, PbObject};
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use tokio::sync::Mutex;

use crate::manager::catalog::reference_checker::{Refer, ReferWeak};
use crate::manager::catalog::utils::{alter_relation_rename, alter_relation_rename_refs};
use crate::manager::{
    commit_meta, DatabaseId, IdCategory, MetaSrvEnv, NotificationVersion, SchemaId, UserId,
};
use crate::model::{
    BTreeMapTransaction, MetadataModel, MetadataModelResult, Transactional, ValTransaction,
};
use crate::storage::{MetaStore, Transaction};
use crate::{MetaError, MetaResult};

// FIXME: move `GlobalId` and use other ids in common crate.
/// `GlobalId` could be identifier of relations including table, source, view, materialized view,
/// index, etc. And other objects like function, connection, etc. could also be considered as
/// `GlobalId`.
pub type GlobalId = u32;

#[derive(Clone)]
pub(crate) enum CatalogItem {
    Table(Table),
    Source(Source),
    Index(Index),
    View(View),
    Sink(Sink),
    Function(Function),
    Connection(Connection),
}

macro_rules! impl_catalog_item {
    ($({ $method:ident: $return_type:ty => $fn:ident }),*) => {
        impl CatalogItem {
            $(
                pub(crate) fn $method(&self) -> $return_type {
                    match self {
                        CatalogItem::Table(t) => t.$fn(),
                        CatalogItem::Source(s) => s.$fn(),
                        CatalogItem::Index(i) => i.$fn(),
                        CatalogItem::View(v) => v.$fn(),
                        CatalogItem::Sink(s) => s.$fn(),
                        CatalogItem::Function(f) => f.$fn(),
                        CatalogItem::Connection(c) => c.$fn(),
                    }
                }
            )*
        }
    }
}

impl_catalog_item! {
    { database_id: DatabaseId => get_database_id },
    { schema_id: SchemaId => get_schema_id },
    { id: GlobalId => get_id },
    { name: &str => get_name },
    { owner: UserId => get_owner }
}

impl CatalogItem {
    fn item_type(&self) -> &'static str {
        match self {
            CatalogItem::Table(_) => "table",
            CatalogItem::Source(_) => "source",
            CatalogItem::Index(_) => "index",
            CatalogItem::View(_) => "view",
            CatalogItem::Sink(_) => "sink",
            CatalogItem::Function(_) => "function",
            CatalogItem::Connection(_) => "connection",
        }
    }

    fn pb_object_id(&self) -> PbObject {
        match self {
            CatalogItem::Table(t) => PbObject::TableId(t.get_id()),
            CatalogItem::Source(s) => PbObject::SourceId(s.get_id()),
            CatalogItem::Index(_) => unreachable!(),
            CatalogItem::View(v) => PbObject::ViewId(v.get_id()),
            CatalogItem::Sink(s) => PbObject::SinkId(s.get_id()),
            CatalogItem::Function(f) => PbObject::FunctionId(f.get_id()),
            CatalogItem::Connection(_) => todo!(),
        }
    }

    fn is_relation(&self) -> bool {
        match self {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Index(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_) => true,
            CatalogItem::Function(_) => false,
            CatalogItem::Connection(_) => false,
        }
    }

    fn has_streaming_job(&self) -> bool {
        match self {
            CatalogItem::Table(t) => valid_table_name(&t.name),
            CatalogItem::Sink(_) => true,
            _ => false,
        }
    }

    fn set_name(&mut self, new_name: &str) {
        match self {
            CatalogItem::Table(t) => t.name = new_name.to_string(),
            CatalogItem::Source(s) => s.name = new_name.to_string(),
            CatalogItem::Index(i) => i.name = new_name.to_string(),
            CatalogItem::View(v) => v.name = new_name.to_string(),
            CatalogItem::Sink(s) => s.name = new_name.to_string(),
            CatalogItem::Function(_) | CatalogItem::Connection(_) => unreachable!(),
        }
    }

    fn definition(&self) -> &str {
        match self {
            CatalogItem::Table(t) => &t.definition,
            CatalogItem::Source(s) => &s.definition,
            CatalogItem::Index(_i) => "",
            CatalogItem::View(_v) => "",
            CatalogItem::Sink(s) => &s.definition,
            CatalogItem::Function(_) | CatalogItem::Connection(_) => unreachable!(),
        }
    }

    fn set_definition(&mut self, new_definition: String) {
        match self {
            CatalogItem::Table(t) => t.definition = new_definition,
            CatalogItem::Source(s) => s.definition = new_definition,
            CatalogItem::Index(_i) => (),
            CatalogItem::View(_v) => (),
            CatalogItem::Sink(s) => s.definition = new_definition,
            CatalogItem::Function(_) | CatalogItem::Connection(_) => unreachable!(),
        }
    }

    fn depends_on(&self) -> &[GlobalId] {
        match self {
            CatalogItem::Table(t) => &t.dependent_relations,
            CatalogItem::Source(s) => match &s.connection_id {
                Some(id) => std::slice::from_ref(id),
                None => &[],
            },
            CatalogItem::Index(_) => &[],
            CatalogItem::View(v) => &v.dependent_relations,
            CatalogItem::Sink(s) => &s.dependent_relations,
            CatalogItem::Function(_) => &[],
            CatalogItem::Connection(_) => &[],
        }
    }

    fn is_duplicated_with(&self, other: &Self) -> bool {
        if self.is_relation() ^ other.is_relation() {
            return false;
        }
        if self.database_id() != other.database_id()
            || self.schema_id() != other.schema_id()
            || self.name() != other.name()
        {
            return false;
        }
        if let CatalogItem::Function(f) = self {
            let CatalogItem::Function(f2) = other else { unreachable!(); };
            return f.arg_types == f2.arg_types;
        }
        false
    }
}

impl From<CatalogItem> for RelationInfo {
    fn from(value: CatalogItem) -> Self {
        match value {
            CatalogItem::Table(t) => RelationInfo::Table(t),
            CatalogItem::Source(s) => RelationInfo::Source(s),
            CatalogItem::Index(i) => RelationInfo::Index(i),
            CatalogItem::View(v) => RelationInfo::View(v),
            CatalogItem::Sink(s) => RelationInfo::Sink(s),
            CatalogItem::Function(_) | CatalogItem::Connection(_) => unreachable!(),
        }
    }
}

#[derive(Clone)]
struct DatabaseEntry {
    database: Database,
    owner: Option<ReferWeak>,
}

impl DatabaseEntry {
    fn new(database: Database, owner: Option<ReferWeak>) -> Self {
        Self { database, owner }
    }
}

impl Transactional for DatabaseEntry {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.database.upsert_in_transaction(trx)
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.database.delete_in_transaction(trx)
    }
}

#[derive(Clone)]
struct SchemaEntry {
    schema: Schema,
    owner: Option<ReferWeak>,
}

impl SchemaEntry {
    fn new(schema: Schema, owner: Option<ReferWeak>) -> Self {
        Self { schema, owner }
    }
}

impl Transactional for SchemaEntry {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.schema.upsert_in_transaction(trx)
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.schema.delete_in_transaction(trx)
    }
}

#[derive(Clone)]
pub struct CatalogEntry {
    item: CatalogItem,

    /// Hold refer.
    refer: Refer,
    owner: Option<ReferWeak>,
    depends_on: HashSet<ReferWeak>,
}

impl CatalogEntry {
    fn new(item: CatalogItem, owner: Option<ReferWeak>) -> Self {
        Self {
            item,
            refer: Refer::new(),
            owner,
            // will be filled in later.
            depends_on: HashSet::new(),
        }
    }

    fn id(&self) -> GlobalId {
        self.item.id()
    }
}

#[derive(Clone)]
pub struct UserEntry {
    info: UserInfo,

    /// Hold refer.
    refer: Refer,
    granted_by: HashSet<ReferWeak>,
}

impl UserEntry {
    fn new(info: UserInfo) -> Self {
        Self {
            info,
            refer: Refer::new(),
            granted_by: HashSet::new(),
        }
    }
}

impl Transactional for CatalogEntry {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        match &self.item {
            CatalogItem::Table(t) => t.upsert_in_transaction(trx),
            CatalogItem::Source(s) => s.upsert_in_transaction(trx),
            CatalogItem::Index(i) => i.upsert_in_transaction(trx),
            CatalogItem::View(v) => v.upsert_in_transaction(trx),
            CatalogItem::Sink(s) => s.upsert_in_transaction(trx),
            CatalogItem::Function(f) => f.upsert_in_transaction(trx),
            CatalogItem::Connection(c) => c.upsert_in_transaction(trx),
        }
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        match &self.item {
            CatalogItem::Table(t) => t.delete_in_transaction(trx),
            CatalogItem::Source(s) => s.delete_in_transaction(trx),
            CatalogItem::Index(i) => i.delete_in_transaction(trx),
            CatalogItem::View(v) => v.delete_in_transaction(trx),
            CatalogItem::Sink(s) => s.delete_in_transaction(trx),
            CatalogItem::Function(f) => f.delete_in_transaction(trx),
            CatalogItem::Connection(c) => c.delete_in_transaction(trx),
        }
    }
}

impl Transactional for UserEntry {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.info.upsert_in_transaction(trx)
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        self.info.delete_in_transaction(trx)
    }
}

/// TODO: manage `TableFragment` here.
pub struct CatalogCore {
    /// Cached database information.
    databases: BTreeMap<DatabaseId, DatabaseEntry>,
    /// Cached schema information.
    schemas: BTreeMap<SchemaId, SchemaEntry>,
    /// Cached catalog item information.
    /// TODO: we need to guarantee that the id of all items are unique, currently the index id is
    /// same as it's table id, the implicate created source id is same as it's table id.
    items: BTreeMap<GlobalId, CatalogEntry>,
    /// Cached user information.
    users: BTreeMap<UserId, UserEntry>,
}

pub struct CatalogSnapshot {
    databases: Vec<Database>,
    schemas: Vec<Schema>,

    // items
    tables: Vec<Table>,
    sources: Vec<Source>,
    indexes: Vec<Index>,
    views: Vec<View>,
    sinks: Vec<Sink>,
    functions: Vec<Function>,
    connections: Vec<Connection>,

    // users
    users: Vec<UserInfo>,
}

impl CatalogCore {
    fn snapshot(&self) -> CatalogSnapshot {
        let mut snapshot = CatalogSnapshot {
            databases: self
                .databases
                .values()
                .map(|e| e.database.clone())
                .collect_vec(),
            schemas: self
                .schemas
                .values()
                .map(|e| e.schema.clone())
                .collect_vec(),
            users: self.users.values().map(|u| u.info.clone()).collect_vec(),
            tables: Vec::new(),
            sources: Vec::new(),
            indexes: Vec::new(),
            views: Vec::new(),
            sinks: Vec::new(),
            functions: Vec::new(),
            connections: Vec::new(),
        };

        for entry in self.items.values() {
            match &entry.item {
                CatalogItem::Table(t) => snapshot.tables.push(t.clone()),
                CatalogItem::Source(s) => snapshot.sources.push(s.clone()),
                CatalogItem::Index(i) => snapshot.indexes.push(i.clone()),
                CatalogItem::View(v) => snapshot.views.push(v.clone()),
                CatalogItem::Sink(s) => snapshot.sinks.push(s.clone()),
                CatalogItem::Function(f) => snapshot.functions.push(f.clone()),
                CatalogItem::Connection(c) => snapshot.connections.push(c.clone()),
            }
        }

        snapshot
    }

    fn ensure_database_id(&self, id: DatabaseId) -> MetaResult<()> {
        if !self.databases.contains_key(&id) {
            Err(MetaError::catalog_id_not_found("database", id))
        } else {
            Ok(())
        }
    }

    fn ensure_schema_id(&self, id: SchemaId) -> MetaResult<()> {
        if !self.schemas.contains_key(&id) {
            Err(MetaError::catalog_id_not_found("schema", id))
        } else {
            Ok(())
        }
    }

    fn ensure_user_id(&self, id: UserId) -> MetaResult<()> {
        if !self.users.contains_key(&id) {
            Err(MetaError::catalog_id_not_found("user", id))
        } else {
            Ok(())
        }
    }

    fn get_user_by_id(&self, id: UserId) -> MetaResult<&UserEntry> {
        self.users
            .get(&id)
            .ok_or_else(|| MetaError::catalog_id_not_found("user", id))
    }

    fn check_user_droppable(&self, id: UserId) -> MetaResult<()> {
        self.ensure_user_id(id)?;
        let user = self.users.get(&id).unwrap();
        if user.info.name == DEFAULT_SUPER_USER || user.info.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop default super user {}",
                user.info.name
            )));
        }
        if !user.refer.can_delete() {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {}, because {} other item(s) depend on it",
                user.info.name,
                user.refer.used_count()
            )));
        }

        Ok(())
    }

    fn check_database_duplicated(&self, name: &str) -> MetaResult<()> {
        if self.databases.values().any(|db| db.database.name == name) {
            Err(MetaError::catalog_duplicated("database", name))
        } else {
            Ok(())
        }
    }

    fn check_schema_duplicated(&self, database_id: DatabaseId, name: &str) -> MetaResult<()> {
        if self
            .schemas
            .values()
            .any(|entry| entry.schema.database_id == database_id && entry.schema.name == name)
        {
            Err(MetaError::catalog_duplicated("schema", name))
        } else {
            Ok(())
        }
    }

    fn check_name_duplicated_for_create(&self, item: &CatalogItem) -> MetaResult<()> {
        if self
            .items
            .values()
            .any(|entry| entry.item.is_duplicated_with(item))
        {
            Err(MetaError::catalog_duplicated(item.item_type(), item.name()))
        } else {
            Ok(())
        }
    }

    fn schema_is_empty(&self, schema_id: SchemaId) -> bool {
        self.items
            .values()
            .all(|entry| entry.item.schema_id() != schema_id)
    }

    fn check_user_name_duplicated(&self, name: &str) -> MetaResult<()> {
        if self.users.values().any(|user| user.info.name == name) {
            Err(MetaError::catalog_duplicated("user", name))
        } else {
            Ok(())
        }
    }

    fn validate_item_for_create(&self, item: &CatalogItem) -> MetaResult<()> {
        self.ensure_database_id(item.database_id())?;
        self.ensure_schema_id(item.schema_id())?;
        self.check_name_duplicated_for_create(item)?;

        Ok(())
    }

    fn validate_item_for_drop(&self, relation: &str, id: GlobalId) -> MetaResult<()> {
        let item = self
            .items
            .get(&id)
            .ok_or_else(|| MetaError::catalog_id_not_found("item", id))?;
        if !item.refer.can_delete() {
            return Err(MetaError::permission_denied(format!(
                "Can't drop {relation}, because {} other object(s) depend on it",
                item.refer.used_count()
            )));
        }

        Ok(())
    }

    fn validate_item_for_rename(&self, id: GlobalId, new_name: &str) -> MetaResult<()> {
        let item = self
            .items
            .get(&id)
            .ok_or_else(|| MetaError::catalog_id_not_found("item", id))?;

        let mut item = item.item.clone();
        item.set_name(new_name);

        if self
            .items
            .values()
            .any(|entry| entry.item.is_duplicated_with(&item))
        {
            return Err(MetaError::catalog_duplicated(item.item_type(), new_name));
        }

        Ok(())
    }

    fn resolve_dependencies(&self, entry: &mut CatalogEntry) -> MetaResult<()> {
        for id in entry.item.depends_on() {
            let refer = self
                .items
                .get(id)
                .ok_or_else(|| MetaError::catalog_id_not_found("item", *id))?
                .refer
                .refer();
            entry.depends_on.insert(refer);
        }

        Ok(())
    }

    fn resolve_user_grantee(&self, entry: &mut UserEntry) -> MetaResult<()> {
        entry.granted_by.clear();
        for privilege_item in &entry.info.grant_privileges {
            for opt in &privilege_item.action_with_opts {
                entry
                    .granted_by
                    .insert(self.get_user_refer(opt.granted_by)?);
            }
        }
        Ok(())
    }

    fn get_user_refer(&self, user_id: UserId) -> MetaResult<ReferWeak> {
        self.users
            .get(&user_id)
            .map(|user| user.refer.refer())
            .ok_or_else(|| MetaError::catalog_id_not_found("user", user_id))
    }
}

pub struct CatalogManagerV2<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<CatalogCore>,
}

impl<S> CatalogManagerV2<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let databases = Database::list(env.meta_store()).await?;
        let schemas = Schema::list(env.meta_store()).await?;

        let sources = Source::list(env.meta_store()).await?;
        let sinks = Sink::list(env.meta_store()).await?;
        let tables = Table::list(env.meta_store()).await?;
        let indexes = Index::list(env.meta_store()).await?;
        let views = View::list(env.meta_store()).await?;
        let functions = Function::list(env.meta_store()).await?;
        let connections = Connection::list(env.meta_store()).await?;

        let users = UserInfo::list(env.meta_store()).await?;

        let mut user_map: BTreeMap<UserId, UserEntry> = users
            .into_iter()
            .map(|u| (u.id, UserEntry::new(u)))
            .collect();

        let user_refers: HashMap<UserId, ReferWeak> = user_map
            .iter()
            .map(|(id, entry)| (*id, entry.refer.refer()))
            .collect();

        let get_user_refer =
            |user_id: UserId| user_refers.get(&user_id).cloned().expect("user not found");

        // Init granted by for all users.
        for entry in user_map.values_mut() {
            for privilege_item in &entry.info.grant_privileges {
                for opt in &privilege_item.action_with_opts {
                    entry.granted_by.insert(get_user_refer(opt.granted_by));
                }
            }
        }

        // Init owner for all databases and schemas.
        let database_map = databases
            .into_iter()
            .map(|d| {
                let owner_refer = Some(get_user_refer(d.get_owner()));
                (d.id, DatabaseEntry::new(d, owner_refer))
            })
            .collect();
        let schema_map = schemas
            .into_iter()
            .map(|s| {
                let owner_refer = Some(get_user_refer(s.get_owner()));
                (s.id, SchemaEntry::new(s, owner_refer))
            })
            .collect();

        /// Init item map with owner refer.
        macro_rules! init_item_with_owner {
            ($map:ident, $({ $item_ty:ident, $items:expr }),*) => {
                $(
                    for item in $items {
                        let owner_refer = Some(get_user_refer(item.get_owner()));
                        $map.insert(item.get_id(), CatalogEntry::new(CatalogItem::$item_ty(item), owner_refer));
                    }
                )*
            };
        }

        let mut item_map: BTreeMap<GlobalId, CatalogEntry> = BTreeMap::new();
        init_item_with_owner! {
            item_map,
            { Source, sources },
            { Sink, sinks },
            { Table, tables },
            { Index, indexes },
            { View, views },
            { Function, functions },
            { Connection, connections }
        }

        // Init item refers for all items.
        let item_refers: HashMap<GlobalId, ReferWeak> = item_map
            .iter()
            .map(|(id, entry)| (*id, entry.refer.refer()))
            .collect();
        for item in item_map.values_mut() {
            for id in item.item.depends_on() {
                item.depends_on
                    .insert(item_refers.get(id).cloned().expect("item not found"));
            }
        }

        let manager = Self {
            env,
            core: Mutex::new(CatalogCore {
                databases: database_map,
                schemas: schema_map,
                items: item_map,
                users: user_map,
            }),
        };
        manager.init().await?;

        Ok(manager)
    }

    async fn init(&self) -> MetaResult<()> {
        self.init_user().await?;
        self.init_database().await?;
        Ok(())
    }

    async fn init_database(&self) -> MetaResult<()> {
        if self
            .core
            .lock()
            .await
            .check_database_duplicated(DEFAULT_DATABASE_NAME)
            .is_ok()
        {
            self.create_database(Database {
                id: self
                    .env
                    .id_gen_manager()
                    .generate::<{ IdCategory::Database }>()
                    .await? as u32,
                name: DEFAULT_DATABASE_NAME.to_string(),
                owner: DEFAULT_SUPER_USER_ID,
            })
            .await?;
        }

        Ok(())
    }

    async fn init_schemas(
        &self,
        database_id: DatabaseId,
        owner: UserId,
    ) -> MetaResult<Vec<Schema>> {
        let mut schemas = vec![];
        for name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            schemas.push(Schema {
                id: self
                    .env
                    .id_gen_manager()
                    .generate::<{ IdCategory::Schema }>()
                    .await? as u32,
                database_id,
                name: name.to_string(),
                owner,
            });
        }
        Ok(schemas)
    }

    async fn init_user(&self) -> MetaResult<()> {
        for (user, id) in [
            (DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID),
            (DEFAULT_SUPER_USER_FOR_PG, DEFAULT_SUPER_USER_FOR_PG_ID),
        ] {
            if self
                .core
                .lock()
                .await
                .check_user_name_duplicated(user)
                .is_ok()
            {
                self.create_user(UserInfo {
                    id,
                    name: user.to_string(),
                    is_super: true,
                    can_create_db: true,
                    can_create_user: true,
                    can_login: true,
                    ..Default::default()
                })
                .await?;
            }
        }

        Ok(())
    }

    async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }
}

// Catalog related interfaces.
impl<S> CatalogManagerV2<S>
where
    S: MetaStore,
{
    /// Infer item entries from items, the dependency references will be resolved so that the
    /// referred users and items won't be dropped during creation.
    pub(crate) async fn infer_item_entries(
        &self,
        items: Vec<CatalogItem>,
    ) -> MetaResult<Vec<CatalogEntry>> {
        let core = &mut *self.core.lock().await;
        let mut entries = Vec::with_capacity(items.len());
        for item in items {
            core.validate_item_for_create(&item)?;
            let user_refer = core.get_user_refer(item.owner())?;
            let mut entry = CatalogEntry::new(item, Some(user_refer));
            core.resolve_dependencies(&mut entry)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    pub async fn create_database(&self, db: Database) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.check_database_duplicated(&db.name)?;
        core.ensure_user_id(db.owner)?;
        let user_refer = core.get_user_refer(db.owner).unwrap();

        let mut txn_database = BTreeMapTransaction::new(&mut core.databases);
        let mut txn_schema = BTreeMapTransaction::new(&mut core.schemas);

        let new_schemas = self.init_schemas(db.id, db.owner).await?;
        for schema in &new_schemas {
            txn_schema.insert(
                schema.id,
                SchemaEntry::new(schema.clone(), Some(user_refer.clone())),
            );
        }
        txn_database.insert(db.id, DatabaseEntry::new(db.clone(), Some(user_refer)));
        commit_meta!(self, txn_database, txn_schema)?;

        let mut version = self
            .notify_frontend(Operation::Add, Info::Database(db))
            .await;
        for schema in new_schemas {
            version = self
                .notify_frontend(Operation::Add, Info::Schema(schema))
                .await;
        }
        Ok(version)
    }

    /// `drop_database` will drop the database and the corresponding schemas and items in it. It
    /// returns all dropped streaming ids, source ids and the notification version.
    pub async fn drop_database(
        &self,
        id: DatabaseId,
    ) -> MetaResult<(Vec<GlobalId>, Vec<GlobalId>, NotificationVersion)> {
        let core = &mut *self.core.lock().await;
        core.ensure_database_id(id)?;

        let schemas_to_drop = core
            .schemas
            .values()
            .filter(|entry| entry.schema.database_id == id)
            .map(|entry| entry.schema.id)
            .collect_vec();
        let items_to_drop = core
            .items
            .values()
            .filter(|item| item.item.database_id() == id)
            .map(|item| item.id())
            .collect_vec();

        let mut txn_database = BTreeMapTransaction::new(&mut core.databases);
        let mut txn_schema = BTreeMapTransaction::new(&mut core.schemas);
        let mut txn_item = BTreeMapTransaction::new(&mut core.items);
        let mut txn_user = BTreeMapTransaction::new(&mut core.users);

        let mut to_clear_objs = std::iter::once(PbObject::DatabaseId(id))
            .chain(schemas_to_drop.iter().map(|id| PbObject::SchemaId(*id)))
            .collect_vec();
        let mut streaming_ids = vec![];
        let mut source_ids = vec![];
        let db = txn_database.remove(id).unwrap();
        schemas_to_drop.iter().for_each(|id| {
            txn_schema.remove(*id).unwrap();
        });
        items_to_drop.iter().for_each(|id| {
            let item = txn_item.remove(*id).unwrap();
            to_clear_objs.push(item.item.pb_object_id());
            if item.item.has_streaming_job() {
                streaming_ids.push(item.item.id());
            }
            if let CatalogItem::Source(source) = &item.item {
                source_ids.push(source.id);
            }
        });
        let users_need_update = Self::clean_user_privileges(&mut txn_user, &to_clear_objs);

        commit_meta!(self, txn_database, txn_schema, txn_item, txn_user)?;

        for user in users_need_update {
            self.notify_frontend(Operation::Update, Info::User(user))
                .await;
        }
        let version = self
            .notify_frontend(Operation::Delete, Info::Database(db.database))
            .await;

        Ok((streaming_ids, source_ids, version))
    }

    pub async fn create_schema(&self, schema: Schema) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.ensure_database_id(schema.database_id)?;
        core.ensure_user_id(schema.owner)?;
        core.check_schema_duplicated(schema.database_id, &schema.name)?;
        let user_refer = core.get_user_refer(schema.owner).unwrap();

        let mut txn_schema = BTreeMapTransaction::new(&mut core.schemas);
        txn_schema.insert(
            schema.id,
            SchemaEntry::new(schema.clone(), Some(user_refer)),
        );
        commit_meta!(self, txn_schema)?;

        let version = self
            .notify_frontend(Operation::Add, Info::Schema(schema))
            .await;
        Ok(version)
    }

    pub async fn drop_schema(&self, id: SchemaId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.ensure_schema_id(id)?;
        if !core.schema_is_empty(id) {
            return Err(MetaError::permission_denied(
                "The schema is not empty, try dropping them first".into(),
            ));
        }

        let mut txn_schema = BTreeMapTransaction::new(&mut core.schemas);
        let mut txn_user = BTreeMapTransaction::new(&mut core.users);
        let schema = txn_schema.remove(id).unwrap();
        let users_need_update =
            Self::clean_user_privileges(&mut txn_user, &[PbObject::SchemaId(id)]);
        commit_meta!(self, txn_schema, txn_user)?;

        for user in users_need_update {
            self.notify_frontend(Operation::Update, Info::User(user))
                .await;
        }
        let version = self
            .notify_frontend(Operation::Delete, Info::Schema(schema.schema))
            .await;
        Ok(version)
    }

    pub async fn drop_items(&self, ids: Vec<GlobalId>) -> MetaResult<NotificationVersion> {
        assert!(!ids.is_empty());
        let core = &mut *self.core.lock().await;
        for id in &ids {
            core.validate_item_for_drop("item", *id)?;
        }

        let mut txn_item = BTreeMapTransaction::new(&mut core.items);
        let mut txn_user = BTreeMapTransaction::new(&mut core.users);
        let items = ids
            .into_iter()
            .map(|id| txn_item.remove(id).unwrap())
            .collect_vec();
        let users_to_update = Self::clean_user_privileges(
            &mut txn_user,
            &items
                .iter()
                .map(|item| item.item.pb_object_id())
                .collect_vec(),
        );
        commit_meta!(self, txn_item, txn_user)?;

        for user in users_to_update {
            self.notify_frontend(Operation::Update, Info::User(user))
                .await;
        }
        let version = self
            .notify_frontend(
                Operation::Delete,
                Info::RelationGroup(RelationGroup {
                    relations: items
                        .into_iter()
                        .map(|item| Relation {
                            relation_info: Some(item.item.into()),
                        })
                        .collect(),
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_item(&self, id: GlobalId) -> MetaResult<NotificationVersion> {
        self.drop_items(vec![id]).await
    }

    pub async fn create_items(
        &self,
        entries: Vec<CatalogEntry>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let mut txn = BTreeMapTransaction::new(&mut core.items);
        for entry in &entries {
            ensure!(
                entry.depends_on.len() == entry.item.depends_on().len(),
                "reference info not resolved"
            );
            ensure!(entry.owner.is_some(), "owner not resolved");

            txn.insert(entry.id(), entry.clone());
        }
        commit_meta!(self, txn)?;

        let version = self
            .notify_frontend(
                Operation::Add,
                Info::RelationGroup(RelationGroup {
                    relations: entries
                        .into_iter()
                        .map(|entry| Relation {
                            relation_info: Some(entry.item.into()),
                        })
                        .collect(),
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_item(&self, entry: CatalogEntry) -> MetaResult<NotificationVersion> {
        self.create_items(vec![entry]).await
    }

    pub async fn alter_item_name(
        &self,
        id: GlobalId,
        new_name: &str,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.validate_item_for_rename(id, new_name)?;

        let mut item = core.items.get(&id).unwrap().clone();
        ensure!(item.item.is_relation(), "only relation can be renamed");
        let old_name = item.item.name().to_owned();
        item.item.set_name(new_name);
        item.item
            .set_definition(alter_relation_rename(item.item.definition(), new_name));

        let mut items = vec![];
        if let CatalogItem::Table(table) = &item.item &&
            let Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id)) = &table.optional_associated_source_id {
            let mut item = core.items.get(source_id).unwrap().clone();
            item.item.set_name(new_name);
            items.push(item);
        }
        items.push(item);

        for item in core.items.values() {
            if item.item.depends_on().contains(&id) {
                let mut item = item.clone();
                item.item.set_definition(alter_relation_rename_refs(
                    item.item.definition(),
                    &old_name,
                    new_name,
                ));
                items.push(item);
            }
        }

        let mut txn = BTreeMapTransaction::new(&mut core.items);
        for item in &items {
            txn.insert(item.id(), item.clone());
        }
        commit_meta!(self, txn)?;

        let version = self
            .notify_frontend(
                Operation::Update,
                Info::RelationGroup(RelationGroup {
                    relations: items
                        .into_iter()
                        .map(|item| Relation {
                            relation_info: Some(item.item.into()),
                        })
                        .collect(),
                }),
            )
            .await;

        Ok(version)
    }
}

// User related interfaces.
impl<S> CatalogManagerV2<S>
where
    S: MetaStore,
{
    pub async fn create_user(&self, user: UserInfo) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.check_user_name_duplicated(&user.name)?;

        let mut txn = BTreeMapTransaction::new(&mut core.users);
        txn.insert(user.id, UserEntry::new(user.clone()));
        commit_meta!(self, txn)?;

        let version = self.notify_frontend(Operation::Add, Info::User(user)).await;
        Ok(version)
    }

    pub async fn drop_user(&self, id: UserId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        core.check_user_droppable(id)?;

        let mut txn = BTreeMapTransaction::new(&mut core.users);
        let user = txn.remove(id).unwrap();
        commit_meta!(self, txn)?;

        let version = self
            .notify_frontend(Operation::Delete, Info::User(user.info))
            .await;
        Ok(version)
    }

    pub async fn update_user(
        &self,
        update_user: UserInfo,
        update_fields: Vec<UpdateField>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await;
        let rename_flag = update_fields
            .iter()
            .any(|&field| field == UpdateField::Rename);
        if rename_flag {
            core.check_user_name_duplicated(&update_user.name)?;
        }

        let mut txn = BTreeMapTransaction::new(&mut core.users);
        let mut user = txn.get_mut(update_user.id).unwrap();

        for field in update_fields {
            match field {
                UpdateField::Unspecified => unreachable!(),
                UpdateField::Super => user.info.is_super = update_user.is_super,
                UpdateField::Login => user.info.can_login = update_user.can_login,
                UpdateField::CreateDb => user.info.can_create_db = update_user.can_create_db,
                UpdateField::CreateUser => user.info.can_create_user = update_user.can_create_user,
                UpdateField::AuthInfo => unreachable!(),
                UpdateField::Rename => user.info.name = update_user.name.clone(),
            }
        }

        let new_user = user.info.clone();
        commit_meta!(self, txn)?;

        let version = self
            .notify_frontend(Operation::Update, Info::User(new_user))
            .await;
        Ok(version)
    }

    // Check whether new_privilege is a subset of origin_privilege, and check grand_option if
    // `need_grand_option` is set.
    // TODO: https://github.com/risingwavelabs/risingwave/issues/7097
    #[inline(always)]
    fn check_privilege(
        origin_privilege: &GrantPrivilege,
        new_privilege: &GrantPrivilege,
        need_grand_option: bool,
    ) -> bool {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let action_map = HashMap::<i32, bool>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, ao.with_grant_option)),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(with_grant_option) = action_map.get(&nao.action) {
                if !with_grant_option && need_grand_option {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    // Merge new granted privilege.
    #[inline(always)]
    fn merge_privilege(origin_privilege: &mut GrantPrivilege, new_privilege: &GrantPrivilege) {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let mut action_map = HashMap::<i32, (bool, u32)>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, (ao.with_grant_option, ao.granted_by))),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(o) = action_map.get_mut(&nao.action) {
                o.0 |= nao.with_grant_option;
            } else {
                action_map.insert(nao.action, (nao.with_grant_option, nao.granted_by));
            }
        }
        origin_privilege.action_with_opts = action_map
            .into_iter()
            .map(
                |(action, (with_grant_option, granted_by))| ActionWithGrantOption {
                    action,
                    with_grant_option,
                    granted_by,
                },
            )
            .collect();
    }

    pub async fn grant_privilege(
        &self,
        user_ids: &[UserId],
        new_grant_privileges: &[GrantPrivilege],
        grantor: UserId,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let grantor_info = core.get_user_by_id(grantor)?.info.clone();
        let mut txn = BTreeMapTransaction::new(&mut core.users);
        let mut user_updated = Vec::with_capacity(user_ids.len());

        for user_id in user_ids {
            let mut user = txn
                .get_mut(*user_id)
                .ok_or_else(|| MetaError::catalog_id_not_found("user", *user_id))?;

            if user.info.is_super {
                return Err(MetaError::permission_denied(format!(
                    "Cannot grant privilege to super user {}",
                    user_id
                )));
            }
            if !grantor_info.is_super {
                for new_grant_privilege in new_grant_privileges {
                    if let Some(privilege) = grantor_info
                        .grant_privileges
                        .iter()
                        .find(|p| p.object == new_grant_privilege.object)
                    {
                        if !Self::check_privilege(privilege, new_grant_privilege, true) {
                            return Err(MetaError::permission_denied(format!(
                                "Cannot grant privilege without grant permission for user {}",
                                grantor
                            )));
                        }
                    } else {
                        return Err(MetaError::permission_denied(format!(
                            "Grantor {} does not have one of the privileges",
                            grantor
                        )));
                    }
                }
            }
            new_grant_privileges.iter().for_each(|new_grant_privilege| {
                if let Some(privilege) = user
                    .info
                    .grant_privileges
                    .iter_mut()
                    .find(|p| p.object == new_grant_privilege.object)
                {
                    Self::merge_privilege(privilege, new_grant_privilege);
                } else {
                    user.info.grant_privileges.push(new_grant_privilege.clone());
                }
            });
            user_updated.push(user.info.clone());
        }

        commit_meta!(self, txn)?;
        for user_id in user_ids {
            let mut user = core.users.get(user_id).cloned().expect("user must exist");
            core.resolve_user_grantee(&mut user)?;
            core.users.insert(*user_id, user);
        }

        let mut version = 0;
        // FIXME: user might not be updated.
        for user in user_updated {
            version = self
                .notify_frontend(Operation::Update, Info::User(user))
                .await;
        }

        Ok(version)
    }

    // Revoke privilege from object.
    #[inline(always)]
    fn revoke_privilege_inner(
        origin_privilege: &mut GrantPrivilege,
        revoke_grant_privilege: &GrantPrivilege,
        revoke_grant_option: bool,
    ) -> bool {
        assert_eq!(origin_privilege.object, revoke_grant_privilege.object);
        let mut has_change = false;
        if revoke_grant_option {
            // Only revoke with grant option.
            origin_privilege.action_with_opts.iter_mut().for_each(|ao| {
                if revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|ro| ro.action == ao.action)
                {
                    ao.with_grant_option = false;
                    has_change = true;
                }
            })
        } else {
            let sz = origin_privilege.action_with_opts.len();
            // Revoke all privileges matched with revoke_grant_privilege.
            origin_privilege.action_with_opts.retain(|ao| {
                !revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|rao| rao.action == ao.action)
            });
            has_change = sz != origin_privilege.action_with_opts.len();
        }
        has_change
    }

    pub async fn revoke_privilege(
        &self,
        user_ids: &[UserId],
        revoke_grant_privileges: &[GrantPrivilege],
        granted_by: UserId,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await;
        let mut txn = BTreeMapTransaction::new(&mut core.users);
        let mut user_updated = HashMap::new();
        let mut users_info: VecDeque<UserInfo> = VecDeque::new();
        let mut visited = HashSet::new();
        // check revoke permission
        let UserEntry {
            info: revoke_by, ..
        } = txn
            .get(&revoke_by)
            .ok_or_else(|| MetaError::catalog_id_not_found("user", revoke_by))?;
        let same_user = granted_by == revoke_by.id;
        if !revoke_by.is_super {
            for privilege in revoke_grant_privileges {
                if let Some(user_privilege) = revoke_by
                    .grant_privileges
                    .iter()
                    .find(|p| p.object == privilege.object)
                {
                    if !Self::check_privilege(user_privilege, privilege, same_user) {
                        return Err(MetaError::permission_denied(format!(
                            "Cannot revoke privilege without permission for user {}",
                            &revoke_by.name
                        )));
                    }
                } else {
                    return Err(MetaError::permission_denied(format!(
                        "User {} does not have one of the privileges",
                        &revoke_by.name
                    )));
                }
            }
        }
        // revoke privileges
        for user_id in user_ids {
            let user = txn
                .get(user_id)
                .map(|e| e.info.clone())
                .ok_or_else(|| anyhow!("User {} does not exist", user_id))?;
            if user.is_super {
                return Err(MetaError::permission_denied(format!(
                    "Cannot revoke privilege from supper user {}",
                    user_id
                )));
            }
            users_info.push_back(user);
        }
        while !users_info.is_empty() {
            let mut cur_user = users_info.pop_front().unwrap();
            let mut recursive_flag = false;
            let mut empty_privilege = false;
            let cur_revoke_grant_option = revoke_grant_option && user_ids.contains(&cur_user.id);
            visited.insert(cur_user.id);
            revoke_grant_privileges
                .iter()
                .for_each(|revoke_grant_privilege| {
                    for privilege in &mut cur_user.grant_privileges {
                        if privilege.object == revoke_grant_privilege.object {
                            recursive_flag |= Self::revoke_privilege_inner(
                                privilege,
                                revoke_grant_privilege,
                                cur_revoke_grant_option,
                            );
                            empty_privilege |= privilege.action_with_opts.is_empty();
                            break;
                        }
                    }
                });
            if recursive_flag {
                // check with cascade/restrict strategy
                if !cascade && !user_ids.contains(&cur_user.id) {
                    return Err(MetaError::permission_denied(format!(
                        "Cannot revoke privilege from user {} for restrict",
                        &cur_user.name
                    )));
                }
                txn.tree_ref()
                    .values()
                    .filter(|e| {
                        !visited.contains(&e.info.id)
                            && e.info.grant_privileges.iter().any(|privilege| {
                                privilege
                                    .action_with_opts
                                    .iter()
                                    .any(|ao| ao.granted_by == cur_user.id)
                            })
                    })
                    .for_each(|e| users_info.push_back(e.info.clone()));

                if empty_privilege {
                    cur_user
                        .grant_privileges
                        .retain(|privilege| !privilege.action_with_opts.is_empty());
                }
                if let std::collections::hash_map::Entry::Vacant(e) =
                    user_updated.entry(cur_user.id)
                {
                    txn.insert(cur_user.id, UserEntry::new(cur_user.clone()));
                    e.insert(cur_user);
                }
            }
        }

        commit_meta!(self, txn)?;

        // Since we might revoke privileges recursively, just simply re-build the grant relation
        // map here.
        for user_id in user_updated.keys() {
            let mut user = core
                .users
                .get_mut(user_id)
                .cloned()
                .expect("user must exist");
            core.resolve_user_grantee(&mut user)?;
            core.users.insert(*user_id, user);
        }

        let mut version = 0;
        // FIXME: user might not be updated.
        for (_, user_info) in user_updated {
            version = self
                .notify_frontend(Operation::Update, Info::User(user_info))
                .await;
        }

        Ok(version)
    }

    /// `clean_user_privileges` removes the privileges with given object from given users, it will
    /// be called when a database/schema/table/source/sink is dropped.
    fn clean_user_privileges(
        users: &mut BTreeMapTransaction<'_, UserId, UserEntry>,
        objects: &[PbObject],
    ) -> Vec<UserInfo> {
        let mut users_need_update = vec![];
        let user_keys = users.tree_ref().keys().copied().collect_vec();
        for user_id in user_keys {
            let mut user = users.get_mut(user_id).unwrap();
            let mut new_grant_privileges = user.info.grant_privileges.clone();
            new_grant_privileges.retain(|p| !objects.contains(p.object.as_ref().unwrap()));
            if new_grant_privileges.len() != user.info.grant_privileges.len() {
                user.info.grant_privileges = new_grant_privileges;
                users_need_update.push(user.info.clone());
            }
        }
        users_need_update
    }
}
