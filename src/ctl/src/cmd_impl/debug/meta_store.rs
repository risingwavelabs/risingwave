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

use std::collections::BTreeSet;
use std::sync::Arc;

use etcd_client::ConnectOptions;
use risingwave_meta::model::{MetadataModel, TableFragments, Worker};
use risingwave_meta::storage::meta_store::MetaStore;
use risingwave_meta::storage::{EtcdMetaStore, WrappedEtcdClient};
use risingwave_meta::{ElectionClient, ElectionMember, EtcdElectionClient};
use risingwave_pb::catalog;
use risingwave_pb::user::UserInfo;
use serde_yaml::Value;

use crate::{DebugCommon, DebugCommonKind, DebugCommonOutputFormat};

const KIND_KEY: &str = "kind";
const ITEM_KEY: &str = "item";

macro_rules! yaml_arm {
    ($kind:tt, $item:expr) => {{
        let mut mapping = serde_yaml::Mapping::new();
        mapping.insert(
            Value::String(KIND_KEY.to_string()),
            Value::String($kind.to_string()),
        );
        mapping.insert(
            Value::String(ITEM_KEY.to_string()),
            serde_yaml::to_value($item).unwrap(),
        );
        serde_yaml::Value::Mapping(mapping)
    }};
}

macro_rules! json_arm {
    ($kind:tt, $item:expr) => {{
        let mut mapping = serde_json::Map::new();
        mapping.insert(
            KIND_KEY.to_string(),
            serde_json::Value::String($kind.to_string()),
        );
        mapping.insert(ITEM_KEY.to_string(), serde_json::to_value($item).unwrap());
        serde_json::Value::Object(mapping)
    }};
}

enum Item {
    Worker(Worker),
    User(UserInfo),
    Table(TableFragments),
    MetaMember(ElectionMember),
    SourceCatalog(catalog::Source),
    SinkCatalog(catalog::Sink),
    IndexCatalog(catalog::Index),
    FunctionCatalog(catalog::Function),
    ViewCatalog(catalog::View),
    ConnectionCatalog(catalog::Connection),
    DatabaseCatalog(catalog::Database),
    SchemaCatalog(catalog::Schema),
    TableCatalog(catalog::Table),
}

macro_rules! generate_list_branches_for_catalog {
    ($kind:expr, $catalog:ident, $snapshot:expr, $items:ident) => {
        catalog::$catalog::list_at_snapshot::<EtcdMetaStore>($snapshot)
            .await?
            .into_iter()
            .for_each(|item| $items.push($kind(item)))
    };
}

pub async fn dump(common: DebugCommon) -> anyhow::Result<()> {
    let DebugCommon {
        etcd_endpoints,
        etcd_username,
        etcd_password,
        enable_etcd_auth,
        kinds,
        format,
    } = common;

    let client = if enable_etcd_auth {
        let options = ConnectOptions::default().with_user(
            etcd_username.clone().unwrap_or_default(),
            etcd_password.clone().unwrap_or_default(),
        );
        WrappedEtcdClient::connect(etcd_endpoints.clone(), Some(options), true).await?
    } else {
        WrappedEtcdClient::connect(etcd_endpoints.clone(), None, false).await?
    };

    let meta_store = Arc::new(EtcdMetaStore::new(client));
    let snapshot = meta_store.snapshot().await;
    let kinds: BTreeSet<_> = kinds.into_iter().collect();

    let mut items = vec![];
    for kind in kinds {
        match kind {
            DebugCommonKind::Worker => Worker::list_at_snapshot::<EtcdMetaStore>(&snapshot)
                .await?
                .into_iter()
                .for_each(|worker| items.push(Item::Worker(worker))),
            DebugCommonKind::User => UserInfo::list_at_snapshot::<EtcdMetaStore>(&snapshot)
                .await?
                .into_iter()
                .for_each(|user| items.push(Item::User(user))),
            DebugCommonKind::Table => TableFragments::list_at_snapshot::<EtcdMetaStore>(&snapshot)
                .await?
                .into_iter()
                .for_each(|table| items.push(Item::Table(table))),
            DebugCommonKind::MetaMember => {
                let election_client = if enable_etcd_auth {
                    let options = ConnectOptions::default().with_user(
                        etcd_username.clone().unwrap_or_default(),
                        etcd_password.clone().unwrap_or_default(),
                    );
                    EtcdElectionClient::new(
                        etcd_endpoints.clone(),
                        Some(options),
                        true,
                        "_".to_string(),
                    )
                    .await?
                } else {
                    EtcdElectionClient::new(etcd_endpoints.clone(), None, false, "_".to_string())
                        .await?
                };

                let election_client: Box<dyn ElectionClient> = Box::new(election_client);

                election_client
                    .get_members()
                    .await?
                    .into_iter()
                    .for_each(|member| {
                        items.push(Item::MetaMember(member));
                    });
            }
            DebugCommonKind::SourceCatalog => {
                generate_list_branches_for_catalog!(Item::SourceCatalog, Source, &snapshot, items)
            }
            DebugCommonKind::SinkCatalog => {
                generate_list_branches_for_catalog!(Item::SinkCatalog, Sink, &snapshot, items)
            }
            DebugCommonKind::IndexCatalog => {
                generate_list_branches_for_catalog!(Item::IndexCatalog, Index, &snapshot, items)
            }
            DebugCommonKind::FunctionCatalog => {
                generate_list_branches_for_catalog!(
                    Item::FunctionCatalog,
                    Function,
                    &snapshot,
                    items
                )
            }
            DebugCommonKind::ViewCatalog => {
                generate_list_branches_for_catalog!(Item::ViewCatalog, View, &snapshot, items)
            }
            DebugCommonKind::ConnectionCatalog => {
                generate_list_branches_for_catalog!(
                    Item::ConnectionCatalog,
                    Connection,
                    &snapshot,
                    items
                )
            }
            DebugCommonKind::DatabaseCatalog => {
                generate_list_branches_for_catalog!(
                    Item::DatabaseCatalog,
                    Database,
                    &snapshot,
                    items
                )
            }
            DebugCommonKind::SchemaCatalog => {
                generate_list_branches_for_catalog!(Item::SchemaCatalog, Schema, &snapshot, items)
            }
            DebugCommonKind::TableCatalog => {
                generate_list_branches_for_catalog!(Item::TableCatalog, Table, &snapshot, items)
            }
        };
    }

    let writer = std::io::stdout();

    match format {
        DebugCommonOutputFormat::Yaml => {
            let mut seq = serde_yaml::Sequence::new();
            for item in items {
                seq.push(match item {
                    Item::Worker(worker) => yaml_arm!("worker", worker.to_protobuf()),
                    Item::User(user) => yaml_arm!("user", user.to_protobuf()),
                    Item::Table(table) => yaml_arm!("table", table.to_protobuf()),
                    Item::MetaMember(member) => {
                        yaml_arm!("meta_member", member)
                    }
                    Item::SourceCatalog(catalog) => {
                        yaml_arm!("source_catalog", catalog)
                    }
                    Item::SinkCatalog(catalog) => yaml_arm!("sink_catalog", catalog),
                    Item::IndexCatalog(catalog) => yaml_arm!("index_catalog", catalog),
                    Item::FunctionCatalog(catalog) => yaml_arm!("function_catalog", catalog),
                    Item::ViewCatalog(catalog) => yaml_arm!("view_catalog", catalog),
                    Item::ConnectionCatalog(catalog) => yaml_arm!("connection_catalog", catalog),
                    Item::DatabaseCatalog(catalog) => yaml_arm!("database_catalog", catalog),
                    Item::SchemaCatalog(catalog) => yaml_arm!("schema_catalog", catalog),
                    Item::TableCatalog(catalog) => yaml_arm!("table_catalog", catalog),
                });
            }
            serde_yaml::to_writer(writer, &seq).unwrap();
        }
        DebugCommonOutputFormat::Json => {
            let mut seq = vec![];
            for item in items {
                seq.push(match item {
                    Item::Worker(worker) => json_arm!("worker", worker.to_protobuf()),
                    Item::User(user) => json_arm!("user", user.to_protobuf()),
                    Item::Table(table) => json_arm!("table", table.to_protobuf()),
                    Item::MetaMember(member) => {
                        json_arm!("meta_member", member)
                    }
                    Item::SourceCatalog(catalog) => json_arm!("source_catalog", catalog),
                    Item::SinkCatalog(catalog) => json_arm!("sink_catalog", catalog),
                    Item::IndexCatalog(catalog) => json_arm!("index_catalog", catalog),
                    Item::FunctionCatalog(catalog) => json_arm!("function_catalog", catalog),
                    Item::ViewCatalog(catalog) => json_arm!("view_catalog", catalog),
                    Item::ConnectionCatalog(catalog) => json_arm!("connection_catalog", catalog),
                    Item::DatabaseCatalog(catalog) => json_arm!("database_catalog", catalog),
                    Item::SchemaCatalog(catalog) => json_arm!("schema_catalog", catalog),
                    Item::TableCatalog(catalog) => json_arm!("table_catalog", catalog),
                });
            }
            serde_json::to_writer_pretty(writer, &seq).unwrap();
        }
    }

    Ok(())
}
