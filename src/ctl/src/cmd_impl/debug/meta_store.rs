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
            serde_yaml::to_value($item.to_protobuf()).unwrap(),
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
        mapping.insert(
            ITEM_KEY.to_string(),
            serde_json::to_value($item.to_protobuf()).unwrap(),
        );
        serde_json::Value::Object(mapping)
    }};
}

enum Item {
    Worker(Worker),
    User(UserInfo),
    Table(TableFragments),
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
            etcd_username.unwrap_or_default(),
            etcd_password.unwrap_or_default(),
        );
        WrappedEtcdClient::connect(etcd_endpoints, Some(options), true).await?
    } else {
        WrappedEtcdClient::connect(etcd_endpoints, None, false).await?
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
        };
    }

    let writer = std::io::stdout();

    match format {
        DebugCommonOutputFormat::Yaml => {
            let mut seq = serde_yaml::Sequence::new();
            for item in items {
                seq.push(match item {
                    Item::Worker(worker) => yaml_arm!("worker", worker),
                    Item::User(user) => yaml_arm!("user", user),
                    Item::Table(table) => yaml_arm!("table", table),
                });
            }
            serde_yaml::to_writer(writer, &seq).unwrap();
        }
        DebugCommonOutputFormat::Json => {
            let mut seq = vec![];
            for item in items {
                seq.push(match item {
                    Item::Worker(worker) => json_arm!("worker", worker),
                    Item::User(user) => json_arm!("user", user),
                    Item::Table(table) => json_arm!("table", table),
                });
            }
            serde_json::to_writer_pretty(writer, &seq).unwrap();
        }
    }

    Ok(())
}
