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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use risingwave_meta::model::{MetadataModel, TableFragments, Worker};
use risingwave_meta::storage::meta_store::MetaStore;
use risingwave_meta::storage::{EtcdMetaStore, Snapshot, WrappedEtcdClient};
use risingwave_pb::user::UserInfo;
use serde_yaml::Value;

use crate::{DebugCommon, DebugCommonKind};

macro_rules! fetch_items {
    ($t:ty, $kind:expr, $snapshot:expr) => {{
        let result = <$t>::list_at_snapshot::<EtcdMetaStore>($snapshot).await?;
        result.into_iter().map(|item| {
            let mut mapping = serde_yaml::Mapping::new();

            mapping.insert(
                Value::String("kind".to_string()),
                Value::String($kind.to_string()),
            );

            let value = serde_yaml::to_value(item.to_protobuf()).unwrap();

            mapping.insert(Value::String("item".to_string()), value);

            Value::Mapping(mapping)
        })
    }};
}

pub async fn dump(common: DebugCommon) -> anyhow::Result<()> {
    let DebugCommon {
        etcd_endpoints,
        kinds,
        ..
    } = common;

    let client = WrappedEtcdClient::connect(etcd_endpoints, None, false).await?;
    let meta_store = Arc::new(EtcdMetaStore::new(client));
    let snapshot = meta_store.snapshot().await;
    let kinds: BTreeSet<_> = kinds.into_iter().collect();

    let mut total = serde_yaml::Sequence::new();

    for kind in kinds {
        match kind {
            DebugCommonKind::Worker => total.extend(fetch_items!(Worker, "worker", &snapshot)),
            DebugCommonKind::User => total.extend(fetch_items!(UserInfo, "user", &snapshot)),
            DebugCommonKind::Table => {
                total.extend(fetch_items!(TableFragments, "table", &snapshot))
            }
        };
    }

    serde_yaml::to_writer(std::io::stdout(), &total).unwrap();

    Ok(())
}

