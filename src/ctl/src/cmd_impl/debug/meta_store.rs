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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_meta::model::{MetadataModel, Worker};
use risingwave_meta::storage::meta_store::MetaStore;
use risingwave_meta::storage::{EtcdMetaStore, WrappedEtcdClient};

use crate::{DebugCommon, DebugCommonKind};

async fn list<T, S>(client: &S) -> anyhow::Result<Vec<T>>
where
    T: MetadataModel + Send,
    S: MetaStore,
{
    let result = T::list(client).await?;
    Ok(result)
}

pub async fn dump(common: DebugCommon) -> anyhow::Result<()> {
    println!("common {:#?}", common);

    let DebugCommon {
        etcd_endpoints,
        mut kinds,
        ..
    } = common;

    kinds.dedup();

    let client = WrappedEtcdClient::connect(etcd_endpoints, None, false).await?;
    let meta_store = Arc::new(EtcdMetaStore::new(client));
    let snapshot = meta_store.snapshot().await;

    for kind in kinds {
        match kind {
            DebugCommonKind::All => {}
            DebugCommonKind::Worker => {}
            DebugCommonKind::User => {}
            DebugCommonKind::Table => {}
        }
    }

    Ok(())
}
