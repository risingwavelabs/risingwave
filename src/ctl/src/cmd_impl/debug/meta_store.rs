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

use std::ops::Deref;
use std::sync::Arc;

use risingwave_meta::model::{MetadataModel, TableFragments, Worker};
use risingwave_meta::storage::meta_store::MetaStore;
use risingwave_meta::storage::{EtcdMetaStore, WrappedEtcdClient};
use risingwave_pb::common::WorkerNode;

use crate::common::CtlContext;

pub async fn dump(endpoints: Vec<String>) -> anyhow::Result<()> {
    let client = WrappedEtcdClient::connect(endpoints, None, false).await?;
    let meta_store = Arc::new(EtcdMetaStore::new(client));
    let workers = Worker::list(meta_store.clone().deref()).await?;

    for worker in workers {
        let s = serde_yaml::to_string(&worker.to_protobuf()).unwrap();
        println!("{}", s);
    }

    let tables = TableFragments::list(meta_store.clone().deref()).await?;

    for table in tables {
        let s = serde_yaml::to_string(&table.to_protobuf()).unwrap();
        println!("{}", s);
    }




    Ok(())
}
