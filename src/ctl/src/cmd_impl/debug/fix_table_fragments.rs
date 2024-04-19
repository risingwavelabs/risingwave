// Copyright 2024 RisingWave Labs
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

use etcd_client::ConnectOptions;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta::model::{MetadataModel, TableFragments};
use risingwave_meta::storage::{EtcdMetaStore, WrappedEtcdClient};
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::DebugCommon;

pub async fn fix_table_fragments(
    common: DebugCommon,
    table_id: u32,
    dirty_fragment_ids: Vec<u32>,
) -> anyhow::Result<()> {
    let DebugCommon {
        etcd_endpoints,
        etcd_username,
        etcd_password,
        enable_etcd_auth,
        ..
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

    let meta_store = EtcdMetaStore::new(client);

    let mut table_fragments = TableFragments::select(&meta_store, &table_id)
        .await?
        .expect("table fragments not found");

    for fragment in table_fragments.fragments.values_mut() {
        fragment
            .upstream_fragment_ids
            .retain(|id| !dirty_fragment_ids.contains(id));
        for actor in &mut fragment.actors {
            visit_stream_node_cont(actor.nodes.as_mut().unwrap(), |node| {
                if let Some(NodeBody::Union(_)) = node.node_body {
                    node.input.retain_mut(|input| {
                        if let Some(NodeBody::Merge(merge_node)) = &mut input.node_body
                            && dirty_fragment_ids.contains(&merge_node.upstream_fragment_id)
                        {
                            false
                        } else {
                            true
                        }
                    });
                }
                true
            })
        }
    }

    table_fragments.insert(&meta_store).await?;
    Ok(())
}
