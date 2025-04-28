// Copyright 2025 RisingWave Labs
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

use risingwave_common::secret::LocalSecretManager;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common_service::ObserverState;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use risingwave_rpc_client::{ComputeClient, RpcClientPool};

pub struct ComputeObserverNode {
    system_params_manager: LocalSystemParamsManagerRef,
    stream_client_pool: Arc<RpcClientPool<ComputeClient>>,
    batch_client_pool: Arc<RpcClientPool<ComputeClient>>,
}

impl ObserverState for ComputeObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Compute
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        if let Some(info) = resp.info.as_ref() {
            match info.to_owned() {
                Info::SystemParams(p) => self.system_params_manager.try_set_params(p),
                Info::Secret(s) => match resp.operation() {
                    Operation::Add => {
                        LocalSecretManager::global().add_secret(s.id, s.value);
                    }
                    Operation::Delete => {
                        LocalSecretManager::global().remove_secret(s.id);
                    }
                    _ => {
                        panic!("error type notification");
                    }
                },
                Info::Recovery(_) => {
                    self.stream_client_pool.invalidate_all();
                    // Reset batch client pool on recovery is always unnecessary
                    // when serving and streaming have been separated.
                    // It can still be used as a method to manually trigger a reset of the batch client pool.
                    // TODO: invalidate a single batch client on any connection issue.
                    self.batch_client_pool.invalidate_all();
                }
                _ => {
                    panic!("error type notification");
                }
            }
        };
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };
        LocalSecretManager::global().init_secrets(snapshot.secrets);
    }
}

impl ComputeObserverNode {
    pub fn new(
        system_params_manager: LocalSystemParamsManagerRef,
        stream_client_pool: Arc<RpcClientPool<ComputeClient>>,
        batch_client_pool: Arc<RpcClientPool<ComputeClient>>,
    ) -> Self {
        Self {
            system_params_manager,
            stream_client_pool,
            batch_client_pool,
        }
    }
}
