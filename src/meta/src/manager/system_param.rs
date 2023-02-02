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

use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::{
    verify_params, CompactorVerifyParams, ComputeNodeVerifyParams, FrontendVerifyParams,
    SystemParams, VerifyParams as ProstVerifyParams,
};

use super::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::{MetaError, MetaResult};

pub type SystemParamManagerRef<S> = Arc<SystemParamManager<S>>;

pub struct SystemParamManager<S: MetaStore> {
    _env: MetaSrvEnv<S>,
    params: SystemParams,
}

impl<S: MetaStore> SystemParamManager<S> {
    /// Return error if `init_params` conflict with persisted system params.
    pub async fn new(env: MetaSrvEnv<S>, init_params: SystemParams) -> MetaResult<Self> {
        let meta_store = env.meta_store_ref();
        let mut existing_params = SystemParams::list(meta_store.as_ref()).await?;

        // System params are not versioned.
        debug_assert!(existing_params.len() <= 1);

        let params = if let Some(existing_params) = existing_params.pop() {
            if init_params != existing_params {
                return Err(MetaError::system_param(
                    "System parameters from configuration differ from the persisted",
                ));
            }
            existing_params
        } else {
            SystemParams::insert(&init_params, meta_store.as_ref()).await?;
            init_params
        };

        Ok(Self { _env: env, params })
    }

    /// Verify a the params for the given `worker_type`. If all fields match, return the
    /// complete [`SystemParams`]. If `params_to_verify.params` is `None`, the verification will
    /// pass directly.
    ///
    /// Note that the verification is just for backward compatibility. It may be removed after we
    /// deprecate system param items from the config file.
    pub fn verify_params(
        &self,
        worker_type: WorkerType,
        params_to_verify: ProstVerifyParams,
    ) -> MetaResult<SystemParams> {
        macro_rules! compare_params {
            ($params:ident, $worker_type:ident, $( { $worker_variant:ident, $persisted:expr } ),*) => {
                match $params {
                    $(verify_params::Params::$worker_variant(c) => {
                        if !matches!($worker_type, WorkerType::$worker_variant) {
                            Err(MetaError::system_param("worker type and config type mismatch"))
                        } else if c == $persisted {
                            Ok(self.params.clone())
                        } else {
                            Err(MetaError::system_param("worker config and cluster config mismatch"))
                        }
                    }),*
                }
            };
        }

        if let Some(p) = params_to_verify.params {
            compare_params!(
                p, worker_type,
                { Frontend, self.frontend_verify_params() },
                { ComputeNode, self.compute_node_verify_params() },
                { Compactor, self.compactor_verify_params() }
            )
        } else {
            Ok(self.params.clone())
        }
    }

    fn frontend_verify_params(&self) -> FrontendVerifyParams {
        FrontendVerifyParams {}
    }

    fn compute_node_verify_params(&self) -> ComputeNodeVerifyParams {
        ComputeNodeVerifyParams {
            barrier_interval_ms: self.params.barrier_interval_ms,
        }
    }

    fn compactor_verify_params(&self) -> CompactorVerifyParams {
        CompactorVerifyParams {}
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::meta::verify_params::Params;

    use super::*;

    #[tokio::test]
    async fn test_manager() {
        let env = MetaSrvEnv::for_test().await;

        let initial_params = SystemParams {
            barrier_interval_ms: 1,
            ..Default::default()
        };
        let params_manager = SystemParamManager::new(env.clone(), initial_params.clone())
            .await
            .unwrap();

        // Should be persisted in meta store.
        assert_eq!(
            SystemParams::list(env.meta_store()).await.unwrap(),
            vec![initial_params.clone()]
        );

        assert_eq!(
            params_manager
                .verify_params(WorkerType::ComputeNode, ProstVerifyParams { params: None })
                .unwrap(),
            initial_params
        );

        let verify_params = ProstVerifyParams {
            params: Some(Params::ComputeNode(ComputeNodeVerifyParams {
                barrier_interval_ms: 1,
            })),
        };
        assert_eq!(
            params_manager
                .verify_params(WorkerType::ComputeNode, verify_params.clone())
                .unwrap(),
            initial_params
        );

        // Mismatching config and worker type.
        assert!(params_manager
            .verify_params(WorkerType::Compactor, verify_params)
            .is_err());

        // Mismatching config field.
        let verify_params = ProstVerifyParams {
            params: Some(Params::ComputeNode(ComputeNodeVerifyParams {
                barrier_interval_ms: 2,
            })),
        };
        assert!(params_manager
            .verify_params(WorkerType::Compactor, verify_params)
            .is_err());

        let wrong_initial_params = SystemParams {
            barrier_interval_ms: 2,
            ..Default::default()
        };

        assert!(
            SystemParamManager::new(env.clone(), wrong_initial_params.clone())
                .await
                .is_err()
        );
    }
}
