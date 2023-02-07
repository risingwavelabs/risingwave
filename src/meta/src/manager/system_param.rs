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

use risingwave_pb::meta::SystemParams;

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

    pub fn get_params(&self) -> &SystemParams {
        &self.params
    }

    // TODO: modify fields
}
