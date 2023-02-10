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

mod model;

use std::sync::Arc;

use risingwave_pb::meta::SystemParams;

use self::model::KvSingletonModel;
use super::MetaSrvEnv;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type SystemParamManagerRef<S> = Arc<SystemParamManager<S>>;

pub struct SystemParamManager<S: MetaStore> {
    _env: MetaSrvEnv<S>,
    params: SystemParams,
}

impl<S: MetaStore> SystemParamManager<S> {
    /// Return error if `init_params` conflict with persisted system params.
    pub async fn new(env: MetaSrvEnv<S>, init_params: SystemParams) -> MetaResult<Self> {
        let meta_store = env.meta_store_ref();
        let persisted = SystemParams::get(meta_store.as_ref()).await?;

        let params = if let Some(persisted) = persisted {
            Self::validate_init_params(&persisted, &init_params);
            persisted
        } else {
            SystemParams::insert(&init_params, meta_store.as_ref()).await?;
            init_params
        };

        Ok(Self { _env: env, params })
    }

    pub fn get_params(&self) -> &SystemParams {
        &self.params
    }

    // Only compare params from CLI.
    fn validate_init_params(persisted: &SystemParams, init: &SystemParams) {
        if persisted.sstable_size_mb != init.sstable_size_mb
            || persisted.block_size_kb != init.block_size_kb
            || persisted.bloom_false_positive != init.bloom_false_positive
            || persisted.state_store != init.state_store
            || persisted.data_directory != init.data_directory
            || persisted.backup_storage_url != init.backup_storage_url
            || persisted.backup_storage_directory != init.backup_storage_directory
        {
            tracing::warn!("System parameters from CLI differ from the persisted")
        }
    }

    // TODO(zhidong): Support modifying fields
}
