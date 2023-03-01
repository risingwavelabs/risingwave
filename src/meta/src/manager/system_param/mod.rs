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

pub mod model;

use std::ops::DerefMut;
use std::sync::Arc;

use risingwave_common::system_param::{default, set_system_param};
use risingwave_common::{for_all_undeprecated_params, key_of};
use risingwave_pb::meta::SystemParams;
use tokio::sync::RwLock;

use self::model::SystemParamsModel;
use super::MetaSrvEnv;
use crate::model::{ValTransaction, VarTransaction};
use crate::storage::{MetaStore, Transaction};
use crate::{MetaError, MetaResult};

pub type SystemParamManagerRef<S> = Arc<SystemParamManager<S>>;

pub struct SystemParamManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    params: RwLock<SystemParams>,
}

impl<S: MetaStore> SystemParamManager<S> {
    /// Return error if `init_params` conflict with persisted system params.
    pub async fn new(env: MetaSrvEnv<S>, init_params: SystemParams) -> MetaResult<Self> {
        let meta_store = env.meta_store_ref();
        let persisted = SystemParams::get(meta_store.as_ref()).await?;

        let params = if let Some(persisted) = persisted {
            merge_params(persisted, init_params)
        } else {
            SystemParams::insert(&init_params, meta_store.as_ref()).await?;
            init_params
        };

        Ok(Self {
            env,
            params: RwLock::new(params),
        })
    }

    pub async fn get_params(&self) -> SystemParams {
        self.params.read().await.clone()
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<()> {
        let mut params_guard = self.params.write().await;
        let params = params_guard.deref_mut();
        let mut mem_txn = VarTransaction::new(params);

        set_system_param(mem_txn.deref_mut(), name, value).map_err(MetaError::system_param)?;

        let mut store_txn = Transaction::default();
        mem_txn.apply_to_txn(&mut store_txn)?;
        self.env.meta_store().txn(store_txn).await?;

        mem_txn.commit();

        // Sync params to other managers on the meta node only once, since it's infallible.
        self.env
            .notification_manager()
            .notify_local_subscribers(super::LocalNotification::SystemParamsChange(
                params.clone().into(),
            ))
            .await;

        Ok(())
    }
}

// For each field in `persisted` and `init`
// 1. Some, None: Params not from CLI need not be validated. Use persisted value.
// 2. Some, Some: Check equality and warn if they differ.
// 3. None, Some: The persisted param is accidentally deleted or a new version of RW cluster
// is launched for the first time. Use init value.
// 4. None, None: Same as 3, but the init param is not from CLI. Use default value.
macro_rules! impl_merge_params {
    ($({ $field:ident, $type:ty, $default:expr },)*) => {
        fn merge_params(mut persisted: SystemParams, init: SystemParams) -> SystemParams {
            $(
                match (persisted.$field.as_ref(), init.$field) {
                    (Some(persisted), Some(init)) => {
                        if persisted != &init {
                            tracing::warn!("System parameters \"{:?}\" from CLI and config file ({}) differ from persisted ({})", key_of!($field), init, persisted);
                        }
                    },
                    (None, Some(init)) => persisted.$field = Some(init),
                    (None, None) => { persisted.$field = Some(default::$field()) },
                    _ => {},
                }
            )*
            persisted
        }
    };
}

for_all_undeprecated_params!(impl_merge_params);
