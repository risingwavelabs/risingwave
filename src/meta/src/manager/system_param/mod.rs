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

use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::system_param::set_system_param;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
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
            Self::validate_init_params(&persisted, &init_params);
            persisted
        } else {
            SystemParams::insert(&init_params, meta_store.as_ref()).await?;
            init_params
        };

        Ok(Self {
            env,
            params: RwLock::new(params),
        })
    }

    pub async fn get_pb_params(&self) -> SystemParams {
        self.params.read().await.clone()
    }

    pub async fn get_params(&self) -> SystemParamsReader {
        self.get_pb_params().await.into()
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

        // Notify worker nodes.
        self.env
            .notification_manager()
            .notify_compute(Operation::Update, Info::SystemParams(params.clone()))
            .await;

        Ok(())
    }

    fn validate_init_params(persisted: &SystemParams, init: &SystemParams) {
        // Only compare params from CLI and config file.
        // TODO: Currently all fields are from CLI/config, but after CLI becomes the only source of
        // `init`, should only compare them
        if persisted != init {
            tracing::warn!("System parameters from CLI and config file differ from the persisted")
        }
    }
}
