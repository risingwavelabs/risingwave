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
use std::time::Duration;

use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::system_param::set_system_param;
use risingwave_common::{for_all_undeprecated_params, key_of};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SystemParams;
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use self::model::SystemParamsModel;
use super::NotificationManagerRef;
use crate::model::{ValTransaction, VarTransaction};
use crate::storage::{MetaStore, Transaction};
use crate::{MetaError, MetaResult};

pub type SystemParamsManagerRef<S> = Arc<SystemParamsManager<S>>;

pub struct SystemParamsManager<S: MetaStore> {
    meta_store: Arc<S>,
    notification_manager: NotificationManagerRef<S>,
    params: RwLock<SystemParams>,
}

impl<S: MetaStore> SystemParamsManager<S> {
    /// Return error if `init_params` conflict with persisted system params.
    pub async fn new(
        meta_store: Arc<S>,
        notification_manager: NotificationManagerRef<S>,
        init_params: SystemParams,
    ) -> MetaResult<Self> {
        let persisted = SystemParams::get(meta_store.as_ref()).await?;

        let params = if let Some(persisted) = persisted {
            merge_params(persisted, init_params)
        } else {
            init_params
        };

        SystemParams::insert(&params, meta_store.as_ref()).await?;

        Ok(Self {
            meta_store,
            notification_manager,
            params: RwLock::new(params),
        })
    }

    pub async fn get_pb_params(&self) -> SystemParams {
        self.params.read().await.clone()
    }

    pub async fn get_params(&self) -> SystemParamsReader {
        self.params.read().await.clone().into()
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<()> {
        let mut params_guard = self.params.write().await;
        let params = params_guard.deref_mut();
        let mut mem_txn = VarTransaction::new(params);

        set_system_param(mem_txn.deref_mut(), name, value).map_err(MetaError::system_param)?;

        let mut store_txn = Transaction::default();
        mem_txn.apply_to_txn(&mut store_txn)?;
        self.meta_store.txn(store_txn).await?;

        mem_txn.commit();

        // Sync params to other managers on the meta node only once, since it's infallible.
        self.notification_manager
            .notify_local_subscribers(super::LocalNotification::SystemParamsChange(
                params.clone().into(),
            ))
            .await;

        // Sync params to worker nodes.
        self.notify_workers(params).await;

        Ok(())
    }

    // Periodically sync params to worker nodes.
    pub async fn start_params_notifier(
        system_params_manager: Arc<Self>,
    ) -> (JoinHandle<()>, Sender<()>) {
        const NOTIFY_INTERVAL: Duration = Duration::from_millis(5000);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(NOTIFY_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {},
                    _ = &mut shutdown_rx => {
                        tracing::info!("System params notifier is stopped");
                        return;
                    }
                }
                system_params_manager
                    .notify_workers(&*system_params_manager.params.read().await)
                    .await;
            }
        });

        (join_handle, shutdown_tx)
    }

    // Notify workers of parameter change.
    async fn notify_workers(&self, params: &SystemParams) {
        self.notification_manager
            .notify_frontend(Operation::Update, Info::SystemParams(params.clone()))
            .await;
        self.notification_manager
            .notify_compute(Operation::Update, Info::SystemParams(params.clone()))
            .await;
        self.notification_manager
            .notify_compactor(Operation::Update, Info::SystemParams(params.clone()))
            .await;
    }
}

// For each field in `persisted` and `init`
// 1. Some, None: Params not from CLI need not be validated. Use persisted value.
// 2. Some, Some: Check equality and warn if they differ.
// 3. None, Some: A new version of RW cluster is launched for the first time and newly introduced
// params are not set. Use init value.
// 4. None, None: Impossible.
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
                    (None, None) => unreachable!(),
                    _ => {},
                }
            )*
            persisted
        }
    };
}

for_all_undeprecated_params!(impl_merge_params);
