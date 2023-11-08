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
use std::time::Duration;

use anyhow::anyhow;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::system_param::{
    check_missing_params, derive_missing_fields, set_system_param,
};
use risingwave_common::{for_all_params, key_of};
use risingwave_meta_model_v2::prelude::SystemParameter;
use risingwave_meta_model_v2::system_parameter;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::PbSystemParams;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, TransactionTrait};
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::info;

use crate::controller::SqlMetaStore;
use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::{MetaError, MetaResult};

pub type SystemParamsControllerRef = Arc<SystemParamsController>;

pub struct SystemParamsController {
    db: DatabaseConnection,
    // Notify workers and local subscribers of parameter change.
    notification_manager: NotificationManagerRef,
    // Cached parameters.
    params: RwLock<PbSystemParams>,
}

/// Derive system params from db models.
macro_rules! impl_system_params_from_db {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr },)*) => {
        /// Try to deserialize deprecated fields as well.
        /// Warn if there are unrecognized fields.
        pub fn system_params_from_db(mut models: Vec<system_parameter::Model>) -> MetaResult<PbSystemParams> {
            let mut params = PbSystemParams::default();
            models.retain(|model| {
                match model.name.as_str() {
                    $(
                        key_of!($field) => {
                            params.$field = Some(model.value.parse::<$type>().unwrap());
                            false
                        }
                    )*
                    _ => true,
                }
            });
            derive_missing_fields(&mut params);
            if !models.is_empty() {
                let unrecognized_params = models.into_iter().map(|model| model.name).collect::<Vec<_>>();
                tracing::warn!("unrecognized system params {:?}", unrecognized_params);
            }
            Ok(params)
        }
    };
}

/// Derive serialization to db models.
macro_rules! impl_system_params_to_models {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr },)*) => {
        #[allow(clippy::vec_init_then_push)]
        pub fn system_params_to_model(params: &PbSystemParams) -> MetaResult<Vec<system_parameter::ActiveModel>> {
            check_missing_params(params).map_err(|e| anyhow!(e))?;
            let mut models = Vec::new();
            $(
                let value = params.$field.as_ref().unwrap().to_string();
                models.push(system_parameter::ActiveModel {
                    name: Set(key_of!($field).to_string()),
                    value: Set(value),
                    is_mutable: Set($is_mutable),
                    description: Set(None),
                });
            )*
            Ok(models)
       }
    };
}

// For each field in `persisted` and `init`
// 1. Some, None: The persisted field is deprecated, so just ignore it.
// 2. Some, Some: Check equality and warn if they differ.
// 3. None, Some: A new version of RW cluster is launched for the first time and newly introduced
// params are not set. Use init value.
// 4. None, None: A new version of RW cluster is launched for the first time and newly introduced
// params are not set. The new field is not initialized either, just leave it as `None`.
macro_rules! impl_merge_params {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr },)*) => {
        fn merge_params(mut persisted: PbSystemParams, init: PbSystemParams) -> PbSystemParams {
            $(
                match (persisted.$field.as_ref(), init.$field) {
                    (Some(persisted), Some(init)) => {
                        if persisted != &init {
                            tracing::warn!(
                                "The initializing value of \"{:?}\" ({}) differ from persisted ({}), using persisted value",
                                key_of!($field),
                                init,
                                persisted
                            );
                        }
                    },
                    (None, Some(init)) => persisted.$field = Some(init),
                    _ => {},
                }
            )*
            persisted
        }
    };
}

for_all_params!(impl_system_params_from_db);
for_all_params!(impl_merge_params);
for_all_params!(impl_system_params_to_models);

impl SystemParamsController {
    pub async fn new(
        sql_meta_store: SqlMetaStore,
        notification_manager: NotificationManagerRef,
        init_params: PbSystemParams,
    ) -> MetaResult<Self> {
        let db = sql_meta_store.conn;
        let params = SystemParameter::find().all(&db).await?;
        let params = merge_params(system_params_from_db(params)?, init_params);

        info!("system parameters: {:?}", params);
        check_missing_params(&params).map_err(|e| anyhow!(e))?;

        let ctl = Self {
            db,
            notification_manager,
            params: RwLock::new(params),
        };
        // flush to db.
        ctl.flush_params().await?;

        Ok(ctl)
    }

    pub async fn get_pb_params(&self) -> PbSystemParams {
        self.params.read().await.clone()
    }

    pub async fn get_params(&self) -> SystemParamsReader {
        self.params.read().await.clone().into()
    }

    async fn flush_params(&self) -> MetaResult<()> {
        let params = self.params.read().await;
        let models = system_params_to_model(&params)?;
        let txn = self.db.begin().await?;
        // delete all params first and then insert all params. It follows the same logic
        // as the old code, we'd better change it to another way later to keep consistency.
        SystemParameter::delete_many().exec(&txn).await?;

        for model in models {
            model.insert(&txn).await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<PbSystemParams> {
        let mut params_guard = self.params.write().await;

        let Some(param) = SystemParameter::find_by_id(name.to_string())
            .one(&self.db)
            .await?
        else {
            return Err(MetaError::system_param(format!(
                "unrecognized system parameter {}",
                name
            )));
        };
        let mut params = params_guard.clone();
        let mut param: system_parameter::ActiveModel = param.into();
        param.value =
            Set(set_system_param(&mut params, name, value).map_err(MetaError::system_param)?);
        param.update(&self.db).await?;
        *params_guard = params.clone();

        // Sync params to other managers on the meta node only once, since it's infallible.
        self.notification_manager
            .notify_local_subscribers(LocalNotification::SystemParamsChange(params.clone().into()))
            .await;

        // Sync params to worker nodes.
        self.notify_workers(&params).await;

        Ok(params)
    }

    // Periodically sync params to worker nodes.
    pub fn start_params_notifier(
        system_params_controller: Arc<Self>,
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
                system_params_controller
                    .notify_workers(&*system_params_controller.params.read().await)
                    .await;
            }
        });

        (join_handle, shutdown_tx)
    }

    // Notify workers of parameter change.
    async fn notify_workers(&self, params: &PbSystemParams) {
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

#[cfg(test)]
mod tests {
    use risingwave_common::system_param::system_params_for_test;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    #[cfg(not(madsim))]
    async fn test_system_params() {
        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.sql_meta_store().unwrap();
        let init_params = system_params_for_test();

        // init system parameter controller as first launch.
        let system_param_ctl = SystemParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            init_params.clone(),
        )
        .await
        .unwrap();
        let params = system_param_ctl.get_pb_params().await;
        assert_eq!(params, system_params_for_test());

        // set parameter.
        let new_params = system_param_ctl
            .set_param("pause_on_next_bootstrap", Some("true".into()))
            .await
            .unwrap();

        // insert deprecated params.
        let deprecated_param = system_parameter::ActiveModel {
            name: Set("deprecated_param".into()),
            value: Set("foo".into()),
            is_mutable: Set(true),
            description: Set(None),
        };
        deprecated_param.insert(&system_param_ctl.db).await.unwrap();

        // init system parameter controller as not first launch.
        let system_param_ctl = SystemParamsController::new(
            meta_store,
            env.notification_manager_ref(),
            init_params.clone(),
        )
        .await
        .unwrap();
        // check deprecated params are cleaned up.
        assert!(SystemParameter::find_by_id("deprecated_param".to_string())
            .one(&system_param_ctl.db)
            .await
            .unwrap()
            .is_none());
        // check new params are set.
        let params = system_param_ctl.get_pb_params().await;
        assert_eq!(params, new_params);
        // check db consistency.
        let models = SystemParameter::find()
            .all(&system_param_ctl.db)
            .await
            .unwrap();
        let db_params = system_params_from_db(models).unwrap();
        assert_eq!(db_params, new_params);
    }
}
