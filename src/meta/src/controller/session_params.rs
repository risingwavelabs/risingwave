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

use itertools::Itertools;
use risingwave_common::session_config::{SessionConfig, SessionConfigError};
use risingwave_meta_model::prelude::SessionParameter;
use risingwave_meta_model::session_parameter;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SetSessionParamRequest;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, TransactionTrait};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;
use tracing::info;

use crate::controller::MetaStore;
use crate::manager::NotificationManagerRef;
use crate::{MetaError, MetaResult};

pub type SessionParamsControllerRef = Arc<SessionParamsController>;

/// Manages the global default session params on meta.
/// Note that the session params in each session will be initialized from the default value here.
pub struct SessionParamsController {
    db: DatabaseConnection,
    // Cached parameters.
    params: RwLock<SessionConfig>,
    notification_manager: NotificationManagerRef,
}

impl SessionParamsController {
    pub async fn new(
        sql_meta_store: MetaStore,
        notification_manager: NotificationManagerRef,
        mut init_params: SessionConfig,
    ) -> MetaResult<Self> {
        let db = sql_meta_store.conn;
        let params = SessionParameter::find().all(&db).await?;
        for param in params {
            if let Err(e) = init_params.set(&param.name, param.value, &mut ()) {
                match e {
                    SessionConfigError::InvalidValue { .. } => {
                        tracing::error!(error = %e.as_report(), "failed to set parameter from meta database, using default value {}", init_params.get(&param.name)?)
                    }
                    SessionConfigError::UnrecognizedEntry(_) => {
                        tracing::error!(error = %e.as_report(), "failed to set parameter from meta database")
                    }
                }
            }
        }

        info!(?init_params, "session parameters");

        let ctl = Self {
            db,
            params: RwLock::new(init_params.clone()),
            notification_manager,
        };
        // flush to db.
        ctl.flush_params().await?;

        Ok(ctl)
    }

    pub async fn get_params(&self) -> SessionConfig {
        self.params.read().await.clone()
    }

    async fn flush_params(&self) -> MetaResult<()> {
        let params = self.params.read().await.list_all();
        let models = params
            .into_iter()
            .map(|param| session_parameter::ActiveModel {
                name: Set(param.name),
                value: Set(param.setting),
                description: Set(Some(param.description)),
            })
            .collect_vec();
        let txn = self.db.begin().await?;
        // delete all params first and then insert all params. It follows the same logic
        // as the old code, we'd better change it to another way later to keep consistency.
        SessionParameter::delete_many().exec(&txn).await?;
        SessionParameter::insert_many(models).exec(&txn).await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<String> {
        let mut params_guard = self.params.write().await;
        let name = SessionConfig::alias_to_entry_name(name);
        let Some(param) = SessionParameter::find_by_id(name.clone())
            .one(&self.db)
            .await?
        else {
            return Err(MetaError::system_params(format!(
                "unrecognized session parameter {:?}",
                name
            )));
        };
        // FIXME: use a real reporter
        let reporter = &mut ();
        let new_param = if let Some(value) = value {
            params_guard.set(&name, value, reporter)?
        } else {
            params_guard.reset(&name, reporter)?
        };

        let mut param: session_parameter::ActiveModel = param.into();
        param.value = Set(new_param.clone());
        param.update(&self.db).await?;

        self.notify_workers(name.clone(), new_param.clone());

        Ok(new_param)
    }

    pub fn notify_workers(&self, name: String, value: String) {
        self.notification_manager.notify_frontend_without_version(
            Operation::Update,
            Info::SessionParam(SetSessionParamRequest {
                param: name,
                value: Some(value),
            }),
        );
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::ColumnTrait;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_session_params() {
        use sea_orm::QueryFilter;

        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.meta_store_ref();
        let init_params = SessionConfig::default();

        // init system parameter controller as first launch.
        let session_param_ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            init_params.clone(),
        )
        .await
        .unwrap();
        let params = session_param_ctl.get_params().await;
        assert_eq!(params, init_params);

        // set parameter.
        let new_params = session_param_ctl
            .set_param("rw_implicit_flush", Some("true".into()))
            .await
            .unwrap();

        // insert deprecated params.
        let deprecated_param = session_parameter::ActiveModel {
            name: Set("deprecated_param".into()),
            value: Set("foo".into()),
            description: Set(None),
        };
        deprecated_param
            .insert(&session_param_ctl.db)
            .await
            .unwrap();

        // init system parameter controller as not first launch.
        let session_param_ctl = SessionParamsController::new(
            meta_store.clone(),
            env.notification_manager_ref(),
            init_params.clone(),
        )
        .await
        .unwrap();
        // check deprecated params are cleaned up.
        assert!(SessionParameter::find_by_id("deprecated_param".to_owned())
            .one(&session_param_ctl.db)
            .await
            .unwrap()
            .is_none());
        // check new params are set.
        let params = session_param_ctl.get_params().await;
        assert_eq!(params.get("rw_implicit_flush").unwrap(), new_params);
        assert_eq!(
            params.get("rw_implicit_flush").unwrap(),
            params.get("implicit_flush").unwrap()
        );
        // check db consistency.
        // rw_implicit_flush is alias to implicit_flush <https://github.com/risingwavelabs/risingwave/pull/18769>
        let models = SessionParameter::find()
            .filter(session_parameter::Column::Name.eq("rw_implicit_flush"))
            .one(&session_param_ctl.db)
            .await
            .unwrap();
        assert!(models.is_none());
        let models = SessionParameter::find()
            .filter(session_parameter::Column::Name.eq("implicit_flush"))
            .one(&session_param_ctl.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models.value, params.get("rw_implicit_flush").unwrap());
    }
}
