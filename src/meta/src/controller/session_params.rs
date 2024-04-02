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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::session_config::{SessionConfig, SessionConfigError};
use risingwave_meta_model_v2::prelude::SessionParameter;
use risingwave_meta_model_v2::session_parameter;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, TransactionTrait};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;
use tracing::info;

use crate::controller::SqlMetaStore;
use crate::{MetaError, MetaResult};

pub type SessionParamsControllerRef = Arc<SessionParamsController>;

pub struct SessionParamsController {
    db: DatabaseConnection,
    // Cached parameters.
    params: RwLock<SessionConfig>,
}

impl SessionParamsController {
    pub async fn new(
        sql_meta_store: SqlMetaStore,
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

        info!("session parameters: {:?}", init_params);

        let ctl = Self {
            db,
            params: RwLock::new(init_params.clone()),
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
                is_mutable: Set(true),
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

        let Some(param) = SessionParameter::find_by_id(name.to_string())
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
        if let Some(value) = value {
            params_guard.set(name, value, reporter)?
        } else {
            params_guard.reset(name, reporter)?
        }
        let new_param = params_guard.get(name)?;

        let mut param: session_parameter::ActiveModel = param.into();
        param.value = Set(new_param.clone());
        param.update(&self.db).await?;

        Ok(new_param)
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::ColumnTrait;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    #[cfg(not(madsim))]
    async fn test_system_params() {
        use sea_orm::QueryFilter;

        let env = MetaSrvEnv::for_test().await;
        let meta_store = env.sql_meta_store().unwrap();
        let init_params = SessionConfig::default();

        // init system parameter controller as first launch.
        let session_param_ctl =
            SessionParamsController::new(meta_store.clone(), init_params.clone())
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
            is_mutable: Set(true),
            description: Set(None),
        };
        deprecated_param
            .insert(&session_param_ctl.db)
            .await
            .unwrap();

        // init system parameter controller as not first launch.
        let session_param_ctl = SessionParamsController::new(meta_store, init_params.clone())
            .await
            .unwrap();
        // check deprecated params are cleaned up.
        assert!(SessionParameter::find_by_id("deprecated_param".to_string())
            .one(&session_param_ctl.db)
            .await
            .unwrap()
            .is_none());
        // check new params are set.
        let params = session_param_ctl.get_params().await;
        assert_eq!(params.get("rw_implicit_flush").unwrap(), new_params);
        // check db consistency.
        let models = SessionParameter::find()
            .filter(session_parameter::Column::Name.eq("rw_implicit_flush"))
            .one(&session_param_ctl.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(models.value, params.get("rw_implicit_flush").unwrap());
    }
}
