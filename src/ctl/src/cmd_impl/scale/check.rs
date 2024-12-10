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

use std::collections::{HashMap, HashSet};
use std::process::exit;

use itertools::Itertools;
use risingwave_meta::controller::catalog::CatalogController;
use risingwave_meta::controller::SqlMetaStore;
use risingwave_meta::manager::{MetaOpts, MetaSrvEnv};
use risingwave_meta::{MetaResult, MetaStoreBackend};
use risingwave_meta_model_migration::Migrator;
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
use risingwave_pb::meta::GetClusterInfoResponse;
use thiserror_ext::AsReport;

use crate::common::CtlContext;

pub async fn integrity_check(context: &CtlContext, endpoint: String) -> anyhow::Result<()> {
    let opts = MetaOpts::test(false);

    let sql_meta_store = SqlMetaStore::connect(MetaStoreBackend::Sql {
        endpoint,
        config: Default::default(),
    })
    .await?;

    let env = MetaSrvEnv::new(
        opts,
        risingwave_common::system_param::system_params_for_test(),
        Default::default(),
        sql_meta_store,
    )
    .await?;

    let mgr = CatalogController::new(env).await?;

    match mgr.integrity_check().await {
        Ok(_) => {
            println!("all integrity check passed!");
            exit(0);
        }
        Err(e) => {
            println!("integrity check failed! {:?}", e);
            exit(1);
        }
    }
}
