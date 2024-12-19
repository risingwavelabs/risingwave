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

use std::process::exit;

use risingwave_meta::controller::catalog::CatalogController;
use sea_orm::TransactionTrait;

pub async fn graph_check(endpoint: String) -> anyhow::Result<()> {
    let conn = sea_orm::Database::connect(sea_orm::ConnectOptions::new(endpoint)).await?;
    let txn = conn.begin().await?;
    match CatalogController::graph_check(&txn).await {
        Ok(_) => {
            println!("integrity check passed!");
            exit(0);
        }
        Err(_) => {
            println!("integrity check failed!");
            exit(1);
        }
    }
}
