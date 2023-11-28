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

use risingwave_common::catalog::TableId;
use risingwave_simulation::cluster::{Cluster, Configuration};

const SET_PARALLELISM: &str = "SET STREAMING_PARALLELISM = 1;";

#[tokio::test]
async fn test_throttle_mv() {
    let mut cluster = Cluster::start(Configuration::for_backfill()).await.unwrap();
    let mut session = cluster.start_session();

    session.run(SET_PARALLELISM).await.unwrap();
    session
        .run("create table t1 (id int, val varchar, primary key(id))") // table_id: 1001
        .await
        .unwrap();
    session
        .run("create materialized view mv1 as select * from t1") // table_id: 1002
        .await
        .unwrap();

    cluster
        .throttle_mv(TableId::from(1002), Some(200))
        .await
        .unwrap();

    cluster
        .throttle_mv(TableId::from(1002), None)
        .await
        .unwrap();
}
