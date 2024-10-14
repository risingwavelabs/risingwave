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

use std::time::Duration;

use risingwave_simulation::cluster::{Cluster, KillOpts};
use tokio::time::sleep;

pub(crate) async fn kill_cn_and_wait_recover(cluster: &Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3"], 0)
        .await;
    sleep(Duration::from_secs(10)).await;
}

pub(crate) async fn kill_cn_and_meta_and_wait_recover(cluster: &Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3", "meta-1"], 0)
        .await;
    sleep(Duration::from_secs(10)).await;
}

pub(crate) async fn kill_random_and_wait_recover(cluster: &Cluster) {
    // Kill it again
    for _ in 0..3 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL_FAST).await;
    }
    sleep(Duration::from_secs(10)).await;
}
