// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::Registry;

use super::*;
use crate::controller::catalog::CatalogController;
use crate::controller::cluster::ClusterController;
use crate::hummock::{CompactorManager, HummockManager, IcebergCompactorManager};
use crate::manager::MetaOpts;
use crate::manager::iceberg_pk_index_sink::IcebergPkIndexSinkManager;

pub(crate) async fn test_managers() -> (Arc<IcebergCompactionManager>, IcebergPkIndexSinkManager) {
    let mut opts = MetaOpts::test(false);
    opts.iceberg_compaction_report_timeout_sec = 30 * 60;
    let env = MetaSrvEnv::for_test_opts(opts, |_| ()).await;
    let cluster_ctl = Arc::new(
        ClusterController::new(env.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
    );
    let catalog_ctl = Arc::new(CatalogController::new(env.clone()).await.unwrap());
    let (stat_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let metadata_manager = MetadataManager::new(cluster_ctl, catalog_ctl.clone());
    let metrics = Arc::new(MetaMetrics::for_test(&Registry::new()));
    let hummock_manager = HummockManager::new(
        env.clone(),
        metadata_manager.clone(),
        metrics.clone(),
        Arc::new(CompactorManager::for_test()),
        tokio::sync::mpsc::unbounded_channel().0,
    )
    .await
    .unwrap();
    let (barrier_scheduler, _) = BarrierScheduler::new_pair(hummock_manager);
    let (compaction, _) = IcebergCompactionManager::build(
        env,
        metadata_manager,
        Arc::new(IcebergCompactorManager::new()),
        metrics,
        barrier_scheduler,
    );
    let coordinator = IcebergPkIndexSinkManager::new(catalog_ctl, compaction.clone(), stat_tx);
    (compaction, coordinator)
}
