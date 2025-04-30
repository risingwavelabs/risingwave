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

use std::collections::HashMap;

use iceberg::table::Table;
use risingwave_connector::connector_common::IcebergCompactionStat;
use risingwave_connector::sink::SinkParam;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkId};
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_pb::catalog::PbSink;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::MetaResult;
use crate::manager::MetadataManager;

pub struct IcebergCompactionManager {
    iceberg_commits: HashMap<SinkId, usize>,
    metadata_manager: MetadataManager,
    recv: UnboundedReceiver<IcebergCompactionStat>,
}

impl IcebergCompactionManager {
    pub fn new(
        metadata_manager: MetadataManager,
        recv: UnboundedReceiver<IcebergCompactionStat>,
    ) -> Self {
        Self {
            iceberg_commits: HashMap::new(),
            metadata_manager,
            recv,
        }
    }

    pub fn start(mut self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(stat) = self.recv.recv() => {
                        self.record_iceberg_commit(stat.sink_id);
                    },
                    _ = &mut shutdown_rx => {
                        tracing::info!("Iceberg compaction manager is stopped");
                        return;
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    pub fn record_iceberg_commit(&mut self, sink_id: SinkId) {
        let count = self.iceberg_commits.entry(sink_id).or_insert(0);
        *count += 1;
    }

    #[allow(dead_code)]
    pub fn get_max_iceberg_commit_sink_id(&self) -> Option<SinkId> {
        self.iceberg_commits
            .iter()
            .max_by_key(|&(_, count)| count)
            .map(|(sink_id, _)| *sink_id)
    }

    #[allow(dead_code)]
    pub fn clear_iceberg_commits_by_sink_id(&mut self, sink_id: SinkId) {
        self.iceberg_commits.remove(&sink_id);
    }

    pub async fn get_sink_param(&self, sink_id: &SinkId) -> MetaResult<SinkParam> {
        let prost_sink_catalog: PbSink = self
            .metadata_manager
            .catalog_controller
            .get_sink_by_ids(vec![sink_id.sink_id as i32])
            .await?
            .remove(0);
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::try_from_sink_catalog(sink_catalog)?;
        Ok(param)
    }

    #[allow(dead_code)]
    pub async fn load_iceberg_table(&self, sink_id: &SinkId) -> MetaResult<Table> {
        let sink_param = self.get_sink_param(sink_id).await?;
        let iceberg_config = IcebergConfig::from_btreemap(sink_param.properties.clone())?;
        let table = iceberg_config.load_table().await?;
        Ok(table)
    }
}
