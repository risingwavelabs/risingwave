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

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::anyhow;
use async_trait::async_trait;
use fail::fail_point;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::change_log::build_table_change_log_delta;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    HummockContextId, HummockEpoch, HummockVersionId, LocalSstableInfo, SstObjectIdRange,
    SyncResult,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::{TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_request::{Event, ReportTask};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    compact_task, PbHummockVersion, SubscribeCompactionEventRequest,
    SubscribeCompactionEventResponse,
};
use risingwave_rpc_client::error::{Result, RpcError};
use risingwave_rpc_client::{
    CompactionEventItem, HummockMetaClient, HummockMetaClientChangeLogInfo,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::hummock::compaction::selector::{
    default_compaction_selector, CompactionSelector, SpaceReclaimCompactionSelector,
};
use crate::hummock::error::Error;
use crate::hummock::{CommitEpochInfo, HummockManager, NewTableFragmentInfo};

pub struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager>,
    context_id: HummockContextId,
    compact_context_id: AtomicU32,
    // used for hummock replay to avoid collision with existing sst files
    sst_offset: u64,
}

impl MockHummockMetaClient {
    pub fn new(
        hummock_manager: Arc<HummockManager>,
        context_id: HummockContextId,
    ) -> MockHummockMetaClient {
        MockHummockMetaClient {
            hummock_manager,
            context_id,
            compact_context_id: AtomicU32::new(context_id),
            sst_offset: 0,
        }
    }

    pub fn with_sst_offset(
        hummock_manager: Arc<HummockManager>,
        context_id: HummockContextId,
        sst_offset: u64,
    ) -> Self {
        Self {
            hummock_manager,
            context_id,
            compact_context_id: AtomicU32::new(context_id),
            sst_offset,
        }
    }

    pub async fn get_compact_task(&self) -> Option<CompactTask> {
        self.hummock_manager
            .get_compact_task(
                StaticCompactionGroupId::StateDefault.into(),
                &mut default_compaction_selector(),
            )
            .await
            .unwrap_or(None)
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }
}

fn mock_err(error: super::error::Error) -> RpcError {
    anyhow!(error).context("mock error").into()
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        self.hummock_manager
            .unpin_version_before(self.context_id, unpin_version_before)
            .await
            .map_err(mock_err)
    }

    async fn get_current_version(&self) -> Result<HummockVersion> {
        Ok(self.hummock_manager.get_current_version().await)
    }

    async fn get_new_sst_ids(&self, number: u32) -> Result<SstObjectIdRange> {
        fail_point!("get_new_sst_ids_err", |_| Err(anyhow!(
            "failpoint get_new_sst_ids_err"
        )
        .into()));
        self.hummock_manager
            .get_new_sst_ids(number)
            .await
            .map_err(mock_err)
            .map(|range| SstObjectIdRange {
                start_id: range.start_id + self.sst_offset,
                end_id: range.end_id + self.sst_offset,
            })
    }

    async fn commit_epoch_with_change_log(
        &self,
        epoch: HummockEpoch,
        sync_result: SyncResult,
        change_log_info: Option<HummockMetaClientChangeLogInfo>,
    ) -> Result<()> {
        let version: HummockVersion = self.hummock_manager.get_current_version().await;
        let table_ids = version
            .state_table_info
            .info()
            .keys()
            .map(|table_id| table_id.table_id)
            .collect::<BTreeSet<_>>();

        let commit_table_ids = sync_result
            .uncommitted_ssts
            .iter()
            .flat_map(|sstable| sstable.sst_info.table_ids.clone())
            .chain({
                sync_result
                    .old_value_ssts
                    .iter()
                    .flat_map(|sstable| sstable.sst_info.table_ids.clone())
            })
            .chain(
                sync_result
                    .table_watermarks
                    .keys()
                    .map(|table_id| table_id.table_id),
            )
            .chain(table_ids.iter().cloned())
            .collect::<BTreeSet<_>>();

        let new_table_fragment_infos = if commit_table_ids
            .iter()
            .all(|table_id| table_ids.contains(table_id))
        {
            vec![]
        } else {
            vec![NewTableFragmentInfo {
                table_ids: commit_table_ids
                    .iter()
                    .cloned()
                    .map(TableId::from)
                    .collect(),
            }]
        };

        let sst_to_context = sync_result
            .uncommitted_ssts
            .iter()
            .map(|LocalSstableInfo { sst_info, .. }| (sst_info.object_id, self.context_id))
            .collect();
        let new_table_watermark = sync_result.table_watermarks;
        let table_change_log = match change_log_info {
            Some(epochs) => {
                assert_eq!(*epochs.last().expect("non-empty"), epoch);
                build_table_change_log_delta(
                    sync_result
                        .old_value_ssts
                        .into_iter()
                        .map(|sst| sst.sst_info),
                    sync_result.uncommitted_ssts.iter().map(|sst| &sst.sst_info),
                    &epochs,
                    commit_table_ids.iter().map(|&table_id| (table_id, 0)),
                )
            }
            None => Default::default(),
        };

        self.hummock_manager
            .commit_epoch(CommitEpochInfo {
                sstables: sync_result.uncommitted_ssts,
                new_table_watermarks: new_table_watermark,
                sst_to_context,
                new_table_fragment_infos,
                change_log_delta: table_change_log,
                tables_to_commit: commit_table_ids
                    .iter()
                    .cloned()
                    .map(|table_id| (TableId::new(table_id), epoch))
                    .collect(),
            })
            .await
            .map_err(mock_err)?;
        Ok(())
    }

    async fn trigger_manual_compaction(
        &self,
        _compaction_group_id: u64,
        _table_id: u32,
        _level: u32,
        _sst_ids: Vec<u64>,
    ) -> Result<()> {
        todo!()
    }

    async fn trigger_full_gc(
        &self,
        _sst_retention_time_sec: u64,
        _prefix: Option<String>,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn subscribe_compaction_event(
        &self,
    ) -> Result<(
        UnboundedSender<SubscribeCompactionEventRequest>,
        BoxStream<'static, CompactionEventItem>,
    )> {
        let context_id = self
            .hummock_manager
            .metadata_manager()
            .add_worker_node(
                WorkerType::Compactor,
                HostAddress {
                    host: "compactor".to_owned(),
                    port: 0,
                },
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        let _compactor_rx = self
            .hummock_manager
            .compactor_manager_ref_for_test()
            .add_compactor(context_id as _);

        let (request_sender, mut request_receiver) =
            unbounded_channel::<SubscribeCompactionEventRequest>();

        self.compact_context_id
            .store(context_id as _, Ordering::Release);

        let (task_tx, task_rx) = tokio::sync::mpsc::unbounded_channel();

        let hummock_manager_compact = self.hummock_manager.clone();
        let mut join_handle_vec = vec![];

        let handle = tokio::spawn(async move {
            loop {
                let group_and_type = hummock_manager_compact
                    .auto_pick_compaction_group_and_type()
                    .await;

                if group_and_type.is_none() {
                    break;
                }

                let (group, task_type) = group_and_type.unwrap();

                if let TaskType::Ttl = task_type {
                    match hummock_manager_compact
                        .metadata_manager_ref()
                        .get_all_table_options()
                        .await
                        .map_err(|err| Error::MetaStore(err.into()))
                    {
                        Ok(table_options) => {
                            hummock_manager_compact.update_table_id_to_table_option(table_options);
                        }
                        Err(e) => {
                            tracing::error!(error = %e.as_report(), "get_all_table_options fail");
                        }
                    }
                }

                let mut selector: Box<dyn CompactionSelector> = match task_type {
                    compact_task::TaskType::Dynamic => default_compaction_selector(),
                    compact_task::TaskType::SpaceReclaim => {
                        Box::<SpaceReclaimCompactionSelector>::default()
                    }

                    _ => panic!("Error type when mock_hummock_meta_client subscribe_compact_tasks"),
                };
                if let Some(task) = hummock_manager_compact
                    .get_compact_task(group, &mut selector)
                    .await
                    .unwrap()
                {
                    let resp = SubscribeCompactionEventResponse {
                        event: Some(ResponseEvent::CompactTask(task.into())),
                        create_at: SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64,
                    };

                    let _ = task_tx.send(Ok(resp));
                }
            }
        });

        join_handle_vec.push(handle);

        let hummock_manager_compact = self.hummock_manager.clone();
        let report_handle = tokio::spawn(async move {
            tracing::info!("report_handle start");
            loop {
                if let Some(item) = request_receiver.recv().await {
                    if let Event::ReportTask(ReportTask {
                        task_id,
                        task_status,
                        sorted_output_ssts,
                        table_stats_change,
                        object_timestamps,
                    }) = item.event.unwrap()
                    {
                        if let Err(e) = hummock_manager_compact
                            .report_compact_task(
                                task_id,
                                TaskStatus::try_from(task_status).unwrap(),
                                sorted_output_ssts
                                    .into_iter()
                                    .map(SstableInfo::from)
                                    .collect_vec(),
                                Some(table_stats_change),
                                object_timestamps,
                            )
                            .await
                        {
                            tracing::error!(error = %e.as_report(), "report compact_tack fail");
                        }
                    }
                }
            }
        });

        join_handle_vec.push(report_handle);

        Ok((
            request_sender,
            Box::pin(CompactionEventItemStream {
                inner: UnboundedReceiverStream::new(task_rx),
                _handle: join_handle_vec,
            }),
        ))
    }

    async fn get_version_by_epoch(
        &self,
        _epoch: HummockEpoch,
        _table_id: u32,
    ) -> Result<PbHummockVersion> {
        unimplemented!()
    }
}

impl MockHummockMetaClient {
    pub fn hummock_manager_ref(&self) -> Arc<HummockManager> {
        self.hummock_manager.clone()
    }
}

pub struct CompactionEventItemStream {
    inner: UnboundedReceiverStream<CompactionEventItem>,
    _handle: Vec<JoinHandle<()>>,
}

impl Drop for CompactionEventItemStream {
    fn drop(&mut self) {
        self.inner.close();
    }
}

impl Stream for CompactionEventItemStream {
    type Item = CompactionEventItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}
