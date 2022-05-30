// Copyright 2022 Singularity Data
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

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::future::{try_join_all, BoxFuture};
use futures::{stream, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_common::util::compress::decompress_data;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::key::{get_epoch, Epoch, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{HummockSSTableId, VersionedComparator};
use risingwave_pb::hummock::{
    CompactTask, SstableInfo, SubscribeCompactTasksResponse, VNodeBitmap, VacuumTask,
};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::group_builder::KeyValueGroupingImpl::VirtualNode;
use super::group_builder::{GroupedSstableBuilder, VirtualNodeGrouping};
use super::iterator::{BoxedForwardHummockIterator, ConcatIterator, MergeIterator};
use super::{HummockResult, SSTableBuilder, SSTableIterator, SSTableIteratorType, Sstable};
use crate::hummock::compaction_executor::CompactionExecutor;
use crate::hummock::iterator::ReadOptions;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::can_concat;
use crate::hummock::vacuum::Vacuum;
use crate::hummock::{CachePolicy, HummockError};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub type SstableIdGenerator =
    Arc<dyn Fn() -> BoxFuture<'static, HummockResult<HummockSSTableId>> + Send + Sync>;

pub fn get_remote_sstable_id_generator(
    meta_client: Arc<dyn HummockMetaClient>,
) -> SstableIdGenerator {
    Arc::new(move || {
        let meta_client = meta_client.clone();
        async move {
            meta_client
                .get_new_table_id()
                .await
                .map_err(HummockError::meta_error)
        }
        .boxed()
    })
}

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// SSTable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub sstable_id_generator: SstableIdGenerator,

    pub compaction_executor: Option<Arc<CompactionExecutor>>,
}

#[derive(Clone)]
/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: Arc<CompactorContext>,

    /// A compaction task received from the hummock manager.
    /// When it's local compaction from memory, it uses a locally
    /// constructed compaction task.
    compact_task: CompactTask,
}

pub type CompactOutput = (usize, Vec<(Sstable, Vec<VNodeBitmap>)>);

impl Compactor {
    /// Create a new compactor.
    pub fn new(context: Arc<CompactorContext>, compact_task: CompactTask) -> Self {
        Self {
            context,
            compact_task,
        }
    }

    /// For compaction from shared buffer to level 0, this is the only function gets called.
    pub async fn compact_shared_buffer(
        context: Arc<CompactorContext>,
        payload: &UploadTaskPayload,
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Vec<(Sstable, Vec<VNodeBitmap>)>> {
        let mut start_user_keys = payload.iter().map(|m| m.start_user_key()).collect_vec();
        start_user_keys.sort();
        start_user_keys.dedup();
        let mut splits = Vec::with_capacity(start_user_keys.len());
        splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
        let mut key_split_append = |key_before_last: &Bytes| {
            splits.last_mut().unwrap().right = key_before_last.clone();
            splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
        };
        if start_user_keys.len() > 1 {
            let split_num = context.options.share_buffers_sync_parallelism as usize;
            let buffer_per_split = start_user_keys.len() / split_num;
            for i in 1..split_num {
                key_split_append(
                    &FullKey::from_user_key_slice(
                        start_user_keys[i * buffer_per_split],
                        Epoch::MAX,
                    )
                    .into_inner()
                    .into(),
                );
            }
        }

        // Local memory compaction looks at all key ranges.
        let compact_task = CompactTask {
            input_ssts: vec![],
            splits: splits.into_iter().map(|v| v.into()).collect_vec(),
            watermark: u64::MAX,
            sorted_output_ssts: vec![],
            task_id: 0,
            target_level: 0,
            is_target_ultimate_and_leveling: false,
            metrics: None,
            task_status: false,
            // TODO: get compaction group info from meta
            prefix_pairs: vec![],
            // VNode mappings are not required when compacting shared buffer to L0
            vnode_mappings: vec![],
        };

        let parallelism = compact_task.splits.len();
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let compactor = Compactor::new(context, compact_task.clone());

        let vnode2unit: Arc<HashMap<u32, Vec<u32>>> = Arc::new(HashMap::new());

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let iter = {
                let iters = payload.iter().map(|m| {
                    Box::new(m.clone().into_forward_iter()) as BoxedForwardHummockIterator
                });
                Box::new(MergeIterator::new(iters, stats.clone()))
            };
            let vnode2unit = vnode2unit.clone();
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();
            let split_task = async move {
                compactor
                    .compact_key_range(split_index, iter, vnode2unit)
                    .await
            };
            let rx = Compactor::request_execution(compaction_executor, split_task)?;
            compaction_futures.push(rx);
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        let mut err = None;
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!("Shared Buffer Compaction failed with error: {:#?}", e);
                    err = Some(e);
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        if compact_success {
            let mut level0 = Vec::with_capacity(parallelism);

            for (_, sst) in output_ssts {
                for (table, _) in &sst {
                    compactor
                        .context
                        .stats
                        .write_build_l0_bytes
                        .inc_by(table.meta.estimated_size as u64);
                }
                level0.extend(sst);
            }

            Ok(level0)
        } else {
            Err(err.unwrap())
        }
    }

    /// Tries to schedule on `compaction_executor` if `compaction_executor` is not None.
    ///
    /// Tries to schedule on current runtime if `compaction_executor` is None.
    fn request_execution(
        compaction_executor: Option<Arc<CompactionExecutor>>,
        split_task: impl Future<Output = HummockResult<CompactOutput>> + Send + 'static,
    ) -> HummockResult<JoinHandle<HummockResult<CompactOutput>>> {
        match compaction_executor {
            None => Ok(tokio::spawn(split_task)),
            Some(compaction_executor) => {
                let rx = compaction_executor
                    .send_request(split_task)
                    .map_err(HummockError::compaction_executor)?;
                Ok(tokio::spawn(async move {
                    match rx.await {
                        Ok(result) => result,
                        Err(err) => Err(HummockError::compaction_executor(err)),
                    }
                }))
            }
        }
    }

    /// Handle a compaction task and report its status to hummock manager.
    /// Always return `Ok` and let hummock manager handle errors.
    pub async fn compact(context: Arc<CompactorContext>, mut compact_task: CompactTask) -> bool {
        tracing::info!("Ready to handle compaction task: {}", compact_task.task_id,);
        let mut compaction_read_bytes = compact_task.input_ssts[0]
            .table_infos
            .iter()
            .map(|t| t.file_size)
            .sum();
        if let Some(metrics) = compact_task.metrics.as_mut() {
            if let Some(read) = metrics.read_level_n.as_mut() {
                read.level_idx = compact_task.input_ssts[0].level_idx;
                read.cnt = compact_task.input_ssts[0].table_infos.len() as u64;
                read.size_kb = compaction_read_bytes / 1024;
            }
        }
        if compact_task.input_ssts.len() > 1 {
            let sec_level_read_bytes: u64 = compact_task.input_ssts[1]
                .table_infos
                .iter()
                .map(|t| t.file_size)
                .sum();
            if let Some(metrics) = compact_task.metrics.as_mut() {
                if let Some(read) = metrics.read_level_nplus1.as_mut() {
                    read.level_idx = compact_task.input_ssts[1].level_idx;
                    read.cnt = compact_task.input_ssts[1].table_infos.len() as u64;
                    read.size_kb = sec_level_read_bytes / 1024;
                }
            }
            compaction_read_bytes += sec_level_read_bytes;
        }
        context
            .stats
            .compaction_read_bytes
            .inc_by(compaction_read_bytes);

        let timer = context
            .stats
            .compact_task_duration
            .with_label_values(&[compact_task.input_ssts[0].level_idx.to_string().as_str()])
            .start_timer();

        if !compact_task.vnode_mappings.is_empty() {
            compact_task.splits = vec![risingwave_pb::hummock::KeyRange {
                left: vec![],
                right: vec![],
                inf: false,
            }];
        };

        // Number of splits (key ranges) is equal to number of compaction tasks
        let parallelism = compact_task.splits.len();
        assert_ne!(parallelism, 0, "splits cannot be empty");
        let mut compact_success = true;
        let mut output_ssts = Vec::with_capacity(parallelism);
        let mut compaction_futures = vec![];
        let mut compactor = Compactor::new(context, compact_task.clone());

        let vnode2unit: Arc<HashMap<u32, Vec<u32>>> = Arc::new(
            compact_task
                .vnode_mappings
                .into_iter()
                .map(|tar| {
                    (
                        tar.table_id,
                        decompress_data(tar.get_original_indices(), tar.get_data()),
                    )
                })
                .collect(),
        );
        let tables = compact_task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| table.id)
            .collect_vec();
        if let Err(e) = compactor
            .context
            .sstable_store
            .prefetch_sstables(tables)
            .await
        {
            tracing::warn!(
                "Compaction task {} prefetch failed with error: {:#?}",
                compact_task.task_id,
                e
            );
        }

        for (split_index, _) in compact_task.splits.iter().enumerate() {
            let compactor = compactor.clone();
            let vnode2unit = vnode2unit.clone();
            let compaction_executor = compactor.context.compaction_executor.as_ref().cloned();
            let split_task = async move {
                let merge_iter = compactor.build_sst_iter().await?;
                compactor
                    .compact_key_range(split_index, merge_iter, vnode2unit)
                    .await
            };
            let rx = match Compactor::request_execution(compaction_executor, split_task) {
                Ok(rx) => rx,
                Err(err) => {
                    tracing::warn!("Failed to schedule compaction execution: {:#?}", err);
                    return false;
                }
            };
            compaction_futures.push(rx);
        }

        let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
        while let Some(future_result) = buffered.next().await {
            match future_result.unwrap() {
                Ok((split_index, ssts)) => {
                    output_ssts.push((split_index, ssts));
                }
                Err(e) => {
                    compact_success = false;
                    tracing::warn!(
                        "Compaction task {} failed with error: {:#?}",
                        compact_task.task_id,
                        e
                    );
                }
            }
        }

        // Sort by split/key range index.
        output_ssts.sort_by_key(|(split_index, _)| *split_index);

        // After a compaction is done, mutate the compaction task.
        compactor.compact_done(output_ssts, compact_success).await;
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished compaction task in {:?}ms: \n{}",
            cost_time,
            compact_task_to_string(&compactor.compact_task)
        );
        compact_success
    }

    /// Fill in the compact task and let hummock manager know the compaction output ssts.
    async fn compact_done(&mut self, output_ssts: Vec<CompactOutput>, task_ok: bool) {
        self.compact_task.task_status = task_ok;
        self.compact_task
            .sorted_output_ssts
            .reserve(self.compact_task.splits.len());
        let mut compaction_write_bytes = 0;
        for (_, ssts) in output_ssts {
            for (sst, vnode_bitmaps) in ssts {
                let sst_info = SstableInfo {
                    id: sst.id,
                    key_range: Some(risingwave_pb::hummock::KeyRange {
                        left: sst.meta.smallest_key.clone(),
                        right: sst.meta.largest_key.clone(),
                        inf: false,
                    }),
                    file_size: sst.meta.estimated_size as u64,
                    vnode_bitmaps,
                };
                compaction_write_bytes += sst_info.file_size;
                self.compact_task.sorted_output_ssts.push(sst_info);
            }
        }
        self.context
            .stats
            .compaction_write_bytes
            .inc_by(compaction_write_bytes);
        if let Some(metrics) = self.compact_task.metrics.as_mut() {
            if let Some(write) = metrics.write.as_mut() {
                write.cnt = self.compact_task.sorted_output_ssts.len() as u64;
                write.size_kb = compaction_write_bytes / 1024;
            }
        }
        if let Err(e) = self
            .context
            .hummock_meta_client
            .report_compaction_task(self.compact_task.to_owned())
            .await
        {
            tracing::warn!(
                "Failed to report compaction task: {}, error: {}",
                self.compact_task.task_id,
                e
            );
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    async fn compact_key_range(
        &self,
        split_index: usize,
        iter: BoxedForwardHummockIterator,
        vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
    ) -> HummockResult<CompactOutput> {
        let split = self.compact_task.splits[split_index].clone();
        let kr = KeyRange {
            left: Bytes::copy_from_slice(split.get_left()),
            right: Bytes::copy_from_slice(split.get_right()),
            inf: split.get_inf(),
        };

        let get_id_time = Arc::new(AtomicU64::new(0));

        // NOTICE: should be user_key overlap, NOT full_key overlap!
        let mut builder = GroupedSstableBuilder::new(
            || async {
                let timer = Instant::now();
                let table_id = (self.context.sstable_id_generator)().await?;
                let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
                let builder = SSTableBuilder::new(self.context.options.as_ref().into());
                get_id_time.fetch_add(cost, Ordering::Relaxed);
                Ok((table_id, builder))
            },
            VirtualNode(VirtualNodeGrouping::new(vnode2unit)),
        );

        // Monitor time cost building shared buffer to SSTs.
        let _timer = if self.context.is_share_buffer_compact {
            self.context.stats.write_build_l0_sst_duration.start_timer()
        } else {
            self.context.stats.compact_sst_duration.start_timer()
        };
        Compactor::compact_and_build_sst(
            &mut builder,
            kr,
            iter,
            !self.compact_task.is_target_ultimate_and_leveling,
            self.compact_task.watermark,
        )
        .await?;

        // Seal.
        builder.seal_current();

        let mut ssts = Vec::new();
        ssts.reserve(builder.len());
        // TODO: decide upload concurrency. Maybe we shall create a upload task channel for multiple
        // compaction tasks.
        let mut pending_requests = vec![];
        let files = builder.finish();
        let file_count = files.len();
        for (table_id, data, meta, vnode_bitmaps) in files {
            let sst = Sstable { id: table_id, meta };
            let len = data.len();
            ssts.push((sst.clone(), vnode_bitmaps));
            if file_count > 1 {
                let sstable_store = self.context.sstable_store.clone();
                let ret =
                    tokio::spawn(
                        async move { sstable_store.put(sst, data, CachePolicy::Fill).await },
                    );
                pending_requests.push(ret);
            } else {
                self.context
                    .sstable_store
                    .put(sst, data, CachePolicy::Fill)
                    .await?;
            }

            if self.context.is_share_buffer_compact {
                self.context
                    .stats
                    .shared_buffer_to_sstable_size
                    .observe(len as _);
            } else {
                self.context.stats.compaction_upload_sst_counts.inc();
            }
        }
        if !pending_requests.is_empty() {
            for ret in try_join_all(pending_requests)
                .await
                .map_err(HummockError::other)?
            {
                ret?;
            }
        }
        self.context
            .stats
            .get_table_id_total_time_duration
            .observe(get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);
        Ok((split_index, ssts))
    }

    /// Build the merge iterator based on the given input ssts.
    async fn build_sst_iter(&self) -> HummockResult<BoxedForwardHummockIterator> {
        let mut table_iters: Vec<BoxedForwardHummockIterator> = Vec::new();
        let mut stats = StoreLocalStatistic::default();
        let read_options = Arc::new(ReadOptions { prefetch: true });
        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }
            // Do not need to filter the table because manager has done it.
            // let read_statistics: &mut TableSetStatistics = if *level_idx ==
            // compact_task.target_level {
            //     compact_task.metrics.as_mut().unwrap().read_level_nplus1.as_mut().unwrap()
            // } else {
            //     compact_task.metrics.as_mut().unwrap().read_level_n.as_mut().unwrap()
            // };
            // for table in &tables {
            //     read_statistics.size_gb += table.meta.estimated_size as f64 / (1024 * 1024 *
            // 1024) as f64;     read_statistics.cnt += 1;
            // }

            if can_concat(&level.get_table_infos().iter().collect_vec()) {
                table_iters.push(Box::new(ConcatIterator::new(
                    level.table_infos.clone(),
                    self.context.sstable_store.clone(),
                    read_options.clone(),
                )));
            } else {
                for table_info in &level.table_infos {
                    let table = self
                        .context
                        .sstable_store
                        .sstable(table_info.id, &mut stats)
                        .await?;
                    table_iters.push(Box::new(SSTableIterator::create(
                        table,
                        self.context.sstable_store.clone(),
                        read_options.clone(),
                    )));
                }
            }
        }
        stats.report(self.context.stats.as_ref());
        Ok(Box::new(MergeIterator::new(
            table_iters,
            self.context.stats.clone(),
        )))
    }

    pub async fn try_vacuum(
        vacuum_task: Option<VacuumTask>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        if let Some(vacuum_task) = vacuum_task {
            tracing::info!("Try to vacuum SSTs {:?}", vacuum_task.sstable_ids);
            match Vacuum::vacuum(
                sstable_store.clone(),
                vacuum_task,
                hummock_meta_client.clone(),
            )
            .await
            {
                Ok(_) => {
                    tracing::info!("Finish vacuuming SSTs");
                }
                Err(e) => {
                    tracing::warn!("Failed to vacuum SSTs. {:#?}", e);
                }
            }
        }
    }

    /// The background compaction thread that receives compaction tasks from hummock compaction
    /// manager and runs compaction tasks.
    pub fn start_compactor(
        options: Arc<StorageConfig>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        compaction_executor: Option<Arc<CompactionExecutor>>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let compactor_context = Arc::new(CompactorContext {
            options,
            hummock_meta_client: hummock_meta_client.clone(),
            sstable_store: sstable_store.clone(),
            stats,
            is_share_buffer_compact: false,
            sstable_id_generator: get_remote_sstable_id_generator(hummock_meta_client.clone()),
            compaction_executor,
        });
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let stream_retry_interval = Duration::from_secs(60);
        let join_handle = tokio::spawn(async move {
            let process_task = |compact_task,
                                vacuum_task,
                                compactor_context,
                                sstable_store,
                                hummock_meta_client| async {
                if let Some(compact_task) = compact_task {
                    Compactor::compact(compactor_context, compact_task).await;
                }

                Compactor::try_vacuum(vacuum_task, sstable_store, hummock_meta_client).await;
            };
            let mut min_interval = tokio::time::interval(stream_retry_interval);
            // This outer loop is to recreate stream.
            'start_stream: loop {
                tokio::select! {
                    // Wait for interval.
                    _ = min_interval.tick() => {},
                    // Shutdown compactor.
                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return;
                    }
                }

                let mut stream = match compactor_context
                    .hummock_meta_client
                    .subscribe_compact_tasks()
                    .await
                {
                    Ok(stream) => {
                        tracing::debug!("Succeeded subscribe_compact_tasks.");
                        stream
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Subscribing to compaction tasks failed with error: {}. Will retry.",
                            e
                        );
                        continue 'start_stream;
                    }
                };

                // This inner loop is to consume stream.
                loop {
                    let message = tokio::select! {
                        message = stream.message() => {
                            message
                        },
                        // Shutdown compactor
                        _ = &mut shutdown_rx => {
                            tracing::info!("Compactor is shutting down");
                            return
                        }
                    };
                    match message {
                        // The inner Some is the side effect of generated code.
                        Ok(Some(SubscribeCompactTasksResponse {
                            compact_task,
                            vacuum_task,
                        })) => {
                            tokio::spawn(process_task(
                                compact_task,
                                vacuum_task,
                                compactor_context.clone(),
                                sstable_store.clone(),
                                hummock_meta_client.clone(),
                            ));
                        }
                        Err(e) => {
                            tracing::warn!("Failed to consume stream. {}", e.message());
                            continue 'start_stream;
                        }
                        _ => {
                            // The stream is exhausted
                            continue 'start_stream;
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    async fn compact_and_build_sst<B, F>(
        sst_builder: &mut GroupedSstableBuilder<B>,
        kr: KeyRange,
        mut iter: BoxedForwardHummockIterator,
        has_user_key_overlap: bool,
        watermark: Epoch,
    ) -> HummockResult<()>
    where
        B: Clone + Fn() -> F,
        F: Future<Output = HummockResult<(u64, SSTableBuilder)>>,
    {
        if !kr.left.is_empty() {
            iter.seek(&kr.left).await?;
        } else {
            iter.rewind().await?;
        }

        let mut skip_key = BytesMut::new();
        let mut last_key = BytesMut::new();

        while iter.is_valid() {
            let iter_key = iter.key();

            if !skip_key.is_empty() {
                if VersionedComparator::same_user_key(iter_key, &skip_key) {
                    iter.next().await?;
                    continue;
                } else {
                    skip_key.clear();
                }
            }

            let is_new_user_key =
                last_key.is_empty() || !VersionedComparator::same_user_key(iter_key, &last_key);

            if is_new_user_key {
                if !kr.right.is_empty()
                    && VersionedComparator::compare_key(iter_key, &kr.right)
                        != std::cmp::Ordering::Less
                {
                    break;
                }

                last_key.clear();
                last_key.extend_from_slice(iter_key);
            }

            let epoch = get_epoch(iter_key);

            // Among keys with same user key, only retain keys which satisfy `epoch` >= `watermark`,
            // and the latest key which satisfies `epoch` < `watermark`
            if epoch < watermark {
                skip_key = BytesMut::from(iter_key);
                if iter.value().is_delete() && !has_user_key_overlap {
                    iter.next().await?;
                    continue;
                }
            }

            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(FullKey::from_slice(iter_key), iter.value(), is_new_user_key)
                .await?;

            iter.next().await?;
        }
        Ok(())
    }
}
