use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use prost::Message;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::{
    CompactTask, HummockContext, HummockContextPinnedSnapshot, HummockContextPinnedVersion,
    HummockSnapshot, HummockTablesToDelete, HummockVersion, Level, LevelType, SstableInfo,
    UncommittedEpoch,
};
use risingwave_storage::hummock::key_range::KeyRange;
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockError, HummockSSTableId, HummockTTL, HummockVersionId,
    INVALID_EPOCH,
};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::hummock::compaction::{CompactStatus, CompactionInner};
use crate::hummock::level_handler::{LevelHandler, SSTableStat};
use crate::manager::{IdCategory, MetaSrvEnv, SINGLE_VERSION_EPOCH};
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

#[derive(Clone)]
pub struct Config {
    // millisecond
    pub context_ttl: u64,
    // millisecond
    pub context_check_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            context_ttl: 10000,
            context_check_interval: 1000,
        }
    }
}

#[async_trait]
pub trait HummockManager: Sync + Send + 'static {
    async fn create_hummock_context(&self) -> Result<HummockContext>;
    async fn invalidate_hummock_context(&self, context_id: HummockContextId) -> Result<()>;
    /// Extend the context's TTL
    async fn refresh_hummock_context(&self, context_id: HummockContextId) -> Result<HummockTTL>;
    /// Pin a version, so that all files at this version won't be deleted.
    async fn pin_version(
        &self,
        context_id: HummockContextId,
    ) -> Result<(HummockVersionId, HummockVersion)>;
    async fn unpin_version(
        &self,
        context_id: HummockContextId,
        pinned_version_id: HummockVersionId,
    ) -> Result<()>;
    /// Add some SSTs to manifest
    async fn add_tables(
        &self,
        context_id: HummockContextId,
        tables: Vec<SstableInfo>,
        epoch: HummockEpoch,
    ) -> Result<HummockVersionId>;
    async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot>;
    async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()>;
    async fn get_compact_task(&self, context_id: HummockContextId) -> Result<CompactTask>;
    async fn report_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: CompactTask,
        task_result: bool,
    ) -> Result<()>;
    async fn commit_epoch(&self, context_id: HummockContextId, epoch: HummockEpoch) -> Result<()>;
    async fn abort_epoch(&self, context_id: HummockContextId, epoch: HummockEpoch) -> Result<()>;
    async fn get_new_table_id(&self) -> Result<HummockSSTableId>;
}

const RESERVED_HUMMOCK_CONTEXT_ID: HummockContextId = -1;

pub struct DefaultHummockManager {
    env: MetaSrvEnv,
    hummock_config: Config,
    // lock order: context_expires_at before compaction
    context_expires_at: RwLock<HashMap<HummockContextId, Instant>>,
    // lock order: compaction before inner
    compaction: Mutex<CompactionInner>,
    inner: RwLock<DefaultHummockManagerInner>,
}

pub(super) struct DefaultHummockManagerInner {
    env: MetaSrvEnv,
}

/// [`DefaultHummockManagerInner`] manages hummock meta data in meta store.
/// Table refers to `SSTable` in `HummockManager`.
/// `cf(hummock_context)`: `HummockContextId` -> `HummockContext`
/// `cf(hummock_version)`: `HummockVersionId` -> `HummockVersion`
/// `cf(hummock_table)`: `table_id` -> `Table`
/// `cf(hummock_context_pinned_version)`: `HummockContextId` -> `HummockContextPinnedVersion`
/// `cf(hummock_context_pinned_snapshot)`: `HummockContextId` -> `HummockContextPinnedSnapshot`
/// `cf(hummock_deletion)`: `HummockVersionId` -> `HummockTablesToDelete`
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionId`
///                        `hummock_compact_status_key` -> `CompactStatus`
impl DefaultHummockManagerInner {
    fn new(env: MetaSrvEnv) -> DefaultHummockManagerInner {
        DefaultHummockManagerInner { env }
    }

    async fn pick_few_tables(&self, table_ids: &[u64]) -> Result<Vec<SstableInfo>> {
        let mut ret = Vec::with_capacity(table_ids.len());
        for &table_id in table_ids {
            let table: SstableInfo = self.get_table_data(table_id).await?;
            ret.push(table);
        }
        Ok(ret)
    }

    async fn get_table_data(&self, table_id: u64) -> Result<SstableInfo> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_table_cf(),
                &table_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|t| SstableInfo::decode(t.as_slice()).unwrap())
    }

    async fn get_current_version_id(&self) -> Result<HummockVersionId> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_default_cf(),
                self.env.config().get_hummock_version_id_key().as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockVersionId::from_be_bytes(v.as_slice().try_into().unwrap()))
    }

    async fn get_version_data(&self, version_id: HummockVersionId) -> Result<HummockVersion> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_version_cf(),
                &version_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|s| HummockVersion::decode(s.as_slice()).unwrap())
    }

    fn add_table_in_trx(&self, trx: &mut Box<dyn Transaction>, table: &SstableInfo) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                table.id.to_be_bytes().as_slice(),
                self.env.config().get_hummock_table_cf().as_bytes(),
            ),
            table.encode_to_vec(),
            vec![],
        )]);
    }

    fn delete_table_in_trx(&self, trx: &mut Box<dyn Transaction>, table_id: u64) {
        trx.add_operations(vec![Operation::Delete(
            ColumnFamilyUtils::prefix_key_with_cf(
                table_id.to_be_bytes().as_slice(),
                self.env.config().get_hummock_table_cf().as_bytes(),
            ),
            vec![],
        )]);
    }

    fn add_version_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        new_version_id: HummockVersionId,
        hummock_version: &HummockVersion,
    ) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                new_version_id.to_be_bytes().as_slice(),
                self.env.config().get_hummock_version_cf().as_bytes(),
            ),
            hummock_version.encode_to_vec(),
            vec![],
        )]);
    }

    async fn get_pinned_version_by_context(
        &self,
        context_id: HummockContextId,
    ) -> Result<HummockContextPinnedVersion> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_context_pinned_version_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedVersion::decode(v.as_slice()).unwrap())
    }

    fn pin_version(
        &self,
        pinned_versions: &mut HummockContextPinnedVersion,
        version_id: HummockVersionId,
    ) {
        pinned_versions.version_id.push(version_id);
    }

    fn unpin_version(
        &self,
        context_pinned_version: &mut HummockContextPinnedVersion,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let found = context_pinned_version
            .version_id
            .iter()
            .position(|&v| v == pinned_version_id);
        match found {
            Some(pos) => {
                context_pinned_version.version_id.remove(pos);
                Ok(())
            }
            None => Err(HummockError::meta_error(format!(
                "version id not found: {0}",
                pinned_version_id
            ))
            .into()),
        }
    }

    fn update_pinned_versions_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        context_id: HummockContextId,
        pinned_versions: &HummockContextPinnedVersion,
    ) {
        if pinned_versions.version_id.is_empty() {
            trx.add_operations(vec![Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_version_cf()
                        .as_bytes(),
                ),
                vec![],
            )]);
        } else {
            trx.add_operations(vec![Operation::Put(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_version_cf()
                        .as_bytes(),
                ),
                pinned_versions.encode_to_vec(),
                vec![],
            )]);
        }
    }

    async fn get_pinned_snapshot_by_context(
        &self,
        context_id: HummockContextId,
    ) -> Result<HummockContextPinnedSnapshot> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_context_pinned_snapshot_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedSnapshot::decode(v.as_slice()).unwrap())
    }
    fn pin_snapshot(
        &self,
        context_pinned_snapshot: &mut HummockContextPinnedSnapshot,
        new_snapshot_id: HummockEpoch,
    ) {
        context_pinned_snapshot.snapshot_id.push(new_snapshot_id);
    }

    fn unpin_snapshot(
        &self,
        context_pinned_snapshot: &mut HummockContextPinnedSnapshot,
        pinned_snapshot_id: HummockEpoch,
    ) -> Result<()> {
        let found = context_pinned_snapshot
            .snapshot_id
            .iter()
            .position(|&v| v == pinned_snapshot_id);
        match found {
            Some(pos) => {
                context_pinned_snapshot.snapshot_id.remove(pos);
                Ok(())
            }
            None => Err(HummockError::meta_error(format!(
                "epoch not found: {0}",
                pinned_snapshot_id
            ))
            .into()),
        }
    }

    fn update_pinned_snapshots_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        context_id: HummockContextId,
        pinned_snapshots: &HummockContextPinnedSnapshot,
    ) {
        if pinned_snapshots.snapshot_id.is_empty() {
            trx.add_operations(vec![Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_snapshot_cf()
                        .as_bytes(),
                ),
                vec![],
            )]);
        } else {
            trx.add_operations(vec![Operation::Put(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_snapshot_cf()
                        .as_bytes(),
                ),
                pinned_snapshots.encode_to_vec(),
                vec![],
            )]);
        }
    }

    async fn get_snapshot_low_watermark(&self) -> Result<HummockEpoch> {
        let version_data = self
            .get_version_data(self.get_current_version_id().await?)
            .await?;
        let result = self
            .env
            .meta_store_ref()
            .list_cf(self.env.config().get_hummock_context_pinned_snapshot_cf())
            .await?;
        let low_watermark = result
            .iter()
            .flat_map(|v| {
                HummockContextPinnedSnapshot::decode(v.as_slice())
                    .unwrap()
                    .snapshot_id
            })
            .fold(version_data.max_committed_epoch, std::cmp::min);
        Ok(low_watermark)
    }

    fn update_version_id_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        new_version_id: HummockVersionId,
    ) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                self.env.config().get_hummock_version_id_key().as_bytes(),
                self.env.config().get_hummock_default_cf().as_bytes(),
            ),
            new_version_id.to_be_bytes().to_vec(),
            vec![],
        )]);
    }

    async fn get_tables(&self) -> Result<Vec<SstableInfo>> {
        let table_list = self
            .env
            .meta_store_ref()
            .list_cf(self.env.config().get_hummock_table_cf())
            .await?;
        Ok(table_list
            .iter()
            .map(|v| SstableInfo::decode(v.as_slice()).unwrap())
            .collect())
    }

    async fn get_tables_to_delete(
        &self,
        version_id: HummockVersionId,
    ) -> Result<HummockTablesToDelete> {
        self.env
            .meta_store_ref()
            .get_cf(
                self.env.config().get_hummock_deletion_cf(),
                &version_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockTablesToDelete::decode(v.as_slice()).unwrap())
    }

    fn update_tables_to_delete_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        version_id: HummockVersionId,
        hummock_tables_to_delete: &HummockTablesToDelete,
    ) {
        if hummock_tables_to_delete.id.is_empty() {
            trx.add_operations(vec![Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    version_id.to_be_bytes().as_slice(),
                    self.env.config().get_hummock_deletion_cf().as_bytes(),
                ),
                vec![],
            )]);
        } else {
            trx.add_operations(vec![Operation::Put(
                ColumnFamilyUtils::prefix_key_with_cf(
                    version_id.to_be_bytes().as_slice(),
                    self.env.config().get_hummock_deletion_cf().as_bytes(),
                ),
                hummock_tables_to_delete.encode_to_vec(),
                vec![],
            )]);
        }
    }

    fn invalidate_hummock_context_in_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        context_id: HummockContextId,
    ) {
        trx.add_operations(vec![
            Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_version_cf()
                        .as_bytes(),
                ),
                vec![],
            ),
            Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env
                        .config()
                        .get_hummock_context_pinned_snapshot_cf()
                        .as_bytes(),
                ),
                vec![],
            ),
            Operation::Delete(
                ColumnFamilyUtils::prefix_key_with_cf(
                    context_id.to_be_bytes().as_slice(),
                    self.env.config().get_hummock_context_cf().as_bytes(),
                ),
                vec![],
            ),
        ]);
    }

    fn add_hummock_context_in_trx(&self, trx: &mut Box<dyn Transaction>, context: &HummockContext) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                context.identifier.to_be_bytes().as_slice(),
                self.env.config().get_hummock_context_cf().as_bytes(),
            ),
            context.encode_to_vec(),
            vec![],
        )]);
    }
}

impl DefaultHummockManager {
    pub async fn new(
        env: MetaSrvEnv,
        hummock_config: Config,
    ) -> Result<(Arc<Self>, JoinHandle<Result<()>>)> {
        let instance = Arc::new(Self {
            env: env.clone(),
            hummock_config,
            context_expires_at: RwLock::new(HashMap::new()),
            inner: RwLock::new(DefaultHummockManagerInner::new(env.clone())),
            compaction: Mutex::new(CompactionInner::new(env.clone())),
        });

        instance.restore_context_meta().await?;
        instance.restore_table_meta().await?;

        let join_handle = tokio::spawn(Self::hummock_context_tracker_loop(Arc::downgrade(
            &instance,
        )));

        Ok((instance, join_handle))
    }

    /// Restore the hummock context related data in meta store to a consistent state.
    async fn restore_context_meta(&self) -> Result<()> {
        let hummock_context_list = self
            .inner
            .read()
            .await
            .env
            .meta_store_ref()
            .list_cf(self.env.config().get_hummock_context_cf())
            .await?;
        let mut context_guard = self.context_expires_at.write().await;
        hummock_context_list.iter().for_each(|v| {
            let hummock_context: HummockContext = HummockContext::decode(v.as_slice()).unwrap();
            context_guard.insert(
                hummock_context.identifier,
                Instant::now().add(Duration::from_millis(self.hummock_config.context_ttl)),
            );
        });
        Ok(())
    }

    /// Restore the table related data in meta store to a consistent state.
    async fn restore_table_meta(&self) -> Result<()> {
        let compaction_guard = self.compaction.lock().await;
        let inner_guard = self.inner.write().await;
        let version_id = inner_guard.get_current_version_id().await;
        match version_id {
            Ok(_) => {}
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                // Use the absence of current_version_id to indicate a uninitialized meta store.
                // reset the meta data
                let mut transaction = self.env.meta_store_ref().get_transaction();
                let init_version_id: HummockVersionId = 0;
                inner_guard.update_version_id_in_trx(&mut transaction, init_version_id);
                // pin latest version use RESERVED_HUMMOCK_CONTEXT_ID
                inner_guard.update_pinned_versions_in_trx(
                    &mut transaction,
                    RESERVED_HUMMOCK_CONTEXT_ID,
                    &HummockContextPinnedVersion {
                        version_id: vec![init_version_id],
                    },
                );
                inner_guard.add_version_in_trx(
                    &mut transaction,
                    init_version_id,
                    &HummockVersion {
                        levels: vec![
                            Level {
                                level_type: LevelType::Overlapping as i32,
                                table_ids: vec![],
                            },
                            Level {
                                level_type: LevelType::Nonoverlapping as i32,
                                table_ids: vec![],
                            },
                        ],
                        uncommitted_epochs: vec![],
                        max_committed_epoch: INVALID_EPOCH,
                    },
                );
                let vec_handler_having_l0 = vec![
                    LevelHandler::Overlapping(vec![], vec![]),
                    LevelHandler::Nonoverlapping(vec![], vec![]),
                ];
                compaction_guard.save_compact_status_in_transaction(
                    &mut transaction,
                    &CompactStatus {
                        level_handlers: vec_handler_having_l0,
                        next_compact_task_id: 1,
                    },
                );

                self.commit_trx(&mut transaction, RESERVED_HUMMOCK_CONTEXT_ID)
                    .await?;
            }
        };

        Ok(())
    }

    /// A background task that periodically invalidates the hummock contexts whose TTL are expired.
    async fn hummock_context_tracker_loop(weak_self: Weak<Self>) -> Result<()> {
        loop {
            let hummock_manager_ref = weak_self.upgrade();
            if hummock_manager_ref.is_none() {
                return Ok(());
            }
            let hummock_manager = hummock_manager_ref.unwrap();
            let mut interval = tokio::time::interval(Duration::from_millis(
                hummock_manager.hummock_config.context_check_interval,
            ));
            interval.tick().await;
            interval.tick().await;
            let context_to_invalidate: Vec<HummockContextId>;
            {
                let guard = hummock_manager.context_expires_at.read().await;
                context_to_invalidate = guard
                    .iter()
                    .filter(|kv| Instant::now().saturating_duration_since(*kv.1) > Duration::ZERO)
                    .map(|kv| *kv.0)
                    .collect();
            }
            for context_id in context_to_invalidate {
                hummock_manager
                    .invalidate_hummock_context(context_id)
                    .await?;
            }
        }
    }

    async fn commit_trx(
        &self,
        trx: &mut Box<dyn Transaction>,
        context_id: HummockContextId,
    ) -> Result<()> {
        if context_id != RESERVED_HUMMOCK_CONTEXT_ID {
            // check context validity
            // TODO disable hummock context validation until we decide to adopt it.
            // trx.add_preconditions(vec![Precondition::KeyExists {
            //   key: ColumnFamilyUtils::prefix_key_with_cf(
            //     context_id.to_be_bytes().as_slice(),
            //     self.env.config().get_hummock_context_cf().as_bytes(),
            //   ),
            //   version: None,
            // }]);
        }
        trx.commit().map_err(|e| e.into())
    }
}

#[async_trait]
impl HummockManager for DefaultHummockManager {
    async fn create_hummock_context(&self) -> Result<HummockContext> {
        let context_id = self
            .env
            .id_gen_manager_ref()
            .generate(IdCategory::HummockContext)
            .await?;
        let new_context = HummockContext {
            identifier: context_id,
            ttl: self.hummock_config.context_ttl,
        };
        let result = {
            let inner_guard = self.inner.write().await;
            let mut transaction = self.env.meta_store_ref().get_transaction();
            inner_guard.add_hummock_context_in_trx(&mut transaction, &new_context);
            transaction.commit()
        };
        match result {
            Ok(()) => {
                let mut guard = self.context_expires_at.write().await;
                let expires_at = Instant::now().add(Duration::from_millis(new_context.ttl));
                guard.insert(new_context.identifier, expires_at);
                Ok(new_context)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn invalidate_hummock_context(&self, context_id: HummockContextId) -> Result<()> {
        {
            let mut guard = self.context_expires_at.write().await;
            if guard.remove(&context_id) == None {
                return Err(RwError::from(HummockError::invalid_hummock_context(
                    context_id,
                )));
            }
        }
        let mut transaction = self.env.meta_store_ref().get_transaction();
        let inner_guard = self.inner.write().await;
        inner_guard.invalidate_hummock_context_in_trx(&mut transaction, context_id);
        self.commit_trx(&mut transaction, context_id).await
    }

    async fn refresh_hummock_context(&self, context_id: HummockContextId) -> Result<HummockTTL> {
        let mut guard = self.context_expires_at.write().await;
        match guard.get_mut(&context_id) {
            Some(_) => {
                let new_ttl = self.hummock_config.context_ttl;
                guard.insert(
                    context_id,
                    Instant::now().add(Duration::from_millis(new_ttl)),
                );
                Ok(new_ttl)
            }
            None => Err(HummockError::invalid_hummock_context(context_id).into()),
        }
    }

    async fn pin_version(
        &self,
        context_id: HummockContextId,
    ) -> Result<(HummockVersionId, HummockVersion)> {
        let inner_guard = self.inner.write().await;
        let version_id = inner_guard.get_current_version_id().await?;
        let hummock_version: HummockVersion = inner_guard.get_version_data(version_id).await?;
        // pin the version
        let mut context_pinned_version = inner_guard
            .get_pinned_version_by_context(context_id)
            .await
            .unwrap_or(HummockContextPinnedVersion { version_id: vec![] });
        inner_guard.pin_version(&mut context_pinned_version, version_id);
        let mut transaction = self.env.meta_store_ref().get_transaction();
        inner_guard.update_pinned_versions_in_trx(
            &mut transaction,
            context_id,
            &context_pinned_version,
        );
        self.commit_trx(&mut transaction, context_id).await?;

        Ok((version_id, hummock_version))
    }

    async fn unpin_version(
        &self,
        context_id: HummockContextId,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let mut transaction = self.env.meta_store_ref().get_transaction();
        let mut context_pinned_version: HummockContextPinnedVersion = inner_guard
            .get_pinned_version_by_context(context_id)
            .await?;
        inner_guard.unpin_version(&mut context_pinned_version, pinned_version_id)?;
        inner_guard.update_pinned_versions_in_trx(
            &mut transaction,
            context_id,
            &context_pinned_version,
        );
        self.commit_trx(&mut transaction, context_id).await
    }

    async fn add_tables(
        &self,
        context_id: HummockContextId,
        tables: Vec<SstableInfo>,
        epoch: HummockEpoch,
    ) -> Result<HummockVersionId> {
        // TODO #2156 the types will be unified to prost
        let stats = tables
            .iter()
            .map(|table| SSTableStat {
                key_range: KeyRange::new(
                    Bytes::copy_from_slice(&table.key_range.as_ref().unwrap().left),
                    Bytes::copy_from_slice(&table.key_range.as_ref().unwrap().right),
                ),
                table_id: table.id,
                compact_task: None,
            })
            .collect_vec();

        // Hold the compact status lock so that no one else could add/drop SST or search compaction
        // plan.
        let compaction_guard = self.compaction.lock().await;
        let mut compact_status: CompactStatus = compaction_guard.load_compact_status().await?;
        match compact_status.level_handlers.first_mut().unwrap() {
            LevelHandler::Overlapping(vec_tier, _) => {
                for stat in stats {
                    let insert_point = vec_tier.partition_point(
                        |SSTableStat {
                             key_range: other_key_range,
                             ..
                         }| { other_key_range <= &stat.key_range },
                    );
                    vec_tier.insert(insert_point, stat);
                }
            }
            LevelHandler::Nonoverlapping(_, _) => {
                panic!("L0 must be Tiering.");
            }
        }
        let mut transaction = self.env.meta_store_ref().get_transaction();
        // update compact_status
        compaction_guard.save_compact_status_in_transaction(&mut transaction, &compact_status);

        let inner_guard = self.inner.write().await;
        let current_tables = inner_guard.get_tables().await?;
        if tables
            .iter()
            .any(|t| current_tables.iter().any(|ct| ct.id == t.id))
        {
            panic!("Duplicate hummock table id when add_tables")
        }
        let old_version_id = inner_guard.get_current_version_id().await?;
        let mut hummock_version = inner_guard.get_version_data(old_version_id).await?;

        // check whether the epoch is valid
        // TODO: return error instead of panic
        if epoch <= hummock_version.max_committed_epoch {
            panic!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, hummock_version.max_committed_epoch
            );
        }

        // add tables
        tables
            .iter()
            .for_each(|t: &SstableInfo| inner_guard.add_table_in_trx(&mut transaction, t));
        let new_version_id = old_version_id + 1;

        // create new_version by adding tables in UncommittedEpoch
        match hummock_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch)
        {
            Some(uncommitted_epoch) => {
                tables
                    .iter()
                    .for_each(|t| uncommitted_epoch.table_ids.push(t.id));
            }
            None => hummock_version.uncommitted_epochs.push(UncommittedEpoch {
                epoch,
                table_ids: tables.iter().map(|t| t.id).collect(),
            }),
        };
        inner_guard.add_version_in_trx(&mut transaction, new_version_id, &hummock_version);

        // update pinned_version
        let mut reserved_context_pinned_version = inner_guard
            .get_pinned_version_by_context(RESERVED_HUMMOCK_CONTEXT_ID)
            .await?;
        inner_guard.unpin_version(&mut reserved_context_pinned_version, old_version_id)?;
        inner_guard.pin_version(&mut reserved_context_pinned_version, new_version_id);
        inner_guard.update_pinned_versions_in_trx(
            &mut transaction,
            RESERVED_HUMMOCK_CONTEXT_ID,
            &reserved_context_pinned_version,
        );

        // increase version counter
        inner_guard.update_version_id_in_trx(&mut transaction, new_version_id);

        // the trx contain update for both tables and compact_status
        self.commit_trx(&mut transaction, context_id).await?;

        Ok(new_version_id)
    }

    async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let inner_guard = self.inner.write().await;

        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let version_id = inner_guard.get_current_version_id().await.unwrap();
        let version = inner_guard.get_version_data(version_id).await.unwrap();
        let max_committed_epoch = version.max_committed_epoch;

        let mut context_pinned_snapshot = inner_guard
            .get_pinned_snapshot_by_context(context_id)
            .await
            .unwrap_or(HummockContextPinnedSnapshot {
                snapshot_id: vec![],
            });
        inner_guard.pin_snapshot(&mut context_pinned_snapshot, max_committed_epoch);
        let mut transaction = self.env.meta_store_ref().get_transaction();
        inner_guard.update_pinned_snapshots_in_trx(
            &mut transaction,
            context_id,
            &context_pinned_snapshot,
        );
        self.commit_trx(&mut transaction, context_id).await?;
        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let mut context_pinned_snapshot = inner_guard
            .get_pinned_snapshot_by_context(context_id)
            .await?;
        let mut transaction = self.env.meta_store_ref().get_transaction();
        inner_guard.unpin_snapshot(&mut context_pinned_snapshot, hummock_snapshot.epoch)?;
        inner_guard.update_pinned_snapshots_in_trx(
            &mut transaction,
            context_id,
            &context_pinned_snapshot,
        );
        self.commit_trx(&mut transaction, context_id).await
    }

    async fn get_compact_task(&self, context_id: HummockContextId) -> Result<CompactTask> {
        let watermark = self.inner.read().await.get_snapshot_low_watermark().await?;
        let compaction_guard = self.compaction.lock().await;
        let old_compact_status = compaction_guard.load_compact_status().await?;
        let (compact_status, mut compact_task) = compaction_guard
            .get_compact_task(old_compact_status)
            .await?;
        compact_task.watermark = watermark;
        let mut transaction = self.env.meta_store_ref().get_transaction();
        compaction_guard.save_compact_status_in_transaction(&mut transaction, &compact_status);
        self.commit_trx(&mut transaction, context_id).await?;
        Ok(compact_task)
    }

    async fn report_compact_task(
        &self,
        context_id: HummockContextId,
        compact_task: CompactTask,
        task_result: bool,
    ) -> Result<()> {
        // TODO #2156 the types will be unified to prost
        let output_table_compact_entries: Vec<_> = compact_task
            .sorted_output_ssts
            .iter()
            .map(|table| SSTableStat {
                key_range: KeyRange::new(
                    Bytes::copy_from_slice(&table.key_range.as_ref().unwrap().left),
                    Bytes::copy_from_slice(&table.key_range.as_ref().unwrap().right),
                ),
                table_id: table.id,
                compact_task: None,
            })
            .collect();
        let mut transaction = self.env.meta_store_ref().get_transaction();
        let compaction_guard = self.compaction.lock().await;
        let compact_status = compaction_guard.load_compact_status().await?;
        let (compact_status, sorted_output_ssts, delete_table_ids) = compaction_guard
            .report_compact_task(
                compact_status,
                output_table_compact_entries,
                compact_task,
                task_result,
            );
        compaction_guard.save_compact_status_in_transaction(&mut transaction, &compact_status);
        if task_result {
            let inner_guard = self.inner.write().await;
            let old_version_id = inner_guard.get_current_version_id().await?;
            let old_version = inner_guard.get_version_data(old_version_id).await?;
            let version = HummockVersion {
                levels: compact_status
                    .level_handlers
                    .iter()
                    .map(|level_handler| match level_handler {
                        LevelHandler::Overlapping(l_n, _) => Level {
                            level_type: LevelType::Overlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                        LevelHandler::Nonoverlapping(l_n, _) => Level {
                            level_type: LevelType::Nonoverlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                    })
                    .collect(),
                uncommitted_epochs: old_version.uncommitted_epochs,
                max_committed_epoch: old_version.max_committed_epoch,
            };

            sorted_output_ssts
                .into_iter()
                .for_each(|table| inner_guard.add_table_in_trx(&mut transaction, &table));

            // Add epoch number and make the modified snapshot available.
            let new_version_id = old_version_id + 1;
            inner_guard.update_version_id_in_trx(&mut transaction, new_version_id);
            let mut pinned_versions = inner_guard
                .get_pinned_version_by_context(RESERVED_HUMMOCK_CONTEXT_ID)
                .await?;
            inner_guard.pin_version(&mut pinned_versions, new_version_id);
            inner_guard.unpin_version(&mut pinned_versions, old_version_id)?;
            inner_guard.update_pinned_versions_in_trx(
                &mut transaction,
                RESERVED_HUMMOCK_CONTEXT_ID,
                &pinned_versions,
            );
            inner_guard.add_version_in_trx(&mut transaction, new_version_id, &version);
            let mut tables_to_delete = inner_guard
                .get_tables_to_delete(new_version_id)
                .await
                .unwrap_or(HummockTablesToDelete { id: vec![] });
            tables_to_delete.id.extend(delete_table_ids);
            inner_guard.update_tables_to_delete_in_trx(
                &mut transaction,
                new_version_id,
                &tables_to_delete,
            );
        }
        self.commit_trx(&mut transaction, context_id).await?;
        Ok(())
    }

    async fn commit_epoch(&self, context_id: HummockContextId, epoch: HummockEpoch) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let version_id = inner_guard.get_current_version_id().await?;
        let mut hummock_version: HummockVersion = inner_guard.get_version_data(version_id).await?;

        // get tables in the committing epoch
        if let Some(idx) = hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];

            // commit tables by moving them into level0
            let version_first_level = hummock_version.levels.first_mut().unwrap();
            match version_first_level.get_level_type() {
                LevelType::Overlapping => {
                    uncommitted_epoch
                        .table_ids
                        .iter()
                        .for_each(|id| version_first_level.table_ids.push(*id));
                }
                LevelType::Nonoverlapping => {
                    unimplemented!()
                }
            };

            // remove the epoch from uncommitted_epochs and update max_committed_epoch
            hummock_version.uncommitted_epochs.swap_remove(idx);
            if epoch > hummock_version.max_committed_epoch {
                hummock_version.max_committed_epoch = epoch;
            }

            // create new_version
            let mut transaction = self.env.meta_store_ref().get_transaction();
            let new_version_id = version_id + 1;
            inner_guard.add_version_in_trx(&mut transaction, new_version_id, &hummock_version);
            inner_guard.update_version_id_in_trx(&mut transaction, new_version_id);

            // update pinned_version
            let mut reserved_context_pinned_version = inner_guard
                .get_pinned_version_by_context(RESERVED_HUMMOCK_CONTEXT_ID)
                .await?;
            inner_guard.unpin_version(&mut reserved_context_pinned_version, version_id)?;
            inner_guard.pin_version(&mut reserved_context_pinned_version, new_version_id);
            inner_guard.update_pinned_versions_in_trx(
                &mut transaction,
                RESERVED_HUMMOCK_CONTEXT_ID,
                &reserved_context_pinned_version,
            );

            self.commit_trx(&mut transaction, context_id).await
        } else {
            Ok(())
        }
    }

    async fn abort_epoch(&self, context_id: HummockContextId, epoch: HummockEpoch) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let version_id = inner_guard.get_current_version_id().await?;
        let mut hummock_version: HummockVersion = inner_guard.get_version_data(version_id).await?;

        // get tables in the committing epoch
        match hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            Some(idx) => {
                let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];
                let mut transaction = self.env.meta_store_ref().get_transaction();

                // remove tables of the aborting epoch
                uncommitted_epoch.table_ids.iter().for_each(|table_id| {
                    inner_guard.delete_table_in_trx(&mut transaction, *table_id)
                });
                hummock_version.uncommitted_epochs.swap_remove(idx);

                // create new_version
                let new_version_id = version_id + 1;
                inner_guard.add_version_in_trx(&mut transaction, new_version_id, &hummock_version);
                inner_guard.update_version_id_in_trx(&mut transaction, new_version_id);

                // update pinned_version
                let mut reserved_context_pinned_version = inner_guard
                    .get_pinned_version_by_context(RESERVED_HUMMOCK_CONTEXT_ID)
                    .await?;
                inner_guard.unpin_version(&mut reserved_context_pinned_version, version_id)?;
                inner_guard.pin_version(&mut reserved_context_pinned_version, new_version_id);
                inner_guard.update_pinned_versions_in_trx(
                    &mut transaction,
                    RESERVED_HUMMOCK_CONTEXT_ID,
                    &reserved_context_pinned_version,
                );

                self.commit_trx(&mut transaction, context_id).await
            }
            None => Ok(()),
        }
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        // TODO id_gen_manager generates u32, we need u64
        self.env
            .id_gen_manager_ref()
            .generate(IdCategory::HummockSSTableId)
            .await
            .map(|id| id as HummockSSTableId)
    }
}
