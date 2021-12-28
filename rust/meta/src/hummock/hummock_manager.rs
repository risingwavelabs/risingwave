use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::level::LevelType;
use risingwave_pb::hummock::{
    HummockContext, HummockContextPinnedSnapshot, HummockContextPinnedVersion, HummockSnapshot,
    HummockVersion, Level, Table,
};
use risingwave_pb::meta::get_id_request::IdCategory;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::hummock::{HummockContextId, HummockTTL, HummockVersionId};
use crate::manager;
use crate::manager::{Epoch, IdGeneratorManagerRef, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

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
        tables: Vec<Table>,
    ) -> Result<HummockVersionId>;
    /// Get the iterators on the underlying tables.
    async fn get_tables(
        &self,
        context_id: HummockContextId,
        hummock_version: HummockVersion,
    ) -> Result<Vec<Table>>;
    async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot>;
    async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()>;
}

const RESERVED_HUMMOCK_CONTEXT_ID: HummockContextId = -1;

pub struct DefaultHummockManager {
    id_generator_manager_ref: IdGeneratorManagerRef,
    // lock order: context_expires_at before inner
    context_expires_at: RwLock<HashMap<HummockContextId, Instant>>,
    inner: RwLock<DefaultHummockManagerInner>,
    manager_config: manager::Config,
    hummock_config: Config,
}

struct DefaultHummockManagerInner {
    meta_store_ref: MetaStoreRef,
}

impl DefaultHummockManagerInner {
    fn new(meta_store_ref: MetaStoreRef) -> DefaultHummockManagerInner {
        DefaultHummockManagerInner { meta_store_ref }
    }

    /// The caller should have held the read lock
    async fn pick_few_tables(
        &self,
        manager_config: &manager::Config,
        table_ids: &[u64],
    ) -> Result<Vec<Table>> {
        let mut ret = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            let table: Table = self
                .meta_store_ref
                .get_cf(
                    manager_config.get_hummock_table_cf(),
                    &table_id.to_be_bytes(),
                    SINGLE_VERSION_EPOCH,
                )
                .await
                .map(|t| Table::decode(t.as_slice()).unwrap())?;
            ret.push(table);
        }
        Ok(ret)
    }
}

impl DefaultHummockManager {
    pub async fn new(
        meta_store_ref: MetaStoreRef,
        id_generator_manager_ref: IdGeneratorManagerRef,
        manager_config: manager::Config,
        hummock_config: Config,
    ) -> Result<(Arc<Self>, JoinHandle<Result<()>>)> {
        let instance = Arc::new(Self {
            id_generator_manager_ref,
            context_expires_at: RwLock::new(HashMap::new()),
            inner: RwLock::new(DefaultHummockManagerInner::new(meta_store_ref.clone())),
            manager_config,
            hummock_config,
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
        let hummock_context_list;
        {
            let inner_guard = self.inner.read().await;
            hummock_context_list = inner_guard
                .meta_store_ref
                .list_cf(self.manager_config.get_hummock_context_cf())
                .await?;
        }

        let mut guard = self.context_expires_at.write().await;
        hummock_context_list.iter().for_each(|v| {
            let hummock_context: HummockContext = HummockContext::decode(v.as_slice()).unwrap();
            guard.insert(
                hummock_context.identifier,
                Instant::now().add(Duration::from_millis(self.hummock_config.context_ttl)),
            );
        });
        Ok(())
    }

    /// Restore the table related data in meta store to a consistent state.
    async fn restore_table_meta(&self) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let version_id = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_default_cf(),
                self.manager_config.get_hummock_version_id_key().as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await;
        match version_id {
            Ok(_) => {}
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                // Initialize meta data
                let mut write_batch: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![];
                let init_version_id: HummockVersionId = 0;
                write_batch.push((
                    self.manager_config.get_hummock_default_cf(),
                    self.manager_config
                        .get_hummock_version_id_key()
                        .as_bytes()
                        .to_vec(),
                    init_version_id.to_be_bytes().to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
                // pin latest version use RESERVED_HUMMOCK_CONTEXT_ID
                write_batch.push((
                    self.manager_config.get_hummock_context_pinned_version_cf(),
                    RESERVED_HUMMOCK_CONTEXT_ID.to_be_bytes().to_vec(),
                    HummockContextPinnedVersion {
                        version_id: vec![init_version_id],
                    }
                    .encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
                write_batch.push((
                    self.manager_config.get_hummock_version_cf(),
                    init_version_id.to_be_bytes().to_vec(),
                    HummockVersion {
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
                    }
                    .encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
                inner_guard.meta_store_ref.put_batch_cf(write_batch).await?;
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
}

/// [`DefaultHummockManager`] manages hummock related meta data.
/// Table refers to SSTable in HummockManager.
/// cf(hummock_context): `HummockContextId` -> `HummockContext`
/// cf(hummock_version): `HummockVersionId` -> `HummockVersion`
/// cf(hummock_table): `table_id` -> `Table`
/// cf(hummock_default): currently only one record: `hummock_version_id_key` -> `HummockVersionId`
/// cf(hummock_deletion): `HummockVersionId` -> table_id
/// cf(hummock_context_pinned_version): `HummockContextId` -> `HummockContextPinnedVersion`
/// cf(hummock_context_pinned_snapshot): `HummockContextId` -> `HummockContextPinnedSnapshot`
#[async_trait]
impl HummockManager for DefaultHummockManager {
    async fn create_hummock_context(&self) -> Result<HummockContext> {
        let context_id = self
            .id_generator_manager_ref
            .generate(IdCategory::HummockContext)
            .await?;
        let new_context = HummockContext {
            identifier: context_id,
            ttl: self.hummock_config.context_ttl,
        };
        let result;
        {
            let inner_guard = self.inner.write().await;
            result = inner_guard
                .meta_store_ref
                .put_cf(
                    self.manager_config.get_hummock_context_cf(),
                    &new_context.identifier.to_be_bytes(),
                    &new_context.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                )
                .await;
        }
        match result {
            Ok(()) => {
                let mut guard = self.context_expires_at.write().await;
                let expires_at = Instant::now().add(Duration::from_millis(new_context.ttl));
                guard.insert(new_context.identifier, expires_at);
                Ok(new_context)
            }
            Err(err) => Err(err),
        }
    }

    async fn invalidate_hummock_context(&self, context_id: HummockContextId) -> Result<()> {
        {
            let mut guard = self.context_expires_at.write().await;
            if guard.remove(&context_id) == None {
                return Err(RwError::from(ErrorCode::HummockContextNotFound(context_id)));
            }
        }
        let inner_guard = self.inner.write().await;
        // TODO the deletions should be done in a transaction.
        // delete version pin and snapshot pin
        inner_guard
            .meta_store_ref
            .delete_cf(
                self.manager_config.get_hummock_context_pinned_version_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        inner_guard
            .meta_store_ref
            .delete_cf(
                self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        // delete context record
        inner_guard
            .meta_store_ref
            .delete_cf(
                self.manager_config.get_hummock_context_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
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
            None => Err(RwError::from(ErrorCode::HummockContextNotFound(context_id))),
        }
    }

    async fn pin_version(
        &self,
        context_id: HummockContextId,
    ) -> Result<(HummockVersionId, HummockVersion)> {
        let inner_guard = self.inner.write().await;
        // get latest version id
        let version_id = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_default_cf(),
                self.manager_config.get_hummock_version_id_key().as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        // get version data
        let hummock_version: HummockVersion = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_version_cf(),
                version_id.as_slice(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|s| HummockVersion::decode(s.as_slice()).unwrap())?;

        // pin the version
        let hummock_version_id = HummockVersionId::from_be_bytes(version_id.try_into().unwrap());
        let mut context_pinned_version: HummockContextPinnedVersion = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_context_pinned_version_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedVersion::decode(v.as_slice()).unwrap())
            .unwrap_or(HummockContextPinnedVersion { version_id: vec![] });
        context_pinned_version.version_id.push(hummock_version_id);
        // TODO write in transaction with context_id validation
        inner_guard
            .meta_store_ref
            .put_cf(
                self.manager_config.get_hummock_context_pinned_version_cf(),
                &context_id.to_be_bytes(),
                &context_pinned_version.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok((hummock_version_id, hummock_version))
    }

    async fn unpin_version(
        &self,
        context_id: HummockContextId,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let mut context_pinned_version: HummockContextPinnedVersion = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_context_pinned_version_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedVersion::decode(v.as_slice()).unwrap())?;
        let found = context_pinned_version
            .version_id
            .iter()
            .position(|&v| v == pinned_version_id);
        match found {
            Some(pos) => {
                context_pinned_version.version_id.remove(pos);
                // TODO write in transaction with context_id validation
                if context_pinned_version.version_id.is_empty() {
                    inner_guard
                        .meta_store_ref
                        .delete_cf(
                            self.manager_config.get_hummock_context_pinned_version_cf(),
                            &context_id.to_be_bytes(),
                            SINGLE_VERSION_EPOCH,
                        )
                        .await
                } else {
                    inner_guard
                        .meta_store_ref
                        .put_cf(
                            self.manager_config.get_hummock_context_pinned_version_cf(),
                            &context_id.to_be_bytes(),
                            &context_pinned_version.encode_to_vec(),
                            SINGLE_VERSION_EPOCH,
                        )
                        .await
                }
            }
            None => Err(RwError::from(ErrorCode::HummockVersionNotPinned(
                pinned_version_id,
                context_id,
            ))),
        }
    }

    async fn add_tables(
        &self,
        _context_id: HummockContextId,
        tables: Vec<Table>,
    ) -> Result<HummockVersionId> {
        let inner_guard = self.inner.write().await;
        let table_list = inner_guard
            .meta_store_ref
            .list_cf(self.manager_config.get_hummock_table_cf())
            .await?;
        let current_tables: Vec<Table> = table_list
            .iter()
            .map(|t| Table::decode(t.as_slice()).unwrap())
            .collect();
        if tables
            .iter()
            .any(|t| current_tables.iter().any(|ct| ct.sst_id == t.sst_id))
        {
            panic!("Duplicate hummock table id when add_tables")
        }
        let old_version_id = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_default_cf(),
                self.manager_config.get_hummock_version_id_key().as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockVersionId::from_be_bytes(v.as_slice().try_into().unwrap()))?;
        let mut hummock_version: HummockVersion = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_version_cf(),
                &old_version_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|s| HummockVersion::decode(s.as_slice()).unwrap())?;

        let mut write_batch: Vec<(&str, Vec<u8>, Vec<u8>, Epoch)> = vec![];
        // add tables
        tables.iter().for_each(|t: &Table| {
            write_batch.push((
                self.manager_config.get_hummock_table_cf(),
                t.sst_id.to_be_bytes().to_vec(),
                t.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            ))
        });
        let new_version_id = old_version_id + 1;

        // add tables for new_version
        let version_first_level = hummock_version.levels.first_mut().unwrap();
        match version_first_level.get_level_type() {
            LevelType::Overlapping => {
                tables
                    .iter()
                    .for_each(|t| version_first_level.table_ids.push(t.sst_id));
                write_batch.push((
                    self.manager_config.get_hummock_version_cf(),
                    new_version_id.to_be_bytes().to_vec(),
                    hummock_version.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                ));
            }
            LevelType::Nonoverlapping => {
                unimplemented!()
            }
        };

        let mut reserved_context_pinned_version: HummockContextPinnedVersion = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_context_pinned_version_cf(),
                &RESERVED_HUMMOCK_CONTEXT_ID.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedVersion::decode(v.as_slice()).unwrap())?;
        match reserved_context_pinned_version
            .version_id
            .iter()
            .position(|&v| v == old_version_id)
        {
            Some(pos) => {
                // unpin old_version pinned by the default context
                reserved_context_pinned_version.version_id.remove(pos);
            }
            None => {
                return Err(RwError::from(ErrorCode::HummockVersionNotPinned(
                    old_version_id,
                    RESERVED_HUMMOCK_CONTEXT_ID,
                )));
            }
        }
        // pin latest version by the default context
        reserved_context_pinned_version
            .version_id
            .push(new_version_id);
        write_batch.push((
            self.manager_config.get_hummock_context_pinned_version_cf(),
            RESERVED_HUMMOCK_CONTEXT_ID.to_be_bytes().to_vec(),
            reserved_context_pinned_version.encode_to_vec(),
            SINGLE_VERSION_EPOCH,
        ));

        // increase version counter
        write_batch.push((
            self.manager_config.get_hummock_default_cf(),
            self.manager_config
                .get_hummock_version_id_key()
                .as_bytes()
                .to_vec(),
            new_version_id.to_be_bytes().to_vec(),
            SINGLE_VERSION_EPOCH,
        ));

        // TODO write in transaction with context_id validation
        inner_guard.meta_store_ref.put_batch_cf(write_batch).await?;

        Ok(new_version_id)
    }

    async fn get_tables(
        &self,
        _context_id: HummockContextId,
        hummock_version: HummockVersion,
    ) -> Result<Vec<Table>> {
        let inner_guard = self.inner.read().await;
        let mut out: Vec<Table> = vec![];
        for level in hummock_version.levels {
            match level.get_level_type() {
                LevelType::Overlapping => {
                    let mut tables = inner_guard
                        .pick_few_tables(&self.manager_config, level.table_ids.as_slice())
                        .await?;
                    out.append(&mut tables);
                }
                LevelType::Nonoverlapping => {
                    let mut tables = inner_guard
                        .pick_few_tables(&self.manager_config, level.table_ids.as_slice())
                        .await?;
                    out.append(&mut tables);
                }
            }
        }
        Ok(out)
    }

    async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let new_snapshot_id = self
            .id_generator_manager_ref
            .generate(IdCategory::HummockContext)
            .await? as u64;
        let inner_guard = self.inner.write().await;
        let mut context_pinned_snapshot: HummockContextPinnedSnapshot = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedSnapshot::decode(v.as_slice()).unwrap())
            .unwrap_or(HummockContextPinnedSnapshot {
                snapshot_id: vec![],
            });
        context_pinned_snapshot.snapshot_id.push(new_snapshot_id);
        // TODO write in transaction with context_id validation
        inner_guard
            .meta_store_ref
            .put_cf(
                self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                &context_id.to_be_bytes(),
                &context_pinned_snapshot.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        Ok(HummockSnapshot {
            ts: new_snapshot_id,
        })
    }

    async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let inner_guard = self.inner.write().await;
        let mut context_pinned_snapshot: HummockContextPinnedSnapshot = inner_guard
            .meta_store_ref
            .get_cf(
                self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
            .map(|v| HummockContextPinnedSnapshot::decode(v.as_slice()).unwrap())?;
        let found = context_pinned_snapshot
            .snapshot_id
            .iter()
            .position(|&v| v == hummock_snapshot.ts);
        match found {
            Some(pos) => {
                context_pinned_snapshot.snapshot_id.remove(pos);
                // TODO write in transaction with context_id validation
                if context_pinned_snapshot.snapshot_id.is_empty() {
                    inner_guard
                        .meta_store_ref
                        .delete_cf(
                            self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                            &context_id.to_be_bytes(),
                            SINGLE_VERSION_EPOCH,
                        )
                        .await
                } else {
                    inner_guard
                        .meta_store_ref
                        .put_cf(
                            self.manager_config.get_hummock_context_pinned_snapshot_cf(),
                            &context_id.to_be_bytes(),
                            &context_pinned_snapshot.encode_to_vec(),
                            SINGLE_VERSION_EPOCH,
                        )
                        .await
                }
            }
            None => Err(RwError::from(ErrorCode::HummockSnapshotNotPinned(
                hummock_snapshot.ts,
                context_id,
            ))),
        }
    }
}
