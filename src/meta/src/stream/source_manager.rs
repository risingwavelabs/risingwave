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

mod split_assignment;
mod worker;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::panic_if_debug;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::cdc::SchemaChangeFailurePolicy;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceEnumeratorInfo, SplitId, SplitImpl,
    SplitMetaData, fill_adaptive_split,
};
use risingwave_meta_model::SourceId;
use risingwave_pb::catalog::Source;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use serde_json;
pub use split_assignment::{SplitDiffOptions, SplitState, reassign_splits};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, MutexGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};
pub use worker::create_source_worker;
use worker::{ConnectorSourceWorkerHandle, create_source_worker_async};

use crate::MetaResult;
use crate::barrier::{BarrierScheduler, Command, ReplaceStreamJobPlan, SharedActorInfos};
use crate::error::MetaError;
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, FragmentId, StreamJobFragments};
use crate::rpc::metrics::MetaMetrics;

pub type SourceManagerRef = Arc<SourceManager>;
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;
pub type DiscoveredSourceSplits = HashMap<SourceId, Vec<SplitImpl>>;
pub type ThrottleConfig = HashMap<FragmentId, HashMap<ActorId, Option<u32>>>;
// ALTER CONNECTOR parameters, specifying the new parameters to be set for each job_id (source_id/sink_id)
pub type ConnectorPropsChange = HashMap<u32, HashMap<String, String>>;

const DEFAULT_SOURCE_TICK_TIMEOUT: Duration = Duration::from_secs(10);

/// `SourceManager` keeps fetching the latest split metadata from the external source services ([`worker::ConnectorSourceWorker::tick`]),
/// and sends a split assignment command if split changes detected ([`Self::tick`]).
pub struct SourceManager {
    pub paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler,
    core: Mutex<SourceManagerCore>,
    pub metrics: Arc<MetaMetrics>,
}
pub struct SourceManagerCore {
    metadata_manager: MetadataManager,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// `source_id` -> `(fragment_id, upstream_fragment_id)`
    backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,

    /// Table ID -> Schema Change Failure Policy mapping
    /// Key: `TableId`, Value: `SchemaChangeFailurePolicy`
    cdc_table_schema_change_policies: HashMap<String, SchemaChangeFailurePolicy>,

    env: MetaSrvEnv,
}

pub struct SourceManagerRunningInfo {
    pub source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    pub backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
}

impl SourceManagerCore {
    fn new(
        metadata_manager: MetadataManager,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            cdc_table_schema_change_policies: HashMap::new(),
            env,
        }
    }

    /// Get the current table schema change policies mapping
    pub fn get_cdc_table_schema_change_policies(
        &self,
    ) -> &HashMap<String, SchemaChangeFailurePolicy> {
        &self.cdc_table_schema_change_policies
    }

    /// Print current table schema change policies for debugging
    pub fn debug_print_table_schema_policies(&self) {
        tracing::info!(
            "SourceManagerCore cdc_table_schema_change_policies count: {}, policies: {:?}",
            self.cdc_table_schema_change_policies.len(),
            self.cdc_table_schema_change_policies
        );
    }

    /// Add a table schema change policy for a CDC table
    pub fn add_table_schema_policy(
        &mut self,
        cdc_table_id: String,
        source_id: SourceId,
        policy: SchemaChangeFailurePolicy,
    ) -> Result<(), String> {
        tracing::info!(
            "Adding CDC table schema change policy: cdc_table_id={}, source_id={}, policy={:?}",
            cdc_table_id,
            source_id,
            policy
        );
        self.cdc_table_schema_change_policies
            .insert(cdc_table_id, policy);
        self.debug_print_table_schema_policies();

        // Serialize the policies mapping to JSON
        let policies_json = serde_json::to_string(&self.cdc_table_schema_change_policies)
            .map_err(|e| format!("Failed to serialize policies: {}", e.as_report()))?;

        tracing::info!(
            "Serialized CDC table schema change policies for source_id={}: {}",
            source_id,
            policies_json
        );

        Ok(())
    }

    /// Remove a table schema change policy for a CDC table
    pub fn remove_table_schema_policy(
        &mut self,
        cdc_table_id: String,
        source_id: SourceId,
    ) -> Result<(), String> {
        tracing::info!(
            "Removing CDC table schema change policy: cdc_table_id={}, source_id={}",
            cdc_table_id,
            source_id
        );
        let removed = self.cdc_table_schema_change_policies.remove(&cdc_table_id);
        if removed.is_some() {
            tracing::info!(
                "Successfully removed policy for cdc_table_id={}",
                cdc_table_id
            );
        } else {
            tracing::warn!(
                "No policy found for cdc_table_id={}, nothing to remove",
                cdc_table_id
            );
        }
        self.debug_print_table_schema_policies();

        // Serialize the updated policies mapping to JSON
        let policies_json = serde_json::to_string(&self.cdc_table_schema_change_policies)
            .map_err(|e| format!("Failed to serialize policies: {}", e.as_report()))?;

        tracing::info!(
            "Serialized CDC table schema change policies for source_id={}: {}",
            source_id,
            policies_json
        );

        Ok(())
    }

    /// Updates states after all kinds of source change.
    pub fn apply_source_change(&mut self, source_change: SourceChange) {
        let mut added_source_fragments = Default::default();
        let mut added_backfill_fragments = Default::default();
        let mut finished_backfill_fragments = Default::default();
        let mut fragment_replacements = Default::default();
        let mut dropped_source_fragments = Default::default();
        let mut dropped_source_ids = Default::default();
        let mut recreate_source_id_map_new_props: Vec<(u32, HashMap<String, String>)> =
            Default::default();

        match source_change {
            SourceChange::CreateJob {
                added_source_fragments: added_source_fragments_,
                added_backfill_fragments: added_backfill_fragments_,
            } => {
                added_source_fragments = added_source_fragments_;
                added_backfill_fragments = added_backfill_fragments_;
            }
            SourceChange::CreateJobFinished {
                finished_backfill_fragments: finished_backfill_fragments_,
            } => {
                finished_backfill_fragments = finished_backfill_fragments_;
            }

            SourceChange::DropMv {
                dropped_source_fragments: dropped_source_fragments_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
            }
            SourceChange::ReplaceJob {
                dropped_source_fragments: dropped_source_fragments_,
                added_source_fragments: added_source_fragments_,
                fragment_replacements: fragment_replacements_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
                added_source_fragments = added_source_fragments_;
                fragment_replacements = fragment_replacements_;
            }
            SourceChange::DropSource {
                dropped_source_ids: dropped_source_ids_,
            } => {
                dropped_source_ids = dropped_source_ids_;
            }

            SourceChange::UpdateSourceProps {
                source_id_map_new_props,
            } => {
                for (source_id, new_props) in source_id_map_new_props {
                    recreate_source_id_map_new_props.push((source_id, new_props));
                }
            }
        }

        for source_id in dropped_source_ids {
            let dropped_fragments = self.source_fragments.remove(&source_id);

            if let Some(handle) = self.managed_sources.remove(&source_id) {
                handle.terminate(dropped_fragments);
            }
            if let Some(_fragments) = self.backfill_fragments.remove(&source_id) {
                // TODO: enable this assertion after we implemented cleanup for backfill fragments
                // debug_assert!(
                //     fragments.is_empty(),
                //     "when dropping source, there should be no backfill fragments, got: {:?}",
                //     fragments
                // );
            }
        }

        for (source_id, fragments) in added_source_fragments {
            self.source_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (source_id, fragments) in added_backfill_fragments {
            self.backfill_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (source_id, fragments) in finished_backfill_fragments {
            let handle = self.managed_sources.get(&source_id).unwrap_or_else(|| {
                panic!(
                    "source {} not found when adding backfill fragments {:?}",
                    source_id, fragments
                );
            });
            handle.finish_backfill(fragments.iter().map(|(id, _up_id)| *id).collect());
        }

        for (source_id, fragment_ids) in dropped_source_fragments {
            self.drop_source_fragments(Some(source_id), fragment_ids);
        }

        for (old_fragment_id, new_fragment_id) in fragment_replacements {
            // TODO: add source_id to the fragment_replacements to avoid iterating all sources
            self.drop_source_fragments(None, BTreeSet::from([old_fragment_id]));

            for fragment_ids in self.backfill_fragments.values_mut() {
                let mut new_backfill_fragment_ids = fragment_ids.clone();
                for (fragment_id, upstream_fragment_id) in fragment_ids.iter() {
                    assert_ne!(
                        fragment_id, upstream_fragment_id,
                        "backfill fragment should not be replaced"
                    );
                    if *upstream_fragment_id == old_fragment_id {
                        new_backfill_fragment_ids.remove(&(*fragment_id, *upstream_fragment_id));
                        new_backfill_fragment_ids.insert((*fragment_id, new_fragment_id));
                    }
                }
                *fragment_ids = new_backfill_fragment_ids;
            }
        }

        for (source_id, new_props) in recreate_source_id_map_new_props {
            tracing::info!("recreate source {source_id} in source manager");
            if let Some(handle) = self.managed_sources.get_mut(&(source_id as _)) {
                // the update here should not involve fragments change and split change
                // Or we need to drop and recreate the source worker instead of updating inplace
                let props_wrapper =
                    WithOptionsSecResolved::without_secrets(new_props.into_iter().collect());
                let props = ConnectorProperties::extract(props_wrapper, false).unwrap(); // already checked when sending barrier
                handle.update_props(props);
            }
        }
    }

    fn drop_source_fragments(
        &mut self,
        source_id: Option<SourceId>,
        dropped_fragment_ids: BTreeSet<FragmentId>,
    ) {
        if let Some(source_id) = source_id {
            if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                let mut dropped_ids = vec![];
                let managed_fragment_ids = entry.get_mut();
                for fragment_id in &dropped_fragment_ids {
                    managed_fragment_ids.remove(fragment_id);
                    dropped_ids.push(*fragment_id);
                }
                if let Some(handle) = self.managed_sources.get(&source_id) {
                    handle.drop_fragments(dropped_ids);
                } else {
                    panic_if_debug!(
                        "source {source_id} not found when dropping fragment {dropped_ids:?}",
                    );
                }
                if managed_fragment_ids.is_empty() {
                    entry.remove();
                }
            }
        } else {
            for (source_id, fragment_ids) in &mut self.source_fragments {
                let mut dropped_ids = vec![];
                for fragment_id in &dropped_fragment_ids {
                    if fragment_ids.remove(fragment_id) {
                        dropped_ids.push(*fragment_id);
                    }
                }
                if !dropped_ids.is_empty() {
                    if let Some(handle) = self.managed_sources.get(source_id) {
                        handle.drop_fragments(dropped_ids);
                    } else {
                        panic_if_debug!(
                            "source {source_id} not found when dropping fragment {dropped_ids:?}",
                        );
                    }
                }
            }
        }
    }

    async fn update_source_splits(&self, source_id: SourceId) -> MetaResult<()> {
        let handle_ref = self.managed_sources.get(&source_id).unwrap();

        let discovered_splits = handle_ref.splits.lock().await.splits.clone();

        if let Some(splits) = discovered_splits {
            let source_splits =
                HashMap::from([(source_id as _, splits.into_values().collect_vec())]);
            self.metadata_manager
                .update_source_splits(&source_splits)
                .await?;
        }
        Ok(())
    }
}

impl SourceManager {
    const DEFAULT_SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        barrier_scheduler: BarrierScheduler,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
        env: MetaSrvEnv,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = metadata_manager.list_sources().await?;
            for source in sources {
                create_source_worker_async(source, &mut managed_sources, metrics.clone())?
            }
        }

        let source_fragments = metadata_manager
            .catalog_controller
            .load_source_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids.into_iter().map(|id| id as _).collect(),
                )
            })
            .collect();
        let backfill_fragments = metadata_manager
            .catalog_controller
            .load_backfill_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids
                        .into_iter()
                        .map(|(id, up_id)| (id as _, up_id as _))
                        .collect(),
                )
            })
            .collect();

        // Recover CDC table schema change policies from catalog
        let cdc_table_schema_change_policies =
            Self::recover_cdc_table_policies_from_catalog(&metadata_manager).await?;

        let mut core = SourceManagerCore::new(
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            env,
        );

        // Set recovered policies
        core.cdc_table_schema_change_policies = cdc_table_schema_change_policies.clone();

        let source_manager = Self {
            barrier_scheduler,
            core: Mutex::new(core),
            paused: Mutex::new(()),
            metrics,
        };

        Ok(source_manager)
    }

    /// Recover CDC table schema change policies from catalog on Meta startup
    async fn recover_cdc_table_policies_from_catalog(
        metadata_manager: &MetadataManager,
    ) -> MetaResult<HashMap<String, SchemaChangeFailurePolicy>> {
        tracing::info!(
            "Starting recovery of CDC table schema change failure policies from catalog"
        );

        let tables = metadata_manager
            .catalog_controller
            .list_all_state_tables()
            .await?;
        tracing::info!(
            total_tables = tables.len(),
            "Loaded all state tables from catalog for recovery"
        );

        let mut policies = HashMap::new();
        let mut cdc_tables_count = 0;
        let mut cdc_tables_with_policy_count = 0;

        for table in &tables {
            // Only process CDC tables with cdc_table_id and policy set
            if let Some(ref cdc_table_id) = table.cdc_table_id {
                cdc_tables_count += 1;

                if let Some(pb_policy) = table.cdc_schema_change_failure_policy {
                    cdc_tables_with_policy_count += 1;

                    // Convert from proto enum to connector enum
                    use risingwave_pb::catalog::SchemaChangeFailurePolicy as PbPolicy;
                    let policy = match PbPolicy::try_from(pb_policy) {
                        Ok(PbPolicy::Block) => SchemaChangeFailurePolicy::Block,
                        Ok(PbPolicy::Skip) | Ok(PbPolicy::Unspecified) => {
                            SchemaChangeFailurePolicy::Skip
                        }
                        Err(_) => {
                            tracing::warn!(
                                cdc_table_id = cdc_table_id,
                                pb_policy = pb_policy,
                                "Invalid CDC schema change failure policy in catalog, using default policy (Skip)."
                            );
                            SchemaChangeFailurePolicy::Skip
                        }
                    };

                    tracing::info!(
                        table_id = table.id,
                        table_name = &table.name,
                        cdc_table_id = cdc_table_id,
                        policy = ?policy,
                        "Recovered CDC table schema change policy from catalog"
                    );

                    policies.insert(cdc_table_id.clone(), policy);
                } else {
                    tracing::debug!(
                        table_id = table.id,
                        table_name = &table.name,
                        cdc_table_id = cdc_table_id,
                        "CDC table has no schema change failure policy set, will use source-level or default"
                    );
                }
            }
        }

        tracing::info!(
            total_tables = tables.len(),
            cdc_tables = cdc_tables_count,
            cdc_tables_with_policy = cdc_tables_with_policy_count,
            recovered_policies = policies.len(),
            policies = ?policies,
            "Completed recovery of CDC table schema change failure policies from catalog"
        );

        Ok(policies)
    }

    pub async fn validate_source_once(
        &self,
        source_id: u32,
        new_source_props: WithOptionsSecResolved,
    ) -> MetaResult<()> {
        let props = ConnectorProperties::extract(new_source_props, false).unwrap();

        {
            let mut enumerator = props
                .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                    metrics: self.metrics.source_enumerator_metrics.clone(),
                    info: SourceEnumeratorInfo { source_id },
                }))
                .await
                .context("failed to create SplitEnumerator")?;

            let _ = tokio::time::timeout(DEFAULT_SOURCE_TICK_TIMEOUT, enumerator.list_splits())
                .await
                .context("failed to list splits")??;
        }
        Ok(())
    }

    /// For replacing job (alter table/source, create sink into table).
    #[await_tree::instrument]
    pub async fn handle_replace_job(
        &self,
        dropped_job_fragments: &StreamJobFragments,
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        replace_plan: &ReplaceStreamJobPlan,
    ) {
        // Extract the fragments that include source operators.
        let dropped_source_fragments = dropped_job_fragments.stream_source_fragments().clone();

        self.apply_source_change(SourceChange::ReplaceJob {
            dropped_source_fragments,
            added_source_fragments,
            fragment_replacements: replace_plan.fragment_replacements(),
        })
        .await;
    }

    /// Updates states after all kinds of source change.
    /// e.g., split change (`post_collect` barrier) or scaling (`post_apply_reschedule`).
    #[await_tree::instrument("apply_source_change({source_change})")]
    pub async fn apply_source_change(&self, source_change: SourceChange) {
        let mut core = self.core.lock().await;
        core.apply_source_change(source_change);
    }

    /// create and register connector worker for source.
    #[await_tree::instrument("register_source({})", source.name)]
    pub async fn register_source(&self, source: &Source) -> MetaResult<()> {
        tracing::debug!("register_source: {}", source.get_id());
        let mut core = self.core.lock().await;
        let source_id = source.get_id() as _;
        if core.managed_sources.contains_key(&source_id) {
            tracing::warn!("source {} already registered", source_id);
            return Ok(());
        }

        let handle = create_source_worker(source, self.metrics.clone())
            .await
            .context("failed to create source worker")?;
        core.managed_sources.insert(source_id, handle);
        core.update_source_splits(source_id).await?;
        Ok(())
    }

    /// register connector worker for source.
    pub async fn register_source_with_handle(
        &self,
        source_id: SourceId,
        handle: ConnectorSourceWorkerHandle,
    ) -> MetaResult<()> {
        let mut core = self.core.lock().await;
        if core.managed_sources.contains_key(&source_id) {
            tracing::warn!("source {} already registered", source_id);
            return Ok(());
        }
        core.managed_sources.insert(source_id, handle);
        core.update_source_splits(source_id).await?;

        Ok(())
    }

    pub async fn get_running_info(&self) -> SourceManagerRunningInfo {
        let core = self.core.lock().await;

        SourceManagerRunningInfo {
            source_fragments: core.source_fragments.clone(),
            backfill_fragments: core.backfill_fragments.clone(),
        }
    }

    /// Checks whether the external source metadata has changed, and sends a split assignment command
    /// if it has.
    ///
    /// This is also how a newly created `SourceExecutor` is initialized.
    /// (force `tick` in `Self::create_source_worker`)
    ///
    /// The command will first updates `SourceExecutor`'s splits, and finally calls `Self::apply_source_change`
    /// to update states in `SourceManager`.
    async fn tick(&self) -> MetaResult<()> {
        let split_states = {
            let core_guard = self.core.lock().await;
            core_guard.reassign_splits().await?
        };

        for (database_id, split_state) in split_states {
            if !split_state.split_assignment.is_empty() {
                let command = Command::SourceChangeSplit(split_state);
                tracing::info!(command = ?command, "pushing down split assignment command");
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    error = %e.as_report(),
                    "error happened while running source manager tick",
                );
            }
        }
    }

    /// Add a CDC table schema change policy and trigger `ConnectorPropsChange`
    pub async fn add_cdc_table_schema_policy(
        &self,
        cdc_table_id: String,
        source_id: SourceId,
        policy: SchemaChangeFailurePolicy,
    ) -> MetaResult<()> {
        tracing::info!(
            cdc_table_id = cdc_table_id,
            source_id = source_id,
            policy = ?policy,
            "META: add_cdc_table_schema_policy called"
        );

        // First, add the policy to the mapping
        {
            let mut core = self.core.lock().await;
            core.add_table_schema_policy(cdc_table_id.clone(), source_id, policy)
                .map_err(|e| {
                    MetaError::invalid_parameter(format!(
                        "Failed to add table schema policy: {}",
                        e
                    ))
                })?;
        }
        // Get the existing source properties and add the policies
        let sources = {
            let core = self.core.lock().await;
            core.metadata_manager.list_sources().await?
        };
        let source = sources.iter().find(|s| s.id == source_id as u32);
        if let Some(source) = source {
            // Get the policies JSON
            let policies_json = {
                let core = self.core.lock().await;
                serde_json::to_string(core.get_cdc_table_schema_change_policies()).map_err(|e| {
                    MetaError::invalid_parameter(format!(
                        "Failed to serialize policies: {}",
                        e.as_report()
                    ))
                })?
            };
            // Get existing source properties
            let mut props: HashMap<String, String> =
                source.with_properties.clone().into_iter().collect();
            // Add the CDC table schema change policies
            props.insert("cdc_table_schema_change_policies".to_owned(), policies_json);

            // Update fragment table's StreamNode.with_properties
            // This ensures CN recovery can read the latest policies from fragments
            {
                let core = self.core.lock().await;
                let is_shared = source.info.as_ref().is_some_and(|info| info.is_shared());
                core.metadata_manager
                    .catalog_controller
                    .update_source_props_fragments_by_source_id(
                        source_id,
                        props.clone().into_iter().collect(),
                        is_shared,
                    )
                    .await?;
            }

            tracing::info!(
                source_id = source_id,
                "META: Updated fragment table's StreamNode.with_properties for add_cdc_table_schema_policy"
            );

            // Get the database_id for this source
            let database_id = {
                let core = self.core.lock().await;
                core.metadata_manager
                    .catalog_controller
                    .get_object_database_id(source_id)
                    .await?
            };

            // Send ConnectorPropsChange command directly to propagate to CN
            let command =
                Command::ConnectorPropsChange(HashMap::from([(source_id as u32, props.clone())]));
            tracing::info!(
                source_id = source_id,
                database_id = database_id,
                props_keys = ?props.keys().collect::<Vec<_>>(),
                cdc_policies = props.get("cdc_table_schema_change_policies"),
                "META: Sending ConnectorPropsChange command to barrier scheduler"
            );

            match self
                .barrier_scheduler
                .run_command(DatabaseId::new(database_id as u32), command)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        source_id = source_id,
                        database_id = database_id,
                        "META: Successfully sent ConnectorPropsChange command"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        source_id = source_id,
                        database_id = database_id,
                        error = %e.as_report(),
                        "META: Failed to send ConnectorPropsChange command"
                    );
                }
            }
        } else {
            tracing::warn!(
                "Source {} not found when trying to propagate CDC table schema change policies",
                source_id
            );
        }

        tracing::info!(
            "CDC table schema change policies updated for source_id={} and propagated via ConnectorPropsChange",
            source_id
        );

        Ok(())
    }

    /// Remove a CDC table schema change policy and trigger `ConnectorPropsChange`
    pub async fn remove_cdc_table_schema_policy(
        &self,
        cdc_table_id: String,
        source_id: SourceId,
    ) -> MetaResult<()> {
        tracing::info!(
            cdc_table_id = cdc_table_id,
            source_id = source_id,
            "META: remove_cdc_table_schema_policy called"
        );

        // First, remove the policy from the mapping
        {
            let mut core = self.core.lock().await;
            core.remove_table_schema_policy(cdc_table_id.clone(), source_id)
                .map_err(|e| {
                    MetaError::invalid_parameter(format!(
                        "Failed to remove table schema policy: {}",
                        e
                    ))
                })?;
        }

        // Get the existing source properties and add the updated policies
        let sources = {
            let core = self.core.lock().await;
            core.metadata_manager.list_sources().await?
        };
        let source = sources.iter().find(|s| s.id == source_id as u32);

        if let Some(source) = source {
            // Get the updated policies JSON
            let policies_json = {
                let core = self.core.lock().await;
                serde_json::to_string(core.get_cdc_table_schema_change_policies()).map_err(|e| {
                    MetaError::invalid_parameter(format!(
                        "Failed to serialize policies: {}",
                        e.as_report()
                    ))
                })?
            };

            // Get existing source properties
            let mut props: HashMap<String, String> =
                source.with_properties.clone().into_iter().collect();
            // Add the updated CDC table schema change policies
            props.insert("cdc_table_schema_change_policies".to_owned(), policies_json);

            // Update fragment table's StreamNode.with_properties
            // This ensures CN recovery can read the latest policies from fragments
            {
                let core = self.core.lock().await;
                let is_shared = source.info.as_ref().is_some_and(|info| info.is_shared());
                core.metadata_manager
                    .catalog_controller
                    .update_source_props_fragments_by_source_id(
                        source_id,
                        props.clone().into_iter().collect(),
                        is_shared,
                    )
                    .await?;
            }

            tracing::info!(
                source_id = source_id,
                "META: Updated fragment table's StreamNode.with_properties for remove_cdc_table_schema_policy"
            );

            // Get the database_id for this source
            let database_id = {
                let core = self.core.lock().await;
                core.metadata_manager
                    .catalog_controller
                    .get_object_database_id(source_id)
                    .await?
            };

            // Send ConnectorPropsChange command directly to propagate to CN
            let command =
                Command::ConnectorPropsChange(HashMap::from([(source_id as u32, props.clone())]));
            tracing::info!(
                source_id = source_id,
                database_id = database_id,
                props_keys = ?props.keys().collect::<Vec<_>>(),
                cdc_policies = props.get("cdc_table_schema_change_policies"),
                "META: Sending ConnectorPropsChange command to barrier scheduler"
            );

            match self
                .barrier_scheduler
                .run_command(DatabaseId::new(database_id as u32), command)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        source_id = source_id,
                        database_id = database_id,
                        "META: Successfully sent ConnectorPropsChange command"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        source_id = source_id,
                        database_id = database_id,
                        error = %e.as_report(),
                        "META: Failed to send ConnectorPropsChange command"
                    );
                }
            }
        } else {
            tracing::warn!(
                "Source {} not found when trying to propagate updated CDC table schema change policies",
                source_id
            );
        }

        tracing::info!(
            "CDC table schema change policies updated for source_id={} and propagated via ConnectorPropsChange",
            source_id
        );

        Ok(())
    }

    /// Pause the tick loop in source manager until the returned guard is dropped.
    pub async fn pause_tick(&self) -> MutexGuard<'_, ()> {
        tracing::debug!("pausing tick lock in source manager");
        self.paused.lock().await
    }
}

#[derive(strum::Display)]
pub enum SourceChange {
    /// `CREATE SOURCE` (shared), or `CREATE MV`.
    /// This is applied after the job is successfully created (`post_collect` barrier).
    CreateJob {
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        /// (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`))
        added_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    },
    UpdateSourceProps {
        // the new properties to be set for each source_id
        // and the props should not affect split assignment and fragments
        source_id_map_new_props: HashMap<u32, HashMap<String, String>>,
    },
    /// `CREATE SOURCE` (shared), or `CREATE MV` is _finished_ (backfill is done).
    /// This is applied after `wait_streaming_job_finished`.
    /// XXX: Should we merge `CreateJob` into this?
    CreateJobFinished {
        /// (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`))
        finished_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    },
    /// `DROP SOURCE` or `DROP MV`
    DropSource { dropped_source_ids: Vec<SourceId> },
    DropMv {
        // FIXME: we should consider source backfill fragments here for MV on shared source.
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    },
    ReplaceJob {
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        fragment_replacements: HashMap<FragmentId, FragmentId>,
    },
}

pub fn build_actor_connector_splits(
    splits: &HashMap<ActorId, Vec<SplitImpl>>,
) -> HashMap<u32, ConnectorSplits> {
    splits
        .iter()
        .map(|(&actor_id, splits)| {
            (
                actor_id,
                ConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                },
            )
        })
        .collect()
}

pub fn build_actor_split_impls(
    actor_splits: &HashMap<u32, ConnectorSplits>,
) -> HashMap<ActorId, Vec<SplitImpl>> {
    actor_splits
        .iter()
        .map(|(actor_id, ConnectorSplits { splits })| {
            (
                *actor_id,
                splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .collect(),
            )
        })
        .collect()
}
