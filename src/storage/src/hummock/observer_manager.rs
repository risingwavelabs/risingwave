// Copyright 2022 RisingWave Labs
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

use risingwave_common::bitmap::Bitmap;
use risingwave_common::license::LicenseManager;
use risingwave_common_service::ObserverState;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_trace::TraceSpan;
use risingwave_pb::catalog::Table;
use risingwave_pb::id::TableId;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::serving_table_vnode_mappings::PbServingTableVnodeMapping;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{SubscribeResponse, TableCacheRefillPolicies};
use tokio::sync::mpsc::UnboundedSender;

use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::backup_reader::BackupReaderRef;
use crate::hummock::event_handler::HummockVersionUpdate;
use crate::hummock::write_limiter::WriteLimiterRef;

pub struct HummockObserverNode {
    compaction_catalog_manager: CompactionCatalogManagerRef,
    backup_reader: BackupReaderRef,
    write_limiter: WriteLimiterRef,
    version_update_sender: UnboundedSender<HummockVersionUpdate>,
    cache_refill_policy_sender: UnboundedSender<TableCacheRefillPolicies>,
    serving_table_vnode_mapping_sender: UnboundedSender<(Operation, HashMap<TableId, Bitmap>)>,
    version: u64,
}

impl ObserverState for HummockObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Hummock
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        match info.to_owned() {
            Info::ObjectGroup(object_group) => {
                for object in object_group.objects {
                    match object.object_info.unwrap() {
                        PbObjectInfo::Table(table_catalog) => {
                            self.handle_catalog_notification(resp.operation(), table_catalog);
                        }
                        info => panic!("invalid notification info: {info}"),
                    };
                }
                assert!(
                    resp.version > self.version,
                    "resp version={:?}, current version={:?}",
                    resp.version,
                    self.version
                );
                self.version = resp.version;
            }
            Info::HummockVersionDeltas(hummock_version_deltas) => {
                let _ = self
                    .version_update_sender
                    .send(HummockVersionUpdate::VersionDeltas(
                        hummock_version_deltas
                            .version_deltas
                            .iter()
                            .map(HummockVersionDelta::from_rpc_protobuf)
                            .collect(),
                    ))
                    .inspect_err(|e| {
                        tracing::error!(event = ?e.0, "unable to send version delta");
                    });
            }

            Info::MetaBackupManifestId(id) => {
                self.backup_reader.try_refresh_manifest(id.id);
            }

            Info::HummockWriteLimits(write_limits) => {
                self.write_limiter
                    .update_write_limits(write_limits.write_limits);
            }

            Info::ClusterResource(resource) => {
                LicenseManager::get().update_cluster_resource(resource);
            }
            Info::ServingTableVnodeMappings(mappings) => {
                self.handle_serving_table_vnode_mappings(resp.operation(), mappings.mappings);
            }
            Info::TableCacheRefillPolicies(policies) => {
                self.handle_table_cache_refill_policies(policies);
            }
            info => {
                panic!("invalid notification info: {info}");
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };

        self.handle_catalog_snapshot(snapshot.tables);
        self.backup_reader.try_refresh_manifest(
            snapshot
                .meta_backup_manifest_id
                .expect("should get meta backup manifest id")
                .id,
        );
        self.write_limiter.update_write_limits(
            snapshot
                .hummock_write_limits
                .expect("should get hummock_write_limits")
                .write_limits,
        );
        let _ = self
            .version_update_sender
            .send(HummockVersionUpdate::PinnedVersion(Box::new(
                HummockVersion::from_rpc_protobuf(
                    &snapshot
                        .hummock_version
                        .expect("should get hummock version"),
                ),
            )))
            .inspect_err(|e| {
                tracing::error!(event = ?e.0, "unable to send full version");
            });
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
        LicenseManager::get().update_cluster_resource(snapshot.cluster_resource.unwrap());

        self.handle_table_cache_refill_policies(
            snapshot.table_cache_refill_policies.unwrap_or_default(),
        );

        self.handle_serving_table_vnode_mappings(
            Operation::Snapshot,
            snapshot
                .serving_table_vnode_mappings
                .unwrap_or_default()
                .mappings,
        );
    }
}

impl HummockObserverNode {
    pub fn new(
        compaction_catalog_manager: CompactionCatalogManagerRef,
        backup_reader: BackupReaderRef,
        version_update_sender: UnboundedSender<HummockVersionUpdate>,
        cache_refill_policy_sender: UnboundedSender<TableCacheRefillPolicies>,
        serving_table_vnode_mapping_sender: UnboundedSender<(Operation, HashMap<TableId, Bitmap>)>,
        write_limiter: WriteLimiterRef,
    ) -> Self {
        Self {
            compaction_catalog_manager,
            backup_reader,
            version_update_sender,
            cache_refill_policy_sender,
            serving_table_vnode_mapping_sender,
            version: 0,
            write_limiter,
        }
    }

    fn handle_catalog_snapshot(&mut self, tables: Vec<Table>) {
        self.compaction_catalog_manager
            .sync(tables.into_iter().map(|t| (t.id, t)).collect());
    }

    fn handle_serving_table_vnode_mappings(
        &self,
        op: Operation,
        mappings: Vec<PbServingTableVnodeMapping>,
    ) {
        let mappings = mappings
            .into_iter()
            .map(|mapping| {
                (
                    TableId::from(mapping.table_id),
                    Bitmap::from(
                        mapping
                            .bitmap
                            .expect("serving table vnode bitmap must exist"),
                    ),
                )
            })
            .collect();

        tracing::debug!(
            ?op,
            ?mappings,
            "receive serving table vnode mappings updates"
        );

        let _ = self
            .serving_table_vnode_mapping_sender
            .send((op, mappings))
            .inspect_err(|e| {
                tracing::error!(
                    op = ?e.0.0,
                    mappings = ?e.0.1,
                    "unable to send serving table vnode mappings"
                );
            });
    }

    fn handle_table_cache_refill_policies(&self, policies: TableCacheRefillPolicies) {
        tracing::debug!(?policies, "receive table cache refill policy updates");

        let _ = self
            .cache_refill_policy_sender
            .send(policies)
            .inspect_err(|e| {
                tracing::error!(
                    policies = ?e.0,
                    "unable to send table cache refill policies"
                );
            });
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        match operation {
            Operation::Add | Operation::Update => {
                self.compaction_catalog_manager
                    .update(table_catalog.id, table_catalog);
            }

            Operation::Delete => {
                self.compaction_catalog_manager.remove(table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_common_service::ObserverState;
    use risingwave_pb::backup_service::MetaBackupManifestId;
    use risingwave_pb::hummock::{PbHummockVersion, WriteLimits};
    use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
    use risingwave_pb::meta::subscribe_response::{Info, Operation};
    use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
    use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
    use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, TableCacheRefillPolicies};
    use tokio::sync::mpsc::unbounded_channel;

    use super::HummockObserverNode;
    use crate::compaction_catalog_manager::CompactionCatalogManager;
    use crate::hummock::backup_reader::BackupReader;
    use crate::hummock::write_limiter::WriteLimiter;

    fn policy_snapshot(catalog_version: u64, table_id: u32) -> SubscribeResponse {
        SubscribeResponse {
            info: Some(Info::Snapshot(MetaSnapshot {
                hummock_version: Some(PbHummockVersion::default()),
                version: Some(SnapshotVersion {
                    catalog_version,
                    ..Default::default()
                }),
                meta_backup_manifest_id: Some(MetaBackupManifestId { id: 0 }),
                hummock_write_limits: Some(WriteLimits {
                    write_limits: HashMap::new(),
                }),
                cluster_resource: Some(Default::default()),
                table_cache_refill_policies: Some(TableCacheRefillPolicies {
                    policies: vec![PbTableCacheRefillPolicy {
                        table_id,
                        policy: PbCacheRefillPolicy::Serving as i32,
                    }],
                }),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_resubscribe_snapshot_refreshes_table_cache_refill_policies() {
        let (version_update_tx, mut version_update_rx) = unbounded_channel();
        let (cache_refill_policy_tx, mut cache_refill_policy_rx) = unbounded_channel();
        let (serving_table_vnode_mapping_tx, mut serving_table_vnode_mapping_rx) =
            unbounded_channel();
        let mut observer = HummockObserverNode::new(
            Arc::new(CompactionCatalogManager::default()),
            BackupReader::unused().await,
            version_update_tx,
            cache_refill_policy_tx,
            serving_table_vnode_mapping_tx,
            WriteLimiter::unused(),
        );

        observer.handle_initialization_notification(policy_snapshot(1, 233));
        assert_eq!(
            cache_refill_policy_rx.recv().await.unwrap().policies[0].table_id,
            233
        );
        let (op, mappings) = serving_table_vnode_mapping_rx.recv().await.unwrap();
        assert_eq!(op, Operation::Snapshot);
        assert!(mappings.is_empty());
        version_update_rx.recv().await.unwrap();

        observer.handle_initialization_notification(policy_snapshot(2, 234));
        assert_eq!(
            cache_refill_policy_rx.recv().await.unwrap().policies[0].table_id,
            234
        );
        let (op, mappings) = serving_table_vnode_mapping_rx.recv().await.unwrap();
        assert_eq!(op, Operation::Snapshot);
        assert!(mappings.is_empty());
        version_update_rx.recv().await.unwrap();
    }
}
