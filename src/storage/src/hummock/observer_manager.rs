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
            Info::TableCacheRefillPolicies(policies) => {
                tracing::info!(?policies, "receive table cache refill policies updates");
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
            Info::ServingTableVnodeMappings(mappings) => {
                let mappings = mappings
                    .mappings
                    .into_iter()
                    .map(|mapping| {
                        (
                            TableId::from(mapping.table_id),
                            Bitmap::from(
                                mapping
                                    .bitmap
                                    .expect("serving table vnode bitmap cannot inexists"),
                            ),
                        )
                    })
                    .collect();

                let op = resp.operation();
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
