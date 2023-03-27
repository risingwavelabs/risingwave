// Copyright 2023 RisingWave Labs
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
use std::sync::Arc;

use risingwave_common_service::observer_manager::{ObserverState, SubscribeHummock};
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManagerRef,
};
use risingwave_pb::catalog::Table;
use risingwave_pb::hummock::version_update_payload;
use risingwave_pb::meta::relation::RelationInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::UnboundedSender;

use crate::hummock::backup_reader::BackupReaderRef;
use crate::hummock::event_handler::HummockEvent;
use crate::hummock::write_limiter::WriteLimiterRef;

pub struct HummockObserverNode {
    filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    backup_reader: BackupReaderRef,
    write_limiter: WriteLimiterRef,
    version_update_sender: UnboundedSender<HummockEvent>,
    version: u64,
}

impl ObserverState for HummockObserverNode {
    type SubscribeType = SubscribeHummock;

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info.to_owned() {
            Info::RelationGroup(relation_group) => {
                for relation in relation_group.relations {
                    match relation.relation_info.unwrap() {
                        RelationInfo::Table(table_catalog) => {
                            assert!(
                                resp.version > self.version,
                                "resp version={:?}, current version={:?}",
                                resp.version,
                                self.version
                            );

                            self.handle_catalog_notification(resp.operation(), table_catalog);

                            self.version = resp.version;
                        }
                        _ => panic!("error type notification"),
                    };
                }
            }
            Info::HummockVersionDeltas(hummock_version_deltas) => {
                let _ = self
                    .version_update_sender
                    .send(HummockEvent::VersionUpdate(
                        version_update_payload::Payload::VersionDeltas(hummock_version_deltas),
                    ))
                    .inspect_err(|e| {
                        tracing::error!("unable to send version delta: {:?}", e);
                    });
            }

            Info::MetaBackupManifestId(id) => {
                self.backup_reader.try_refresh_manifest(id.id);
            }

            Info::HummockWriteLimits(write_limits) => {
                self.write_limiter
                    .update_write_limits(write_limits.write_limits);
            }

            _ => {
                panic!("error type notification");
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
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
            .send(HummockEvent::VersionUpdate(
                version_update_payload::Payload::PinnedVersion(
                    snapshot
                        .hummock_version
                        .expect("should get hummock version"),
                ),
            ))
            .inspect_err(|e| {
                tracing::error!("unable to send full version: {:?}", e);
            });
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
    }
}

impl HummockObserverNode {
    pub fn new(
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
        backup_reader: BackupReaderRef,
        version_update_sender: UnboundedSender<HummockEvent>,
        write_limiter: WriteLimiterRef,
    ) -> Self {
        Self {
            filter_key_extractor_manager,
            backup_reader,
            version_update_sender,
            version: 0,
            write_limiter,
        }
    }

    fn handle_catalog_snapshot(&mut self, tables: Vec<Table>) {
        let all_filter_key_extractors: HashMap<u32, Arc<FilterKeyExtractorImpl>> = tables
            .iter()
            .map(|t| (t.id, Arc::new(FilterKeyExtractorImpl::from_table(t))))
            .collect();
        self.filter_key_extractor_manager
            .sync(all_filter_key_extractors);
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        match operation {
            Operation::Add | Operation::Update => {
                self.filter_key_extractor_manager.update(
                    table_catalog.id,
                    Arc::new(FilterKeyExtractorImpl::from_table(&table_catalog)),
                );
            }

            Operation::Delete => {
                self.filter_key_extractor_manager.remove(table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}
