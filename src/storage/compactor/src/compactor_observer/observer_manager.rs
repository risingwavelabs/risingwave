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

use risingwave_common::system_param::local_manager::LocalSystemParamManagerRef;
use risingwave_common_service::observer_manager::{ObserverState, SubscribeCompactor};
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManagerRef,
};
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;

pub struct CompactorObserverNode {
    filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    system_param_manager: LocalSystemParamManagerRef,
    version: u64,
}

impl ObserverState for CompactorObserverNode {
    type SubscribeType = SubscribeCompactor;

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info.to_owned() {
            Info::Table(table_catalog) => {
                assert!(
                    resp.version > self.version,
                    "resp version={:?}, current version={:?}",
                    resp.version,
                    self.version
                );

                self.handle_catalog_notification(resp.operation(), table_catalog);

                self.version = resp.version;
            }
            Info::HummockVersionDeltas(_) => {}
            Info::SystemParams(p) => {
                self.system_param_manager.try_set_params(p);
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
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
    }
}

impl CompactorObserverNode {
    pub fn new(
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
        system_param_manager: LocalSystemParamManagerRef,
    ) -> Self {
        Self {
            filter_key_extractor_manager,
            system_param_manager,
            version: 0,
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
