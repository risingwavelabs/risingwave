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
use std::sync::Arc;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManagerRef,
};
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;

pub struct CompactorObserverNode {
    filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    version: u64,
}

impl ObserverNodeImpl for CompactorObserverNode {
    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        assert!(
            resp.version > self.version,
            "resp version={:?}, current version={:?}",
            resp.version,
            self.version
        );

        match info.to_owned() {
            Info::Table(table_catalog) => {
                self.handle_catalog_notification(resp.operation(), table_catalog);
            }

            Info::HummockVersionDeltas(_) => {}

            _ => {
                panic!("error type notification");
            }
        }

        self.version = resp.version;
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        match resp.info {
            Some(Info::Snapshot(snapshot)) => {
                self.handle_catalog_snapshot(snapshot.tables);
                self.version = resp.version;
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be compactor snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }

        Ok(())
    }
}

impl CompactorObserverNode {
    pub fn new(filter_key_extractor_manager: FilterKeyExtractorManagerRef) -> Self {
        Self {
            filter_key_extractor_manager,
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
