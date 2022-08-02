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

use std::sync::Arc;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorImpl, FilterKeyExtractorManagerRef, FullKeyFilterKeyExtractor,
    SchemaFilterKeyExtractor,
};
use risingwave_pb::catalog::{Source, Table};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;

pub struct ComputeObserverNode {
    filter_key_extractor_manager: FilterKeyExtractorManagerRef,

    version: u64,
}

impl ObserverNodeImpl for ComputeObserverNode {
    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info.to_owned() {
            Info::Table(table_catalog) => {
                self.handle_catalog_notification(resp.operation(), table_catalog);
            }

            Info::Source(source_catalog) => {
                self.handle_source_notification(resp.operation(), source_catalog);
            }

            _ => {
                panic!("error type notification");
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        if self.version > resp.version {
            return Err(ErrorCode::InternalError(format!(
                "the SnapshotVersion is incorrect local {} snapshot {}",
                self.version, resp.version
            ))
            .into());
        }

        match resp.info {
            Some(Info::Snapshot(snapshot)) => {
                for table in snapshot.table {
                    self.handle_catalog_notification(Operation::Add, table);
                }

                self.version = resp.version;
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be frontend snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }

        Ok(())
    }
}

impl ComputeObserverNode {
    pub fn new(filter_key_extractor_manager: FilterKeyExtractorManagerRef) -> Self {
        Self {
            filter_key_extractor_manager,
            version: 0,
        }
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        match operation {
            Operation::Add | Operation::Update => {
                let filter_key_extractor = if table_catalog.read_pattern_prefix_column < 1 {
                    // for now frontend had not infer the table_id_to_filter_key_extractor, so we
                    // use FullKeyFilterKeyExtractor
                    FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor::default())
                } else {
                    FilterKeyExtractorImpl::Schema(SchemaFilterKeyExtractor::new(&table_catalog))
                };
                self.filter_key_extractor_manager
                    .update(table_catalog.id, Arc::new(filter_key_extractor));
            }

            Operation::Delete => {
                self.filter_key_extractor_manager.remove(table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }

    fn handle_source_notification(&mut self, operation: Operation, source_catalog: Source) {
        match operation {
            Operation::Add | Operation::Update => {
                self.filter_key_extractor_manager.update(
                    source_catalog.id,
                    Arc::new(FilterKeyExtractorImpl::FullKey(
                        FullKeyFilterKeyExtractor::default(),
                    )),
                );
            }

            Operation::Delete => {
                self.filter_key_extractor_manager.remove(source_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}
