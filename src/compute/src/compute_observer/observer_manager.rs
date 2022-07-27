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

use parking_lot::RwLock;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_hummock_sdk::slice_transform::{
    FullKeySliceTransform, SchemaSliceTransform, SliceTransformImpl,
};
use risingwave_pb::catalog::{Source, Table};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;

pub struct ComputeObserverNode {
    table_id_to_slice_transform: Arc<RwLock<HashMap<u32, SliceTransformImpl>>>,

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
    pub fn new(table_id_to_slice_transform: Arc<RwLock<HashMap<u32, SliceTransformImpl>>>) -> Self {
        Self {
            table_id_to_slice_transform,
            version: 0,
        }
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        let mut guard = self.table_id_to_slice_transform.write();
        match operation {
            Operation::Add | Operation::Update => {
                let slice_transform = if table_catalog.read_pattern_prefix_column < 1 {
                    // for now frontend had not infer the table_id_to_slice_transform, so we use
                    // FullKeySliceTransform
                    SliceTransformImpl::FullKey(FullKeySliceTransform::default())
                } else {
                    SliceTransformImpl::Schema(SchemaSliceTransform::new(&table_catalog))
                };
                guard.insert(table_catalog.id, slice_transform);
            }

            Operation::Delete => {
                guard.remove(&table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }

    fn handle_source_notification(&mut self, operation: Operation, source_catalog: Source) {
        let mut guard = self.table_id_to_slice_transform.write();
        match operation {
            Operation::Add | Operation::Update => {
                guard.insert(
                    source_catalog.id,
                    SliceTransformImpl::FullKey(FullKeySliceTransform::default()),
                );
            }

            Operation::Delete => {
                guard.remove(&source_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}
