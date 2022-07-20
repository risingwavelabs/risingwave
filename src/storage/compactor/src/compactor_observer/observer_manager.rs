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

use risingwave_common::catalog::local_table_catalog_manager::LocalTableManagerRef;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::SubscribeResponse;

pub struct CompactorObserverNode {
    local_table_manager: LocalTableManagerRef,
    version: u64,
}

impl ObserverNodeImpl for CompactorObserverNode {
    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        match info.to_owned() {
            Info::Table(table_catalog) => {
                self.handle_catalog_notification(table_catalog);
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
                    self.handle_catalog_notification(table);
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

impl CompactorObserverNode {
    pub fn new(local_table_manager: LocalTableManagerRef) -> Self {
        Self {
            local_table_manager,
            version: 0,
        }
    }

    fn handle_catalog_notification(&mut self, table_catalog: Table) {
        self.local_table_manager
            .as_ref()
            .insert(table_catalog.id, table_catalog);
    }
}
