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
use risingwave_pb::hummock::pin_version_response;
use risingwave_pb::hummock::pin_version_response::HummockVersionDeltas;
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::SubscribeResponse;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;

pub struct SimpleObserverNode {
    local_version_manager: Arc<LocalVersionManager>,
    version: u64,
}

impl ObserverNodeImpl for SimpleObserverNode {
    fn handle_notification(&mut self, resp: SubscribeResponse) {
        // todo: update local version when recv version deltas

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
            Info::HummockVersionDeltas(hummock_version_deltas) => {
                tracing::info!(
                    "version deltas notification: {:?}",
                    hummock_version_deltas.version_deltas
                );

                self.local_version_manager.try_update_pinned_version(
                    pin_version_response::Payload::VersionDeltas(HummockVersionDeltas {
                        delta: hummock_version_deltas.version_deltas,
                    }),
                );
            }

            _ => {
                panic!("error type notification");
            }
        }

        self.version = resp.version;
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        match resp.info {
            Some(Info::Snapshot(snapshot)) => {
                let version = snapshot.hummock_version.unwrap();
                tracing::info!(
                    "Init snapshot: version_id {}, epoch {}",
                    version.id,
                    version.max_committed_epoch
                );
                self.local_version_manager.try_update_pinned_version(
                    pin_version_response::Payload::PinnedVersion(version),
                );
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be compute snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }

        Ok(())
    }
}

impl SimpleObserverNode {
    pub fn new(local_version_manager: Arc<LocalVersionManager>) -> Self {
        Self {
            local_version_manager,
            version: 0,
        }
    }
}
