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

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common_service::observer_manager::ObserverNodeImpl;
use risingwave_pb::hummock::pin_version_response;
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::SubscribeResponse;
use risingwave_storage::hummock::local_version::local_version_manager::LocalVersionManagerRef;

pub struct RiseCtlObserverNode {
    local_version_manager: LocalVersionManagerRef,
}

impl ObserverNodeImpl for RiseCtlObserverNode {
    fn handle_notification(&mut self, _resp: SubscribeResponse) {
        // We don't care about any update so far.
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        match resp.info {
            Some(Info::Snapshot(snapshot)) => {
                self.local_version_manager.try_update_pinned_version(
                    pin_version_response::Payload::PinnedVersion(snapshot.hummock_version.unwrap()),
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

impl RiseCtlObserverNode {
    pub fn new(local_version_manager: LocalVersionManagerRef) -> Self {
        Self {
            local_version_manager,
        }
    }
}
