// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::pg_protocol::CURRENT_SESSION;
use risingwave_common::session_config::SessionConfig;

use super::SessionImpl;

fn with_current_session<R>(f: impl FnOnce(&SessionImpl) -> R) -> Option<R> {
    CURRENT_SESSION
        .try_with(|s| s.upgrade().and_then(|s| s.downcast_ref().map(f)))
        .ok()
        .flatten()
}

/// Send a notice to the user, if currently in the context of a session.
pub(crate) fn notice_to_user(str: impl Into<String>) {
    let _ = with_current_session(|s| s.notice_to_user(str));
}

/// Get the session config, if currently in the context of a session.
pub(crate) fn config() -> Option<Arc<RwLock<SessionConfig>>> {
    with_current_session(|s| s.shared_config())
}
