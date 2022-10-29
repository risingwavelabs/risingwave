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

use bincode::{Decode, Encode};
#[cfg(all(not(madsim), hm_trace))]
use futures::Future;
#[cfg(all(not(madsim), hm_trace))]
use tokio::task::futures::TaskLocalFuture;
#[cfg(all(not(madsim), hm_trace))]
use tokio::task_local;

#[derive(Copy, Clone, PartialEq, Debug, Eq, Decode, Encode, Hash)]
pub enum TraceLocalId {
    Actor(u32),
    Executor(u32),
    None,
}

#[cfg(all(not(madsim), hm_trace))]
task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static CONCURRENT_ID: TraceLocalId;
}

#[cfg(all(not(madsim), hm_trace))]
pub fn task_local_scope<F: Future>(
    actor_id: TraceLocalId,
    f: F,
) -> TaskLocalFuture<TraceLocalId, F> {
    CONCURRENT_ID.scope(actor_id, f)
}

#[cfg(all(not(madsim), hm_trace))]
pub fn task_local_get() -> TraceLocalId {
    CONCURRENT_ID.get()
}

#[cfg(madism)]
pub fn task_local_get() -> TraceLocalId {
    TraceLocalId::None
}
