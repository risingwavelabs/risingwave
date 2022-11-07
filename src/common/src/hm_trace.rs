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

use serde::{Deserialize, Serialize};
#[cfg(all(not(madsim), hm_trace))]
use {
    futures::Future,
    std::sync::atomic::{AtomicU64, Ordering},
    tokio::task::futures::TaskLocalFuture,
    tokio::task_local,
};

#[derive(Copy, Clone, PartialEq, Debug, Eq, Serialize, Deserialize, Hash)]
pub enum TraceLocalId {
    Actor(u64),
    Executor(u64),
    None,
}

#[cfg(all(not(madsim), hm_trace))]
static CONCURRENT_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(all(not(madsim), hm_trace))]
task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: TraceLocalId;
}

#[cfg(all(not(madsim), hm_trace))]
pub fn actor_local_scope<F: Future>(f: F) -> TaskLocalFuture<TraceLocalId, F> {
    let id = CONCURRENT_ID.fetch_add(1, Ordering::Relaxed);
    let actor_id = TraceLocalId::Actor(id);
    LOCAL_ID.scope(actor_id, f)
}

#[cfg(all(not(madsim), hm_trace))]
pub fn executor_local_scope<F: Future>(f: F) -> TaskLocalFuture<TraceLocalId, F> {
    let id = CONCURRENT_ID.fetch_add(1, Ordering::Relaxed);
    let executor_id = TraceLocalId::Executor(id);
    LOCAL_ID.scope(executor_id, f)
}

#[cfg(all(not(madsim), hm_trace))]
pub fn task_local_get() -> TraceLocalId {
    LOCAL_ID.get()
}

#[cfg(any(madism, not(hm_trace)))]
pub fn task_local_get() -> TraceLocalId {
    TraceLocalId::None
}
