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

use futures::Future;
use tokio::task_local;
#[cfg(all(not(madsim), hm_trace))]
use {
    std::sync::atomic::{AtomicU64, Ordering},
    tokio::task::futures::TaskLocalFuture,
};

#[cfg(all(not(madsim), hm_trace))]
static CONCURRENT_ID: AtomicU64 = AtomicU64::new(0);

type ConcurrentId = u64;

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

pub trait HummockTraceFuture: Sized + Future {
    #[cfg(any(madsim, not(hm_trace)))]
    fn may_trace_hummock(self) -> Self {
        self
    }
    #[cfg(all(not(madsim), hm_trace))]
    fn may_trace_hummock(self) -> TaskLocalFuture<ConcurrentId, Self> {
        let id = CONCURRENT_ID.fetch_add(1, Ordering::Relaxed);
        LOCAL_ID.scope(id, self)
    }
}

impl<F: Future> HummockTraceFuture for F {}

pub fn get_concurrent_id() -> ConcurrentId {
    #[cfg(all(not(madsim), hm_trace))]
    {
        LOCAL_ID.get()
    }
    #[cfg(any(madsim, not(hm_trace)))]
    0
}
