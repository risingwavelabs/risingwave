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

use std::future::Future;

use tokio::task::JoinHandle;

/// `CompactionExecutor` is a dedicated runtime for compaction's CPU intensive jobs.
pub struct CompactionExecutor {
    runtime: Option<tokio::runtime::Runtime>,
}

impl CompactionExecutor {
    #[cfg(not(madsim))]
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads_num) = worker_threads_num {
            builder.worker_threads(worker_threads_num);
        }
        let runtime = builder.enable_all().build().unwrap();
        Self {
            runtime: Some(runtime),
        }
    }

    // FIXME: simulation doesn't support new thread or tokio runtime.
    //        this is a workaround to make it compile.
    #[cfg(madsim)]
    pub fn new(_worker_threads_num: Option<usize>) -> Self {
        Self { runtime: None }
    }

    pub fn send_request<F, T>(&self, t: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match self.runtime.as_ref() {
            Some(runtime) => runtime.spawn(t),
            None => tokio::spawn(t),
        }
    }
}
