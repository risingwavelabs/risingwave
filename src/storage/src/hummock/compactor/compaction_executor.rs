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
    /// Runtime for compaction tasks.
    runtime: &'static tokio::runtime::Runtime,
}

impl CompactionExecutor {
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.thread_name("risingwave-compaction");
            if let Some(worker_threads_num) = worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder.enable_all().build().unwrap()
        };

        Self {
            // Leak the runtime to avoid runtime shutting-down in the main async context.
            // TODO: may manually shutdown the runtime gracefully.
            runtime: Box::leak(Box::new(runtime)),
        }
    }

    /// Send a request to the executor, returns a [`JoinHandle`] to retrieve the result.
    pub fn spawn<F, T>(&self, t: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime.spawn(t)
    }
}
