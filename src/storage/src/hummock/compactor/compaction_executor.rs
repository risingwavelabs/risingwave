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

use std::future::Future;

use more_asserts::assert_gt;
use risingwave_common::util::resource_util;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use tokio::task::JoinHandle;

/// `CompactionExecutor` is a dedicated runtime for compaction's CPU intensive jobs.
pub struct CompactionExecutor {
    /// Runtime for compaction tasks.
    runtime: BackgroundShutdownRuntime,
    worker_num: usize,
}

impl CompactionExecutor {
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let mut worker_num = resource_util::cpu::total_cpu_available().ceil() as usize;
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.thread_name("rw-compaction");
            if let Some(worker_threads_num) = worker_threads_num {
                builder.worker_threads(worker_threads_num);
                worker_num = worker_threads_num;
            }
            assert_gt!(worker_num, 0);
            builder.enable_all().build().unwrap()
        };

        Self {
            runtime: runtime.into(),
            worker_num,
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

    pub fn worker_num(&self) -> usize {
        self.worker_num
    }
}
