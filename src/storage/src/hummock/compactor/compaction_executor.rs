// Copyright 2023 RisingWave Labs
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
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::hummock::CompactTaskPriority;
use tokio::sync::oneshot::{channel, Receiver};
use tokio::task::JoinHandle;
use yatp::queue::Extras;
use yatp::task::future::TaskCell;
use yatp::{Builder, ThreadPool};

use crate::hummock::{HummockError, HummockResult};

pub enum TypedRuntime {
    Tokio(BackgroundShutdownRuntime),
    Yatp(ThreadPool<TaskCell>),
}

/// `CompactionExecutor` is a dedicated runtime for compaction's CPU intensive jobs.
pub struct CompactionExecutor {
    /// Runtime for compaction tasks.
    runtime: TypedRuntime,
}

pub enum TaskHandle<T> {
    Tokio(JoinHandle<T>),
    Yatp(Receiver<T>),
}

impl<T: Send + 'static> Future for TaskHandle<T> {
    type Output = HummockResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            TaskHandle::Yatp(task) => match task.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(ret) => {
                    Poll::Ready(ret.map_err(|_| HummockError::other("yapt pool canceled")))
                }
            },
            TaskHandle::Tokio(task) => match task.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(ret) => {
                    Poll::Ready(ret.map_err(|_| HummockError::other("tokio pool canceled")))
                }
            },
        }
    }
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
            runtime: TypedRuntime::Tokio(runtime.into()),
        }
    }

    pub fn new_priority_pool(worker_threads_num: Option<usize>) -> Self {
        let mut builder = Builder::new("compactor");

        if let Some(worker_threads_num) = worker_threads_num {
            builder.max_thread_count(worker_threads_num);
        }
        let pool = builder.build_multilevel_future_pool();
        Self {
            runtime: TypedRuntime::Yatp(pool),
        }
    }

    /// Send a request to the executor, returns a [`JoinHandle`] to retrieve the result.
    pub fn spawn<F, T>(&self, t: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.spawn_with_priority(0, CompactTaskPriority::Normal, t)
    }

    /// Send a request to the executor, returns a [`JoinHandle`] to retrieve the result.
    pub fn spawn_with_priority<F, T>(
        &self,
        task_id: u64,
        priority: CompactTaskPriority,
        t: F,
    ) -> TaskHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.runtime {
            TypedRuntime::Tokio(pool) => TaskHandle::Tokio(pool.spawn(t)),
            TypedRuntime::Yatp(pool) => {
                let fixed_leve = match priority {
                    CompactTaskPriority::Normal => None,
                    CompactTaskPriority::High => Some(0),
                    CompactTaskPriority::Low => Some(2),
                };
                let (tx, rx) = channel();
                let extras = Extras::new_multilevel(task_id, fixed_leve);
                pool.spawn(TaskCell::new(
                    async move {
                        let res = t.await;
                        let _ = tx.send(res);
                    },
                    extras,
                ));
                TaskHandle::Yatp(rx)
            }
        }
    }
}
