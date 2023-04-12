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
use futures::{FutureExt, TryFutureExt};

use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use tokio::task::JoinHandle;
use yatp::ThreadPool;
use yatp::task::future::TaskCell;
use tokio::sync::oneshot::{channel, Receiver};
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            TaskHandle::Yatp(task) => {
                match task.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(ret) => {
                        Poll::Ready(ret.map_err(|_| HummockError::other("yapt pool canceled")))
                    }
                }
            },
            TaskHandle::Tokio(task) => {
                match task.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(ret) => {
                        Poll::Ready(ret.map_err(|_| HummockError::other("tokio pool canceled")))
                    }
                }
            }
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

    /// Send a request to the executor, returns a [`JoinHandle`] to retrieve the result.
    pub fn spawn<F, T>(&self, t: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match &self.runtime {
            TypedRuntime::Tokio(pool) => {
                TaskHandle::Tokio(pool.spawn(t))
            }
            TypedRuntime::Yatp(pool) => {
                let (tx, rx) = channel();
                pool.spawn(async move {
                    let res = t.await;
                    let _ = tx.send(res);
                });
                TaskHandle::Yatp(rx)
            }
        }
    }
}
