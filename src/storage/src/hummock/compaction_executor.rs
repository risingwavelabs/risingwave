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
use std::pin::Pin;

use crate::hummock::compactor::CompactOutput;
use crate::hummock::{HummockError, HummockResult};

type CompactionRequest = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// `CompactionExecutor` is a dedicated runtime for compaction's CPU intensive jobs.
pub struct CompactionExecutor {
    requests: tokio::sync::mpsc::UnboundedSender<CompactionRequest>,
    // TODO: graceful shutdown
    #[cfg(not(madsim))]
    _runtime_thread: std::thread::JoinHandle<()>,
}

impl CompactionExecutor {
    #[cfg(not(madsim))]
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            requests: tx,
            _runtime_thread: std::thread::spawn(move || {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                if let Some(worker_threads_num) = worker_threads_num {
                    builder.worker_threads(worker_threads_num);
                }
                let runtime = builder.enable_all().build().unwrap();
                runtime.block_on(async {
                    while let Some(request) = rx.recv().await {
                        request.await;
                    }
                });
            }),
        }
    }

    // FIXME: simulation doesn't support new thread or tokio runtime.
    //        this is a workaround to make it compile.
    #[cfg(madsim)]
    pub fn new(_worker_threads_num: Option<usize>) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                request.await;
            }
        });
        Self { requests: tx }
    }

    pub fn send_request<T>(
        &self,
        t: T,
    ) -> HummockResult<tokio::sync::oneshot::Receiver<HummockResult<CompactOutput>>>
    where
        T: Future<Output = HummockResult<CompactOutput>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let compaction_request = Box::pin(async move {
            let result = t.await;
            if tx.send(result).is_err() {
                tracing::warn!("Compaction request output ignored: receiver dropped.");
            }
        });
        self.requests
            .send(compaction_request)
            .map_err(HummockError::compaction_executor)?;
        Ok(rx)
    }
}
