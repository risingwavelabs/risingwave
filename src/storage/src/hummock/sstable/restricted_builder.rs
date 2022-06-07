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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::HummockSSTableId;
use risingwave_pb::common::VNodeBitmap;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::hummock::multi_builder::SSTableBuilderWrapper;
use crate::hummock::sstable::sst_writer::SstWriter;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockError, HummockResult, SSTableBuilder, Sstable};

pub type SstWriteResult = JoinHandle<HummockResult<Sstable>>;

pub struct ResourceLimiter {
    inner: RwLock<ResourceLimiterInner>,
}

struct ResourceLimiterInner {
    current_size: u64,
    max_size: u64,
    notifiers: VecDeque<(u64, Sender<()>)>,
}

impl ResourceLimiter {
    pub fn new(max_size: u64) -> Self {
        assert!(max_size > 0, "max_size should be greater than 0");
        let inner = ResourceLimiterInner {
            current_size: 0,
            max_size,
            notifiers: Default::default(),
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn request_resource(&self, request_size: u64, notifier: Sender<()>) {
        let mut guard = self.inner.write();
        if guard.current_size != 0 && guard.current_size + request_size > guard.max_size {
            guard.notifiers.push_back((request_size, notifier));
            return;
        }
        if notifier.send(()).is_err() {
            tracing::warn!("receiver has already been deallocated");
            return;
        }
        guard.current_size += request_size;
    }

    pub fn free_resource(&self, free_size: u64) {
        let mut guard = self.inner.write();
        assert!(guard.current_size >= free_size);
        guard.current_size -= free_size;
        while let Some((request_size, notifier)) = guard.notifiers.pop_front() {
            if guard.current_size != 0 && guard.current_size + request_size > guard.max_size {
                guard.notifiers.push_front((request_size, notifier));
                return;
            }
            if notifier.send(()).is_err() {
                tracing::warn!("receiver has already been deallocated");
                return;
            }
            guard.current_size += request_size;
        }
    }
}

pub struct RestrictedBuilder<B> {
    get_id_and_builder: B,

    current_builder: Option<SSTableBuilderWrapper>,

    write_results: Vec<SstWriteResult>,

    writer: Arc<SstWriter>,

    limiter: Arc<ResourceLimiter>,

    vnode_bitmaps: Vec<Vec<VNodeBitmap>>,

    resource_size: u64,
}

impl<B, F> RestrictedBuilder<B>
where
    B: Clone + Fn() -> F,
    F: Future<Output = HummockResult<(HummockSSTableId, SSTableBuilder)>>,
{
    pub fn new(
        get_id_and_builder: B,
        writer: Arc<SstWriter>,
        limiter: Arc<ResourceLimiter>,
        resource_size: u64,
    ) -> Self {
        Self {
            get_id_and_builder,
            current_builder: None,
            write_results: Default::default(),
            writer,
            limiter,
            vnode_bitmaps: Default::default(),
            resource_size,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = self.write_results.len();
        if self.current_builder.is_some() {
            len += 1;
        }
        len
    }

    pub fn is_empty(&self) -> bool {
        self.current_builder.is_none() && self.write_results.is_empty()
    }

    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        let new_builder_required = match self.current_builder.as_ref() {
            None => true,
            Some(b) => allow_split && (b.builder.reach_capacity() || b.sealed),
        };

        if new_builder_required {
            self.finish_current_builder();
            let (id, builder) = (self.get_id_and_builder)().await?;
            self.current_builder = Some(SSTableBuilderWrapper {
                id,
                builder,
                sealed: false,
            });
            let (rx, tx) = tokio::sync::oneshot::channel();
            self.limiter.request_resource(self.resource_size, rx);
            if let Err(e) = tx.await {
                return Err(HummockError::other(format!(
                    "Cannot request resource.\n{:#?}",
                    e
                )));
            }
        }

        let builder = &mut self.current_builder.as_mut().unwrap().builder;
        builder.add(full_key.into_inner(), value);
        Ok(())
    }

    pub fn seal_current(&mut self) {
        if let Some(b) = &mut self.current_builder {
            b.sealed = true;
        }
    }

    fn finish_current_builder(&mut self) {
        if let Some(current_builder) = self.current_builder.take() {
            let id = current_builder.id;
            let (data, meta, vnode_bitmaps) = current_builder.builder.finish();
            let (tx, rx) = tokio::sync::oneshot::channel::<HummockResult<Sstable>>();
            self.writer.write(Sstable { id, meta }, data, tx);
            self.vnode_bitmaps.push(vnode_bitmaps);
            let limiter = self.limiter.clone();
            let resource_size = self.resource_size;
            self.write_results.push(tokio::spawn(async move {
                let result = rx
                    .await
                    .map_err(|_| HummockError::other("Unable to get write result"));
                limiter.free_resource(resource_size);
                result?
            }));
        }
    }

    pub async fn finish(mut self) -> HummockResult<Vec<(Sstable, Vec<VNodeBitmap>)>> {
        self.finish_current_builder();
        let mut ssts = Vec::with_capacity(self.write_results.len());
        let results = try_join_all(self.write_results)
            .await
            .map_err(|_| HummockError::other(format!("Unable to get write result")))?;
        for write_result in results {
            ssts.push(write_result?);
        }
        Ok(ssts.into_iter().zip_eq(self.vnode_bitmaps).collect_vec())
    }
}
