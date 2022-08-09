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

use risingwave_hummock_sdk::key::{Epoch, FullKey};
use risingwave_hummock_sdk::HummockSstableId;
use tokio::task::JoinHandle;

use super::{InMemSstableWriter, SstableMeta, SstableWriterBuilder};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::value::HummockValue;
use crate::hummock::{CachePolicy, HummockResult, Sstable, SstableBuilder, SstableWriter};

/// Factory for new [`SstableBuilder`] inside the `CapacitySplitTableBuilder`.
#[async_trait::async_trait]
pub trait TableBuilderFactory<B>: Send + Sync
where
    B: SstableWriterBuilder,
{
    async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder<B::Writer>)>;
}

pub struct SealedSstableBuilder {
    pub id: HummockSstableId,
    pub meta: SstableMeta,
    pub table_ids: Vec<u32>,
    pub upload_join_handle: JoinHandle<HummockResult<()>>,
    pub data_len: usize,
}

/// Consumer for FINISHED [`SstableBuilder`].
#[async_trait::async_trait]
pub trait SstableBuilderConsumer<W>
where
    W: SstableWriter,
{
    async fn consume(
        &self,
        builder: SstableBuilder<W>,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<SealedSstableBuilder>;
}

/// Upload the in-memory SST data and metadata all at once.
pub struct SstableBatchUploader {
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
}

impl SstableBatchUploader {
    pub fn new(sstable_store: SstableStoreRef, policy: CachePolicy) -> SstableBatchUploader {
        Self {
            sstable_store,
            policy,
        }
    }
}

#[async_trait::async_trait]
impl SstableBuilderConsumer<InMemSstableWriter> for SstableBatchUploader {
    async fn consume(
        &self,
        builder: SstableBuilder<InMemSstableWriter>,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<SealedSstableBuilder> {
        let output = builder.finish().await?;

        let table_id = output.sstable_id;
        let data = output.writer_output;
        let len = data.len();
        let sstable_store = self.sstable_store.clone();
        let meta_clone = output.meta.clone();
        let policy = self.policy;
        let upload_join_handle = tokio::spawn(async move {
            let ret = if policy == CachePolicy::Fill {
                let sst = Sstable::new_with_data(table_id, meta_clone, data.clone())?;
                sstable_store.put(sst, data, CachePolicy::Fill).await
            } else {
                sstable_store
                    .put(
                        Sstable::new(table_id, meta_clone),
                        data,
                        CachePolicy::NotFill,
                    )
                    .await
            };
            drop(tracker);
            ret
        });
        Ok(SealedSstableBuilder {
            id: table_id,
            meta: output.meta,
            table_ids: output.table_ids,
            upload_join_handle,
            data_len: len,
        })
    }
}

/// A wrapper for [`SstableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<F, B, C>
where
    F: TableBuilderFactory<B>,
    B: SstableWriterBuilder,
    C: SstableBuilderConsumer<B::Writer>,
{
    /// When creating a new [`SstableBuilder`], caller use this factory to generate it.
    builder_factory: F,

    sealed_builders: Vec<SealedSstableBuilder>,

    current_builder: Option<SstableBuilder<B::Writer>>,

    /// When the `current_builder` is finished, caller use the consumer to consume SST data from
    /// the builder.
    builder_consumer: C,

    tracker: Option<MemoryTracker>,
}

impl<F, B, C> CapacitySplitTableBuilder<F, B, C>
where
    F: TableBuilderFactory<B>,
    B: SstableWriterBuilder,
    C: SstableBuilderConsumer<B::Writer>,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    pub fn new(builder_factory: F, builder_consumer: C) -> Self {
        Self {
            builder_factory,
            sealed_builders: Vec::new(),
            current_builder: None,
            builder_consumer,
            tracker: None,
        }
    }

    /// Returns the number of [`SstableBuilder`]s.
    pub fn len(&self) -> usize {
        self.sealed_builders.len() + if self.current_builder.is_some() { 1 } else { 0 }
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.sealed_builders.is_empty() && self.current_builder.is_none()
    }

    /// Adds a user key-value pair to the underlying builders, with given `epoch`.
    ///
    /// If the current builder reaches its capacity, this function will create a new one with the
    /// configuration generated by the closure provided earlier.
    pub async fn add_user_key(
        &mut self,
        user_key: Vec<u8>,
        value: HummockValue<&[u8]>,
        epoch: Epoch,
    ) -> HummockResult<()> {
        assert!(!user_key.is_empty());
        let full_key = FullKey::from_user_key(user_key, epoch);
        self.add_full_key(full_key.as_slice(), value, true).await?;
        Ok(())
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.as_ref() {
            if allow_split && builder.reach_capacity() {
                self.seal_current().await?;
            }
        }

        if self.current_builder.is_none() {
            let (tracker, builder) = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
            self.tracker = Some(tracker);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder.add(full_key.into_inner(), value)?;
        Ok(())
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub async fn seal_current(&mut self) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.take() {
            self.sealed_builders.push(
                self.builder_consumer
                    .consume(builder, self.tracker.take())
                    .await?,
            );
        }
        Ok(())
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub async fn finish(mut self) -> HummockResult<Vec<SealedSstableBuilder>> {
        self.seal_current().await?;
        Ok(self.sealed_builders)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use itertools::Itertools;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, default_sst_writer_builder_and_consumer_from_opt,
    };
    use crate::hummock::{
        InMemWriterBuilder, MemoryLimiter, SstableBuilderOptions, SstableWriterBuilder,
        DEFAULT_RESTART_INTERVAL,
    };

    pub struct LocalTableBuilderFactory<B>
    where
        B: SstableWriterBuilder,
    {
        next_id: AtomicU64,
        options: SstableBuilderOptions,
        limiter: MemoryLimiter,
        writer_builder: B,
    }

    impl LocalTableBuilderFactory<InMemWriterBuilder> {
        pub fn new(
            next_id: u64,
            writer_builder: InMemWriterBuilder,
            options: SstableBuilderOptions,
        ) -> Self {
            Self {
                limiter: MemoryLimiter::new(1000000),
                next_id: AtomicU64::new(next_id),
                options,
                writer_builder,
            }
        }
    }

    #[async_trait::async_trait]
    impl<B> TableBuilderFactory<B> for LocalTableBuilderFactory<B>
    where
        B: SstableWriterBuilder,
    {
        async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder<B::Writer>)> {
            let id = self.next_id.fetch_add(1, SeqCst);
            let builder =
                SstableBuilder::new(id, self.writer_builder.build().await?, self.options.clone());
            let tracker = self.limiter.require_memory(1).await.unwrap();
            Ok((tracker, builder))
        }
    }

    fn get_capacity_split_builder(
        opt: SstableBuilderOptions,
    ) -> CapacitySplitTableBuilder<
        LocalTableBuilderFactory<InMemWriterBuilder>,
        InMemWriterBuilder,
        SstableBatchUploader,
    > {
        let (writer_builder, builder_consumer) = default_sst_writer_builder_and_consumer_from_opt(
            &opt,
            mock_sstable_store(),
            CachePolicy::NotFill,
        );
        let builder_factory = LocalTableBuilderFactory::new(1001, writer_builder, opt);
        CapacitySplitTableBuilder::new(builder_factory, builder_consumer)
    }

    #[tokio::test]
    async fn test_empty() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opt = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let builder = get_capacity_split_builder(opt);
        let results = builder.finish().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opt = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let mut builder = get_capacity_split_builder(opt);

        for i in 0..table_capacity {
            builder
                .add_user_key(
                    b"key".to_vec(),
                    HummockValue::put(b"value"),
                    (table_capacity - i) as u64,
                )
                .await
                .unwrap();
        }

        let results = builder.finish().await.unwrap();
        assert!(results.len() > 1);
        assert_eq!(results.iter().map(|p| p.id).duplicates().count(), 0);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let opt = default_builder_opt_for_test();
        let mut builder = get_capacity_split_builder(opt);
        let mut epoch = 100;

        macro_rules! add {
            () => {
                epoch -= 1;
                builder
                    .add_user_key(b"k".to_vec(), HummockValue::put(b"v"), epoch)
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);
        builder.seal_current().await.unwrap();
        assert_eq!(builder.len(), 2);

        let results = builder.finish().await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let opt = default_builder_opt_for_test();
        let mut builder = get_capacity_split_builder(opt);

        builder
            .add_full_key(
                FullKey::from_user_key_slice(b"k", 233).as_slice(),
                HummockValue::put(b"v"),
                false,
            )
            .await
            .unwrap();
    }
}
