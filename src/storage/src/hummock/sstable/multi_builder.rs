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
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    CachePolicy, HummockResult, InMemSstableWriter, InMemWriterBuilder, SstableBuilder,
    SstableBuilderOptions, SstableWriter, SstableWriterBuilder, StreamingSstableWriter,
    StreamingWriterBuilder,
};

/// Factory for new [`SstableBuilder`] inside the `CapacitySplitTableBuilder`.
#[async_trait::async_trait]
pub trait TableBuilderFactory<B>: Send
where
    B: SstableWriterBuilder,
{
    async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder<B::Writer>)>;
}

pub struct SealedSstableBuilder {
    pub sst_info: SstableInfo,
    pub upload_join_handle: JoinHandle<HummockResult<()>>,
    pub bloom_filter_size: usize,
}

/// Consumer for FINISHED [`SstableBuilder`].
#[async_trait::async_trait]
pub trait SstableBuilderSealer<W>
where
    W: SstableWriter,
{
    async fn seal(
        &self,
        builder: SstableBuilder<W>,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<SealedSstableBuilder>;
}

/// Upload the in-memory SST data and metadata all at once.
pub struct BatchUploadSealer {
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
}

impl BatchUploadSealer {
    pub fn new(sstable_store: SstableStoreRef, policy: CachePolicy) -> BatchUploadSealer {
        Self {
            sstable_store,
            policy,
        }
    }
}

#[async_trait::async_trait]
impl SstableBuilderSealer<InMemSstableWriter> for BatchUploadSealer {
    async fn seal(
        &self,
        builder: SstableBuilder<InMemSstableWriter>,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<SealedSstableBuilder> {
        let output = builder.finish().await?;

        let sst_id = output.sstable_id;
        let data = output.writer_output;
        let meta = output.meta;
        let table_ids = output.table_ids;

        let sstable_store = self.sstable_store.clone();
        let bloom_filter_size = meta.bloom_filter.len();
        let sst_info = SstableInfo {
            id: sst_id,
            key_range: Some(risingwave_pb::hummock::KeyRange {
                left: meta.smallest_key.clone(),
                right: meta.largest_key.clone(),
                inf: false,
            }),
            file_size: meta.estimated_size as u64,
            table_ids,
        };
        let policy = self.policy;
        let upload_join_handle = tokio::spawn(async move {
            let ret = sstable_store.put_sst(sst_id, meta, data, policy).await;
            drop(tracker);
            ret
        });
        Ok(SealedSstableBuilder {
            sst_info,
            upload_join_handle,
            bloom_filter_size,
        })
    }
}

pub struct StreamingUploadSealer {
    sstable_store: SstableStoreRef,
}

impl StreamingUploadSealer {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        Self { sstable_store }
    }
}

#[async_trait::async_trait]
impl SstableBuilderSealer<StreamingSstableWriter> for StreamingUploadSealer {
    async fn seal(
        &self,
        builder: SstableBuilder<StreamingSstableWriter>,
        tracker: Option<MemoryTracker>,
    ) -> HummockResult<SealedSstableBuilder> {
        let output = builder.finish().await?;

        let sst_id = output.sstable_id;
        let uploader = output.writer_output;
        let meta = output.meta;
        let table_ids = output.table_ids;

        let sstable_store = self.sstable_store.clone();
        let bloom_filter_size = meta.bloom_filter.len();
        let sst_info = SstableInfo {
            id: sst_id,
            key_range: Some(risingwave_pb::hummock::KeyRange {
                left: meta.smallest_key.clone(),
                right: meta.largest_key.clone(),
                inf: false,
            }),
            file_size: meta.estimated_size as u64,
            table_ids,
        };
        let upload_join_handle = tokio::spawn(async move {
            let ret = sstable_store.finish_put_sst_stream(uploader, meta).await;
            drop(tracker);
            ret
        });
        Ok(SealedSstableBuilder {
            sst_info,
            upload_join_handle,
            bloom_filter_size,
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
    C: SstableBuilderSealer<B::Writer>,
{
    /// When creating a new [`SstableBuilder`], caller use this factory to generate it.
    builder_factory: F,

    sealed_builders: Vec<SealedSstableBuilder>,

    current_builder: Option<SstableBuilder<B::Writer>>,

    /// When the `current_builder` is finished, caller use the consumer to consume SST data from
    /// the builder.
    builder_sealer: C,

    tracker: Option<MemoryTracker>,
}

impl<F, B, C> CapacitySplitTableBuilder<F, B, C>
where
    F: TableBuilderFactory<B>,
    B: SstableWriterBuilder,
    C: SstableBuilderSealer<B::Writer>,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    pub fn new(builder_factory: F, builder_sealer: C) -> Self {
        Self {
            builder_factory,
            sealed_builders: Vec::new(),
            current_builder: None,
            builder_sealer,
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
                self.builder_sealer
                    .seal(builder, self.tracker.take())
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

/// The writer will generate an entire SST in memory
/// and the sealer will upload it to object store.
pub fn get_sst_writer_and_sealer_for_batch_upload(
    opt: &SstableBuilderOptions,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
) -> (InMemWriterBuilder, BatchUploadSealer) {
    (
        InMemWriterBuilder::from(opt),
        BatchUploadSealer::new(sstable_store, policy),
    )
}

/// The writer will upload blocks of data to object store
/// and the sealer will finish the streaming upload.
pub fn get_sst_writer_and_sealer_for_streaming_upload(
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
) -> (StreamingWriterBuilder, StreamingUploadSealer) {
    (
        StreamingWriterBuilder::new(sstable_store.clone(), policy),
        StreamingUploadSealer::new(sstable_store),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::test_utils::default_builder_opt_for_test;
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

    impl<B> LocalTableBuilderFactory<B>
    where
        B: SstableWriterBuilder,
    {
        pub fn new(next_id: u64, writer_builder: B, options: SstableBuilderOptions) -> Self {
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
            let tracker = self.limiter.require_memory(1).await.unwrap();
            let id = self.next_id.fetch_add(1, SeqCst);
            let builder = SstableBuilder::new(
                id,
                self.writer_builder.build(id).await?,
                self.options.clone(),
            );
            Ok((tracker, builder))
        }
    }

    fn get_capacity_split_builder_for_batch_upload(
        opt: SstableBuilderOptions,
    ) -> CapacitySplitTableBuilder<
        LocalTableBuilderFactory<InMemWriterBuilder>,
        InMemWriterBuilder,
        BatchUploadSealer,
    > {
        let (writer_builder, builder_sealer) = get_sst_writer_and_sealer_for_batch_upload(
            &opt,
            mock_sstable_store(),
            CachePolicy::NotFill,
        );
        let builder_factory = LocalTableBuilderFactory::new(1001, writer_builder, opt);
        CapacitySplitTableBuilder::new(builder_factory, builder_sealer)
    }

    fn get_capacity_split_builder_for_streaming_upload(
        opt: SstableBuilderOptions,
    ) -> CapacitySplitTableBuilder<
        LocalTableBuilderFactory<StreamingWriterBuilder>,
        StreamingWriterBuilder,
        StreamingUploadSealer,
    > {
        let (writer_builder, builder_sealer) = get_sst_writer_and_sealer_for_streaming_upload(
            mock_sstable_store(),
            CachePolicy::NotFill,
        );
        let builder_factory = LocalTableBuilderFactory::new(1001, writer_builder, opt);
        CapacitySplitTableBuilder::new(builder_factory, builder_sealer)
    }

    #[tokio::test]
    async fn test_empty() {
        async fn test_inner<F, B, C>(builder: CapacitySplitTableBuilder<F, B, C>)
        where
            F: TableBuilderFactory<B>,
            B: SstableWriterBuilder,
            C: SstableBuilderSealer<B::Writer>,
        {
            let results = builder.finish().await.unwrap();
            assert!(results.is_empty());
        }
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opt = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
            estimate_bloom_filter_capacity: 0,
        };
        test_inner(get_capacity_split_builder_for_batch_upload(opt.clone())).await;
        test_inner(get_capacity_split_builder_for_streaming_upload(opt.clone())).await;
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        async fn test_inner<F, B, C>(
            mut builder: CapacitySplitTableBuilder<F, B, C>,
            table_capacity: usize,
        ) where
            F: TableBuilderFactory<B>,
            B: SstableWriterBuilder,
            C: SstableBuilderSealer<B::Writer>,
        {
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
        }
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opt = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
            ..Default::default()
        };
        test_inner(
            get_capacity_split_builder_for_batch_upload(opt.clone()),
            table_capacity,
        )
        .await;
        test_inner(
            get_capacity_split_builder_for_streaming_upload(opt),
            table_capacity,
        )
        .await;
    }

    #[tokio::test]
    async fn test_table_seal() {
        async fn test_inner<F, B, C>(mut builder: CapacitySplitTableBuilder<F, B, C>)
        where
            F: TableBuilderFactory<B>,
            B: SstableWriterBuilder,
            C: SstableBuilderSealer<B::Writer>,
        {
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

        let opt = default_builder_opt_for_test();
        test_inner(get_capacity_split_builder_for_batch_upload(opt.clone())).await;
        test_inner(get_capacity_split_builder_for_streaming_upload(opt)).await;
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        async fn test_inner<F, B, C>(mut builder: CapacitySplitTableBuilder<F, B, C>)
        where
            F: TableBuilderFactory<B>,
            B: SstableWriterBuilder,
            C: SstableBuilderSealer<B::Writer>,
        {
            builder
                .add_full_key(
                    FullKey::from_user_key_slice(b"k", 233).as_slice(),
                    HummockValue::put(b"v"),
                    false,
                )
                .await
                .unwrap();
        }

        let opt = default_builder_opt_for_test();
        test_inner(get_capacity_split_builder_for_batch_upload(opt.clone())).await;
        test_inner(get_capacity_split_builder_for_streaming_upload(opt)).await;
    }
}
