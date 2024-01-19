// Copyright 2024 RisingWave Labs
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

use core::cmp::max;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_connector::sink::log_store::{LogStoreResult, LogWriter, TruncateOffset};
use risingwave_hummock_sdk::table_watermark::{VnodeWatermark, WatermarkDirection};
use risingwave_storage::store::{InitOptions, LocalStateStore, SealCurrentEpochOptions};

use super::kv_log_store::ReaderTruncationOffsetType;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::{SeqIdType, FIRST_SEQ_ID};

pub struct OnlyWriterLogStoreWriter<LS: LocalStateStore> {
    _table_id: TableId,

    seq_id: SeqIdType,

    state_store: LS,

    serde: LogStoreRowSerde,

    _identity: String,

    truncate_offset: Option<ReaderTruncationOffsetType>,
}

impl<LS: LocalStateStore> OnlyWriterLogStoreWriter<LS> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: LS,
        serde: LogStoreRowSerde,
        identity: String,
    ) -> Self {
        Self {
            _table_id: table_id,
            seq_id: FIRST_SEQ_ID,
            state_store,
            serde,
            _identity: identity,
            truncate_offset: None,
        }
    }
}

impl<LS: LocalStateStore> LogWriter for OnlyWriterLogStoreWriter<LS> {
    async fn init(
        &mut self,
        epoch: risingwave_common::util::epoch::EpochPair,
        _pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        self.state_store
            .init(InitOptions::new_with_epoch(epoch))
            .await?;
        self.seq_id = FIRST_SEQ_ID;
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        if chunk.cardinality() == 0 {
            return Ok(());
        }
        let epoch = self.state_store.epoch();
        let start_seq_id = self.seq_id;
        self.seq_id += chunk.cardinality() as SeqIdType;
        for (i, (op, row)) in chunk.rows().enumerate() {
            let seq_id = start_seq_id + (i as SeqIdType);
            let (_vnode, key, value) = self.serde.serialize_data_row(epoch, seq_id, op, row);
            self.state_store.insert(key, value, None)?;
        }
        Ok(())
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> LogStoreResult<()> {
        let epoch = self.state_store.epoch();
        for vnode in self.serde.vnodes().iter_vnodes() {
            let (key, value) = self.serde.serialize_barrier(epoch, vnode, is_checkpoint);
            self.state_store.insert(key, value, None)?;
        }

        let mut watermark = None;
        if let Some(truncate_offset) = self.truncate_offset {
            watermark = Some(VnodeWatermark::new(
                self.serde.vnodes().clone(),
                self.serde
                    .serialize_truncation_offset_watermark(truncate_offset),
            ));
            self.truncate_offset = None;
        }
        self.state_store.flush(vec![]).await?;
        let watermark = watermark.into_iter().collect_vec();
        self.state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions::new(watermark, WatermarkDirection::Ascending),
        );
        self.seq_id = FIRST_SEQ_ID;
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> LogStoreResult<()> {
        self.serde.update_vnode_bitmap(new_vnodes.clone());
        Ok(())
    }

    fn pause(&mut self) -> risingwave_connector::sink::log_store::LogStoreResult<()> {
        unimplemented!()
    }

    fn resume(&mut self) -> risingwave_connector::sink::log_store::LogStoreResult<()> {
        unimplemented!()
    }
}

impl<LS: LocalStateStore> OnlyWriterLogStoreWriter<LS> {
    pub fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        if let TruncateOffset::Barrier { epoch } = &offset {
            let epoch_offset = match self.truncate_offset {
                Some(truncate_offset) => max(*epoch, truncate_offset.0),
                None => *epoch,
            };
            self.truncate_offset = Some((epoch_offset, None));
            Ok(())
        } else {
            Err(anyhow!("only support truncate at barrier."))
        }
    }
}
