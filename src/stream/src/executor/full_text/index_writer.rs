// Copyright 2026 RisingWave Labs
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

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::fulltext::{Analyzer, tokenize_document};
use risingwave_common::row::{Row, RowExt};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    InitOptions, NewFullTextWriterOptions, SealCurrentEpochOptions, StateStoreWriteEpochControl,
    StateStoreWriteFullText,
};

use crate::executor::prelude::try_stream;
use crate::executor::{
    ActorContextRef, BoxedMessageStream, Execute, Executor, Message, StreamExecutorError,
    StreamExecutorResult, expect_first_barrier,
};

/// Writes a full-text index. The first input column is the indexed document, and the remaining
/// columns are the stream key of the base relation, which identifies a document.
pub struct FullTextIndexWriteExecutor<S: StateStore> {
    input: Executor,
    writer: S::FullTextWriter,
    analyzer: Analyzer,
    key_serde: OrderedRowSerde,
    actor_context: ActorContextRef,
    maintenance_enabled: bool,
}

impl<S: StateStore> Execute for FullTextIndexWriteExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> FullTextIndexWriteExecutor<S> {
    pub async fn new(
        input: Executor,
        store: S,
        table_id: TableId,
        analyzer: Analyzer,
        actor_context: ActorContextRef,
        vnode_bitmap: Bitmap,
    ) -> StreamExecutorResult<Self> {
        let key_types = input.schema().data_types()[1..].to_vec();
        let key_serde = OrderedRowSerde::new(
            key_types.clone(),
            vec![OrderType::ascending(); key_types.len()],
        );
        let writer = store
            .new_full_text_writer(NewFullTextWriterOptions { table_id })
            .await;
        Ok(Self {
            input,
            writer,
            analyzer,
            key_serde,
            actor_context,
            maintenance_enabled: vnode_bitmap.is_set(0),
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(mut self) {
        let key_indices = (1..self.input.schema().len()).collect_vec();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.writer.init(InitOptions { epoch: first_epoch }).await?;
        self.writer
            .set_maintenance_enabled(self.maintenance_enabled);

        let mut tokens = vec![];
        while let Some(msg) = input.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    let new_vnodes = barrier.as_update_vnode_bitmap(self.actor_context.id);
                    // Only checkpoint epochs become visible to batch reads, so documents are
                    // buffered across the epochs in between to avoid producing tiny segments.
                    if barrier.kind.is_checkpoint() {
                        self.writer.flush().await?;
                    }
                    self.writer.seal_current_epoch(
                        barrier.epoch.curr,
                        SealCurrentEpochOptions {
                            table_watermarks: None,
                            switch_op_consistency_level: None,
                        },
                    );
                    yield Message::Barrier(barrier);
                    if let Some(vnodes) = new_vnodes {
                        self.writer.set_maintenance_enabled(vnodes.is_set(0));
                    }
                }
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        let mut key = vec![];
                        self.key_serde
                            .serialize(row.project(&key_indices), &mut key);
                        let key = Bytes::from(key);
                        match op {
                            Op::Insert | Op::UpdateInsert => {
                                let Some(doc) = row.datum_at(0) else {
                                    continue;
                                };
                                tokens.clear();
                                tokenize_document(self.analyzer, doc, |token| {
                                    tokens.push(token.to_owned())
                                });
                                self.writer.insert_document(key, &tokens)?;
                            }
                            Op::Delete | Op::UpdateDelete => {
                                self.writer.delete_document(key)?;
                            }
                        }
                    }
                    self.writer.try_flush().await?;
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}
