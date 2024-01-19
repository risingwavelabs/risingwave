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

use core::ops::Bound;

use either::Either;
use futures::pin_mut;
use futures::prelude::stream::{select_with_strategy, PollNext, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::field_generator::ChronoFieldInner;
use risingwave_common::row::{self, OwnedRow, Row};
use risingwave_common::types::{ScalarImpl, Timestamptz};
use risingwave_connector::sink::log_store::TruncateOffset;
use risingwave_storage::store::LocalStateStore;
use risingwave_storage::StateStore;
use tokio::time::sleep;

use super::test_utils::prelude::StateTable;
use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::common::log_store_impl::only_writer_log_store::OnlyWriterLogStoreWriter;

const EXECUTE_GC_INTERVAL: u64 = 3600;

pub struct SubscriptionExecutor<LS: LocalStateStore, S: StateStore> {
    actor_context: ActorContextRef,
    info: ExecutorInfo,
    input: BoxedExecutor,
    log_store: OnlyWriterLogStoreWriter<LS>,
    epoch_store: StateTable<S>,
    retention: i64,
}

impl<LS: LocalStateStore, S: StateStore> SubscriptionExecutor<LS, S> {
    #[allow(clippy::too_many_arguments)]
    #[expect(clippy::unused_async)]
    pub async fn new(
        actor_context: ActorContextRef,
        info: ExecutorInfo,
        input: BoxedExecutor,
        log_store: OnlyWriterLogStoreWriter<LS>,
        epoch_store: StateTable<S>,
        retention: i64,
    ) -> StreamExecutorResult<Self> {
        Ok(Self {
            actor_context,
            info,
            input,
            log_store,
            epoch_store,
            retention,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = std::pin::pin!(self.input.execute());

        let barrier = expect_first_barrier(&mut input).await?;
        self.log_store.init(barrier.epoch, false).await?;
        self.epoch_store.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        let stream = select_with_strategy(
            Self::timeout().map(Either::Left),
            input.by_ref().map(Either::Right),
            |_: &mut PollNext| PollNext::Left,
        );

        #[for_await]
        for msg in stream {
            yield match msg {
                Either::Left(_) => {
                    let mut epoch = 0_u64;
                    let mut delete_row = vec![];
                    {
                        let truncate_timestamptz = Timestamptz::from_secs(Timestamptz::from_now().timestamp() - self.retention).ok_or_else(||{StreamExecutorError::from("Subscription retention time calculation error: timestamp is out of range.".to_string())})?;
                        let sub_range: (Bound<OwnedRow>, Bound<OwnedRow>) = (
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Included(OwnedRow::new(vec![Some(
                                risingwave_common::types::ScalarImpl::Timestamptz(
                                    truncate_timestamptz,
                                ),
                            )])),
                        );
                        let data_iter = self
                            .epoch_store
                            .iter_with_prefix(row::empty(), &sub_range, Default::default())
                            .await?;
                        pin_mut!(data_iter);
                        while let Some(row) = data_iter.next().await {
                            let row = row?;
                            let now_epoch = row
                                .datum_at(1)
                                .ok_or_else(|| {
                                    StreamExecutorError::from(
                                        "Can't find epoch from subscription".to_string(),
                                    )
                                })?
                                .into_decimal();
                            epoch = epoch.max(
                                u64::try_from(now_epoch)
                                    .map_err(|err| StreamExecutorError::from(err.to_string()))?,
                            );
                            delete_row.push(row);
                        }
                    }
                    delete_row.into_iter().for_each(|row| {
                        self.epoch_store.delete(row.as_ref());
                    });
                    self.log_store.truncate(TruncateOffset::Barrier { epoch })?;
                    continue;
                }
                Either::Right(msg) => {
                    let msg = msg?;
                    match msg {
                        Message::Watermark(w) => Message::Watermark(w),
                        Message::Chunk(chunk) => {
                            if chunk.cardinality() == 0 {
                                // empty chunk
                                continue;
                            }
                            self.log_store.write_chunk(chunk.clone())?;
                            Message::Chunk(chunk)
                        }
                        Message::Barrier(barrier) => {
                            self.log_store
                                .flush_current_epoch(
                                    barrier.epoch.curr,
                                    barrier.kind.is_checkpoint(),
                                )
                                .await?;

                            if let Some(vnode_bitmap) =
                                barrier.as_update_vnode_bitmap(self.actor_context.id)
                            {
                                self.log_store.update_vnode_bitmap(vnode_bitmap)?;
                            }
                            let row = OwnedRow::new(vec![
                                Some(ScalarImpl::Timestamptz(Timestamptz::from_now())),
                                Some(ScalarImpl::Decimal(barrier.epoch.curr.into())),
                            ]);
                            self.epoch_store.insert(row);
                            self.epoch_store.commit(barrier.epoch).await?;
                            Message::Barrier(barrier)
                        }
                    }
                }
            }
        }
    }

    #[try_stream(ok = (), error = StreamExecutorError)]
    pub async fn timeout() {
        loop {
            sleep(tokio::time::Duration::from_secs(EXECUTE_GC_INTERVAL)).await;
            yield ();
        }
    }
}
impl<LS: LocalStateStore, S: StateStore> Executor for SubscriptionExecutor<LS, S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn info(&self) -> ExecutorInfo {
        self.info.clone()
    }
}
