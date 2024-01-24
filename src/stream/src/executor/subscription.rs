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

use core::str::FromStr;
use core::time::Duration;
use std::collections::HashMap;

use futures::prelude::stream::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Interval, Timestamptz};
use risingwave_common::util::epoch::Epoch;
use risingwave_storage::store::LocalStateStore;
use tokio::time::Instant;

use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::common::log_store_impl::kv_log_store::ReaderTruncationOffsetType;
use crate::common::log_store_impl::subscription_log_store::SubscriptionLogStoreWriter;

const EXECUTE_GC_INTERVAL: u64 = 3600;

pub struct SubscriptionExecutor<LS: LocalStateStore> {
    actor_context: ActorContextRef,
    info: ExecutorInfo,
    input: BoxedExecutor,
    log_store: SubscriptionLogStoreWriter<LS>,
    retention_seconds: i64,
}

impl<LS: LocalStateStore> SubscriptionExecutor<LS> {
    #[allow(clippy::too_many_arguments)]
    #[expect(clippy::unused_async)]
    pub async fn new(
        actor_context: ActorContextRef,
        info: ExecutorInfo,
        input: BoxedExecutor,
        log_store: SubscriptionLogStoreWriter<LS>,
        properties: HashMap<String, String>,
    ) -> StreamExecutorResult<Self> {
        let retention_seconds_str = properties.get("retention_seconds").ok_or_else(|| {
            StreamExecutorError::serde_error("Subscription retention time not set.".to_string())
        })?;
        let retention_seconds = Interval::from_str(retention_seconds_str)
            .map_err(|_| {
                StreamExecutorError::serde_error(
                    "Retention needs to be set in Interval format".to_string(),
                )
            })?
            .usecs();
        Ok(Self {
            actor_context,
            info,
            input,
            log_store,
            retention_seconds,
        })
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        self.log_store.init(barrier.epoch, false).await?;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let mut next_truncate_time = Instant::now() + Duration::from_secs(EXECUTE_GC_INTERVAL);

        #[for_await]
        for msg in input {
            let msg = msg?;
            println!("{:?}", msg);
            yield match msg {
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
                    let truncate_offset: Option<ReaderTruncationOffsetType> = if next_truncate_time
                        < Instant::now()
                    {
                        let truncate_timestamptz = Timestamptz::from_secs(barrier.get_curr_epoch().as_timestamptz().timestamp() - self.retention_seconds).ok_or_else(||{StreamExecutorError::from("Subscription retention time calculation error: timestamp is out of range.".to_string())})?;
                        let epoch =
                            Epoch::from_unix_millis(truncate_timestamptz.timestamp_millis() as u64);
                        next_truncate_time =
                            Instant::now() + Duration::from_secs(EXECUTE_GC_INTERVAL);
                        Some((epoch.0, None))
                    } else {
                        None
                    };
                    self.log_store
                        .flush_current_epoch(
                            barrier.epoch.curr,
                            barrier.kind.is_checkpoint(),
                            truncate_offset,
                        )
                        .await?;

                    if let Some(vnode_bitmap) =
                        barrier.as_update_vnode_bitmap(self.actor_context.id)
                    {
                        self.log_store.update_vnode_bitmap(vnode_bitmap)?;
                    }
                    Message::Barrier(barrier)
                }
            }
        }
    }
}
impl<LS: LocalStateStore> Executor for SubscriptionExecutor<LS> {
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
