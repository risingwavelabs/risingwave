use futures::StreamExt;
use itertools::Itertools;
use risingwave_common::array::{Row, RowRef, StreamChunk};

use super::*;
use crate::executor::StreamChunkBuilder;

impl<S: StateStore> LookupExecutor<S> {
    /// Try produce one stream message from [`LookupExecutor`]. If there's no message to produce, it
    /// will return `None`, and the `next` function of [`LookupExecutor`] will continously polling
    /// messages until there's one.
    ///
    /// If we can use `async_stream` to write this part, things could be easier.
    pub async fn next_inner(&mut self) -> Result<Option<Message>> {
        match self.input.next().await.expect("unexpected end of stream")? {
            ArrangeMessage::Barrier(barrier) => {
                self.process_barrier(barrier.clone()).await?;
                Ok(Some(Message::Barrier(barrier)))
            }
            ArrangeMessage::Arrange(_) => {
                // TODO: replicate batch
                //
                // As we assume currently all lookups are on the same worker node of arrangements,
                // the data would always be available in the local shared buffer. Therefore, there's
                // no need to replicate batch.
                Ok(None)
            }
            ArrangeMessage::Stream(chunk) => Ok(Some(Message::Chunk(self.lookup(chunk).await?))),
        }
    }

    /// Store the barrier.
    async fn process_barrier(&mut self, barrier: Barrier) -> Result<()> {
        self.last_barrier = Some(barrier);
        Ok(())
    }

    /// Lookup the data in the shared buffer.
    async fn lookup(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        let last_barrier = self
            .last_barrier
            .as_ref()
            .expect("data received before a barrier");
        let lookup_epoch = if self.arrangement.use_current_epoch {
            last_barrier.epoch.curr
        } else {
            last_barrier.epoch.prev
        };
        let chunk = chunk.compact()?;
        let (chunk, ops) = chunk.into_parts();

        let mut builder = StreamChunkBuilder::new(
            chunk.capacity(),
            &self.output_data_types,
            0,
            self.stream.col_types.len(),
        )?;

        for (op, row) in ops.iter().zip_eq(chunk.rows()) {
            for matched_row in self.lookup_one_row(&row, lookup_epoch).await? {
                builder.append_row(*op, &row, &matched_row)?;
            }
            // TODO: support outer join (return null if no rows are matched)
        }

        builder.finish()
    }

    /// Lookup all rows corresponding to a join key in shared buffer.
    async fn lookup_one_row(&mut self, _row: &RowRef<'_>, _lookup_epoch: u64) -> Result<Vec<Row>> {
        todo!();
    }
}
