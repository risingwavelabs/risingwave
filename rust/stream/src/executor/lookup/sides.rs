use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::RwError;
use risingwave_common::types::DataType;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::barrier_align::{AlignedMessage, BarrierAligner};
use crate::executor::{Barrier, Executor};

/// Join side of Lookup Executor's stream
pub struct StreamJoinSide {
    /// Indices of the join key columns
    pub key_indices: Vec<usize>,

    /// The primary key indices of this side, used for state store
    pub pk_indices: Vec<usize>,

    /// The date type of each columns to join on
    pub col_types: Vec<DataType>,
}

impl std::fmt::Debug for StreamJoinSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrangeJoinSide")
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .finish()
    }
}

/// Join side of Arrange Executor's stream
pub struct ArrangeJoinSide<S: StateStore> {
    /// Indices of the join key columns
    pub key_indices: Vec<usize>,

    /// The primary key indices of this side, used for state store
    pub pk_indices: Vec<usize>,

    /// The date type of each columns to join on
    pub col_types: Vec<DataType>,

    /// Keyspace for the arrangement
    pub arrangement: Keyspace<S>,

    /// Whether to join with the arrangement of the current epoch
    pub use_current_epoch: bool,
}

/// Message from the [`arrange_join_stream`].
pub enum ArrangeMessage {
    /// Arrangement sides' update in this epoch. There will be only one arrange batch message
    /// within epoch. Once the executor receives an arrange batch message, it will replicate batch
    /// using the previous epoch.
    Arrange(Vec<StreamChunk>),

    /// There's a message from stream side.
    Stream(StreamChunk),

    /// Barrier (once every epoch).
    Barrier(Barrier),
}

/// Join the stream with the previous stable snapshot of the arrangement.
///
/// For example, the executor will receive the following message sequence from
/// `stream_lookup_arrange_prev_epoch`:
///
/// * [Msg] Barrier (prev = [1], current = [2])
/// * [Msg] Stream (key = a)
/// * [Do] lookup `a` in arrangement of epoch [1] (prev epoch)
/// * [Msg] Arrangement (batch)
/// * [Do] replicate batch with epoch [2]
/// * Barrier (prev = [2], current = [3])
#[allow(dead_code)]
#[try_stream(ok = ArrangeMessage, error = RwError)]
async fn stream_lookup_arrange_prev_epoch(
    stream: Box<dyn Executor>,
    arrangement: Box<dyn Executor>,
) {
    let mut input = BarrierAligner::new(stream, arrangement);
    let mut arrange_updates = vec![];

    loop {
        match input.next().await {
            AlignedMessage::Left(msg) => {
                // As prev epoch is already available, we can directly forward messages from the
                // stream side.
                yield ArrangeMessage::Stream(msg?);
            }
            AlignedMessage::Right(msg) => {
                // For message from the arrangement side, we always send in batch.
                arrange_updates.push(msg?);
            }
            AlignedMessage::Barrier(barrier) => {
                yield ArrangeMessage::Arrange(std::mem::take(&mut arrange_updates));
                yield ArrangeMessage::Barrier(barrier);
            }
        }
    }
}

/// Join the stream with the current state of the arrangement.
///
/// For example, the executor will receive the following message sequence from
/// `stream_lookup_arrange_this_epoch`:
///
/// * [Msg] Barrier (prev = [1], current = [2])
/// * [Msg] Arrangement (batch)
/// * [Do] replicate batch with epoch [2]
/// * [Msg] Stream (key = a)
/// * [Do] lookup `a` in arrangement of epoch [2] (current epoch)
/// * Barrier (prev = [2], current = [3])
#[allow(dead_code)]
#[try_stream(ok = ArrangeMessage, error = RwError)]
async fn stream_lookup_arrange_this_epoch(
    stream: Box<dyn Executor>,
    arrangement: Box<dyn Executor>,
) {
    let mut input = BarrierAligner::new(stream, arrangement);
    let mut stream_buf = vec![];
    let mut arrange_updates = vec![];

    loop {
        match input.next().await {
            AlignedMessage::Left(msg) => {
                // Should wait until arrangement from this epoch is available.
                stream_buf.push(msg?);
            }
            AlignedMessage::Right(msg) => {
                // For message from the arrangement side, we always send in batch.
                arrange_updates.push(msg?);
            }
            AlignedMessage::Barrier(barrier) => {
                yield ArrangeMessage::Arrange(std::mem::take(&mut arrange_updates));
                for msg in std::mem::take(&mut stream_buf) {
                    yield ArrangeMessage::Stream(msg);
                }
                yield ArrangeMessage::Barrier(barrier);
            }
        }
    }
}

impl<S: StateStore> std::fmt::Debug for ArrangeJoinSide<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrangeJoinSide")
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .field("use_current_epoch", &self.use_current_epoch)
            .finish()
    }
}
