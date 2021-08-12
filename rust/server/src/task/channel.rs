use crate::array::DataChunk;
use risingwave_proto::plan::ShuffleInfo;

// `OutputChannel` is a synchronous, bounded producer-consumer queue.
// The producer is the local task executor, the consumer is ExchangeService.
// The implementation of `OutputChannel` depends on the shuffling strategy.
// In terms of rust, `OutputChannel` resembles async_std::channel,
// except that `OutputChannel` could have multiple receivers.
pub(crate) trait OutputChannel: Send + Sync {
  fn send(&mut self, chunk: Box<DataChunk>);

  // Returns `None` if there's no more data to read.
  // Otherwise it will wait until there's data.
  fn recv(&mut self, sink_id: u32) -> Optional<Box<DataChunk>>;

  // Signal the completion of the producer.
  fn complete_write(&mut self);
}

fn create_output_channel(shuffle: &ShuffleInfo) -> Box<OutputChannel>;
