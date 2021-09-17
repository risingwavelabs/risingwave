use super::Result;
use async_trait::async_trait;

/// `Processor` is the head of operators, aka. `OperatorHead`. Generally,
/// a `Processor` needs to run in the background, so as to poll data from
/// channels. `Processor` also controls task granularity and yields CPU
/// to the async runtime scheduler when needed.
#[async_trait]
pub trait Processor: Send + Sync + 'static {
    async fn run(self) -> Result<()>;
}
