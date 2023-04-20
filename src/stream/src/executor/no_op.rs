use risingwave_common::catalog::Schema;

use super::{ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, PkIndicesRef};

pub struct NoOpExecutor {
    _ctx: ActorContextRef,
    identity: String,
    input: BoxedExecutor,
}

impl NoOpExecutor {
    pub fn new(ctx: ActorContextRef, input: BoxedExecutor, executor_id: u64) -> Self {
        Self {
            _ctx: ctx,
            identity: format!("BarrierRecvExecutor {:X}", executor_id),
            input,
        }
    }
}

impl Executor for NoOpExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.input.execute()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
