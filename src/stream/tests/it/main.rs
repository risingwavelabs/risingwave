mod prelude {
    pub use risingwave_common::array::StreamChunk;
    pub use risingwave_common::catalog::{Field, Schema};
    pub use risingwave_common::test_prelude::StreamChunkTestExt;
    pub use risingwave_common::types::DataType;
    pub use risingwave_expr::expr::build_from_pretty;
    pub use risingwave_expr::table_function::repeat;
    pub use risingwave_stream::executor::test_utils::snapshot::*;
    pub use risingwave_stream::executor::test_utils::{MessageSender, MockSource};
    pub use risingwave_stream::executor::{BoxedMessageStream, Executor, PkIndices};
}
mod project_set;
