// Copyright 2023 RisingWave Labs
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
