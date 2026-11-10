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

use risingwave_common::bail;
use risingwave_common::fulltext::Analyzer;
use risingwave_pb::stream_plan::FullTextIndexWriteNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{Executor, FullTextIndexWriteExecutor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct FullTextIndexWriteExecutorBuilder;

impl_stream_node_body!(FullTextIndexWrite(FullTextIndexWriteNode) => FullTextIndexWriteExecutorBuilder);

impl ExecutorBuilder for FullTextIndexWriteExecutorBuilder {
    type Node = FullTextIndexWriteNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let table = node.table.as_ref().unwrap();
        assert_eq!(table.columns.len(), input.schema().len());
        let Some(info) = table.full_text_index_info.as_ref() else {
            bail!(
                "full-text index table {} has no full-text index info",
                table.id
            )
        };
        let Some(analyzer) = Analyzer::from_name(&info.analyzer) else {
            bail!("unknown full-text analyzer: {}", info.analyzer)
        };
        if analyzer.version() != info.analyzer_version {
            bail!(
                "full-text analyzer {} version mismatch: index {}, binary {}",
                info.analyzer,
                info.analyzer_version,
                analyzer.version()
            )
        }
        let executor = FullTextIndexWriteExecutor::new(
            input,
            store,
            table.id,
            analyzer,
            params.actor_context,
            params.vnode_bitmap,
        )
        .await?;
        Ok(Executor::new(params.info, Box::new(executor)))
    }
}
