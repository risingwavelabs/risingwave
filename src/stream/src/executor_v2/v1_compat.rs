// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::ColumnId;
pub use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::hash::HashKey;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_expr::expr::BoxedExpression;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::{Keyspace, StateStore};

use super::error::{StreamExecutorError, TracedStreamExecutorError};
use super::filter::SimpleFilterExecutor;
use super::{
    BatchQueryExecutor, BoxedExecutor, ChainExecutor, Executor, ExecutorInfo, FilterExecutor,
    HashAggExecutor, LocalSimpleAggExecutor, MaterializeExecutor,
};
pub use super::{BoxedMessageStream, ExecutorV1, Message, PkIndices, PkIndicesRef};
use crate::executor::AggCall;
use crate::executor_v2::global_simple_agg::SimpleAggExecutor;
use crate::task::FinishCreateMviewNotifier;

/// The struct wraps a [`BoxedMessageStream`] and implements the interface of [`ExecutorV1`].
///
/// With this wrapper, we can migrate our executors from v1 to v2 step by step.
pub struct StreamExecutorV1 {
    /// The wrapped uninited executor.
    pub(super) executor_v2: Option<BoxedExecutor>,

    /// The wrapped stream.
    pub(super) stream: Option<BoxedMessageStream>,

    pub(super) info: ExecutorInfo,
}

impl fmt::Debug for StreamExecutorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamExecutor")
            .field("info", &self.info)
            .finish()
    }
}

#[async_trait]
impl ExecutorV1 for StreamExecutorV1 {
    async fn next(&mut self) -> Result<Message> {
        let stream = self.stream.as_mut().expect("not inited");

        match stream.next().await {
            Some(result) => result.map_err(RwError::from),
            None => Err(ErrorCode::Eof.into()), // we use `Eof` to represent end of stream in v1
        }
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn logical_operator_info(&self) -> &str {
        // FIXME: use identity temporally.
        &self.info.identity
    }

    fn init(&mut self, epoch: u64) -> Result<()> {
        let executor = self.executor_v2.take().expect("already inited");
        self.stream = Some(executor.execute_with_epoch(epoch));
        Ok(())
    }
}

pub struct ExecutorV1AsV2(pub Box<dyn ExecutorV1>);

impl Executor for ExecutorV1AsV2 {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        self.0.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.0.pk_indices()
    }

    fn identity(&self) -> &str {
        self.0.identity()
    }

    fn execute_with_epoch(mut self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        self.0.init(epoch).expect("failed to init executor epoch");
        self.execute()
    }
}

impl ExecutorV1AsV2 {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        loop {
            let msg = self.0.next().await;
            match msg {
                // For snapshot input of `Chain`, we use `Eof` to represent the end of stream.
                Err(e) if matches!(e.inner(), ErrorCode::Eof) => break,
                _ => yield msg.map_err(StreamExecutorError::executor_v1)?,
            }
        }
    }
}

impl FilterExecutor {
    pub fn new_from_v1(
        input: Box<dyn ExecutorV1>,
        expr: BoxedExpression,
        executor_id: u64,
        _op_info: String,
    ) -> Self {
        let info = ExecutorInfo {
            schema: input.schema().to_owned(),
            pk_indices: input.pk_indices().to_owned(),
            identity: "Filter".to_owned(),
        };
        let input = Box::new(ExecutorV1AsV2(input));
        super::SimpleExecutorWrapper {
            input,
            inner: SimpleFilterExecutor::new(info, expr, executor_id),
        }
    }
}

impl ChainExecutor {
    pub fn new_from_v1(
        snapshot: Box<dyn ExecutorV1>,
        mview: Box<dyn ExecutorV1>,
        notifier: FinishCreateMviewNotifier,
        schema: Schema,
        column_idxs: Vec<usize>,
        _op_info: String,
    ) -> Self {
        let info = ExecutorInfo {
            schema,
            pk_indices: mview.pk_indices().to_owned(),
            identity: "Chain".to_owned(),
        };

        let actor_id = notifier.actor_id;

        Self::new(
            Box::new(ExecutorV1AsV2(snapshot)),
            Box::new(ExecutorV1AsV2(mview)),
            column_idxs,
            notifier,
            actor_id,
            info,
        )
    }
}

impl<S: StateStore> MaterializeExecutor<S> {
    pub fn new_from_v1(
        input: Box<dyn ExecutorV1>,
        keyspace: Keyspace<S>,
        keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        _op_info: String,
        key_indices: Vec<usize>,
    ) -> Self {
        Self::new(
            Box::new(ExecutorV1AsV2(input)),
            keyspace,
            keys,
            column_ids,
            executor_id,
            key_indices,
        )
    }
}

impl LocalSimpleAggExecutor {
    pub fn new_from_v1(
        input: Box<dyn ExecutorV1>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
        _op_info: String,
    ) -> Result<Self> {
        let input = Box::new(ExecutorV1AsV2(input));
        Self::new(input, agg_calls, pk_indices, executor_id)
    }
}

impl<S: StateStore> SimpleAggExecutor<S> {
    pub fn new_from_v1(
        input: Box<dyn ExecutorV1>,
        agg_calls: Vec<AggCall>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        _op_info: String,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let input = Box::new(ExecutorV1AsV2(input));
        Self::new(
            input,
            agg_calls,
            keyspace,
            pk_indices,
            executor_id,
            key_indices,
        )
    }
}

impl<K: HashKey, S: StateStore> HashAggExecutor<K, S> {
    pub fn new_from_v1(
        input: Box<dyn ExecutorV1>,
        agg_calls: Vec<AggCall>,
        key_indices: Vec<usize>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        _op_info: String,
    ) -> Result<Self> {
        let input = Box::new(ExecutorV1AsV2(input));
        Self::new(
            input,
            agg_calls,
            keyspace,
            pk_indices,
            executor_id,
            key_indices,
        )
    }
}

impl<S: StateStore> BatchQueryExecutor<S> {
    pub fn new_from_v1(
        table: CellBasedTable<S>,
        pk_indices: PkIndices,
        _op_info: String,
        key_indices: Vec<usize>,
    ) -> Self {
        let schema = table.schema().clone();
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: "BatchQuery".to_owned(),
        };

        Self::new(table, Self::DEFAULT_BATCH_SIZE, info, key_indices)
    }
}
