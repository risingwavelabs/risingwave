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

use async_trait::async_trait;
use futures::StreamExt;
use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::ExecutorBuilder;
use crate::executor_v2::{Barrier, BoxedMessageStream, Executor, PkIndices, PkIndicesRef};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

mod cache;
mod sides;
use self::cache::LookupCache;
use self::sides::*;
mod impl_;

pub use impl_::LookupExecutorParams;

use super::BoxedExecutor;

#[cfg(test)]
mod tests;

/// `LookupExecutor` takes one input stream and one arrangement. It joins the input stream with the
/// arrangement. Currently, it only supports inner join. See `LookupExecutorParams` for more
/// information.
///
/// The output schema is `| stream columns | arrangement columns |`.
/// The input is required to be first stream and then arrangement.
pub struct LookupExecutor<S: StateStore> {
    /// the data types of the produced data chunk inside lookup (before reordering)
    chunk_data_types: Vec<DataType>,

    /// The schema of the lookup executor (after reordering)
    schema: Schema,

    /// The primary key indices of the schema (after reordering)
    pk_indices: PkIndices,

    /// The join side of the arrangement
    arrangement: ArrangeJoinSide<S>,

    /// The join side of the stream
    stream: StreamJoinSide,

    /// The executor for arrangement.
    arrangement_executor: Option<Box<dyn Executor>>,

    /// The executor for stream.
    stream_executor: Option<Box<dyn Executor>>,

    /// The last received barrier.
    last_barrier: Option<Barrier>,

    /// Information of column reordering
    column_mapping: Vec<usize>,

    /// When we receive a row from the stream side, we will first convert it to join key, and then
    /// map it to arrange side. For example, if we receive `[a, b, c]` from the stream side, where:
    /// `stream_join_key = [1, 2]`, `arrange_join_key = [3, 2]` with order rules [2 ascending, 3
    /// ascending].
    ///
    /// * We will first extract join key `[b, c]`,
    /// * then map it to the order of arrangement join key `[c, b]`.
    ///
    /// This vector records such mapping.
    key_indices_mapping: Vec<usize>,

    /// The cache for arrangement side.
    lookup_cache: LookupCache,
}

#[async_trait]
impl<S: StateStore> Executor for LookupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "LookupExecutor"
    }
}

pub struct LookupExecutorBuilder {}

impl ExecutorBuilder for LookupExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let lookup = try_match_expand!(node.get_node().unwrap(), Node::LookupNode)?;

        let arrangement = params.input.remove(1);
        let stream = params.input.remove(0);

        let arrangement_order_rules = lookup
            .arrange_key
            .iter()
            // TODO: allow descending order
            .map(|x| OrderPair::new(*x as usize, OrderType::Ascending))
            .collect();

        let arrangement_table_id = match lookup.arrangement_table_id.as_ref().unwrap() {
            ArrangementTableId::IndexId(x) => *x,
            ArrangementTableId::TableId(x) => *x,
        };

        let arrangement_col_descs = lookup
            .get_arrangement_table_info()?
            .column_descs
            .iter()
            .map(ColumnDesc::from)
            .collect();

        Ok(Box::new(LookupExecutor::new(LookupExecutorParams {
            schema: Schema::new(node.fields.iter().map(Field::from).collect()),
            arrangement,
            stream,
            arrangement_keyspace: Keyspace::table_root(store, &TableId::from(arrangement_table_id)),
            arrangement_col_descs,
            arrangement_order_rules,
            pk_indices: params.pk_indices,
            use_current_epoch: lookup.use_current_epoch,
            stream_join_key_indices: lookup.stream_key.iter().map(|x| *x as usize).collect(),
            arrange_join_key_indices: lookup.arrange_key.iter().map(|x| *x as usize).collect(),
            column_mapping: lookup.column_mapping.iter().map(|x| *x as usize).collect(),
        })))
    }
}
