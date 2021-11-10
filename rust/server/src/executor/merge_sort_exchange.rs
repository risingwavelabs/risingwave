use prost::Message;
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::execution::exchange_source::ExchangeSource;
use crate::executor::CreateSource;
use crate::executor::{
    BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult,
};
use crate::task::GlobalTaskEnv;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, DataChunkRef};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::Result;
use risingwave_common::types::{build_from_prost as type_build_from_prost, ToOwnedDatum};
use risingwave_common::util::sort_util::{
    fetch_orders, HeapElem, OrderPair, K_PROCESSING_WINDOW_SIZE,
};
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::task_service::exchange_node::Field as ExchangeNodeField;
use risingwave_pb::task_service::ExchangeSource as ProstExchangeSource;
use risingwave_pb::task_service::MergeSortExchangeNode;

/// `MergeSortExchangeExecutor` takes inputs from multiple sources and
/// The outputs of all the sources have been sorted in the same way.
///
/// The size of the output is determined both by `K_PROCESSING_WINDOW_SIZE`.
/// TODO: Does not handle `visibility` for now.
pub(super) struct MergeSortExchangeExecutor<C> {
    server_addr: SocketAddr,
    env: GlobalTaskEnv,
    /// keeps one data chunk of each source if any
    source_inputs: Vec<Option<DataChunkRef>>,
    order_pairs: Arc<Vec<OrderPair>>,
    min_heap: BinaryHeap<HeapElem>,
    proto_sources: Vec<ProstExchangeSource>,
    sources: Vec<Box<dyn ExchangeSource>>,
    /// Mock-able CreateSource.
    source_creator: PhantomData<C>,
    schema: Schema,
    first_execution: bool,
}

impl<CS: 'static + CreateSource> MergeSortExchangeExecutor<CS> {
    async fn get_source_chunk(&mut self, source_idx: usize) -> Result<()> {
        assert!(source_idx < self.source_inputs.len());
        let res = self.sources[source_idx].take_data().await?;
        match res {
            Some(chunk) => {
                let _ =
                    std::mem::replace(&mut self.source_inputs[source_idx], Some(Arc::new(chunk)));
            }
            None => {
                let _ = std::mem::replace(&mut self.source_inputs[source_idx], None);
            }
        }
        Ok(())
    }

    fn push_row_into_heap(&mut self, source_idx: usize, row_idx: usize) {
        assert!(source_idx < self.source_inputs.len());
        if let Some(chunk_ref) = &self.source_inputs[source_idx] {
            assert!(row_idx < chunk_ref.capacity());
            self.min_heap.push(HeapElem {
                order_pairs: self.order_pairs.clone(),
                chunk: chunk_ref.clone(),
                chunk_idx: source_idx,
                elem_idx: row_idx,
            });
        }
    }
}

#[async_trait::async_trait]
impl<CS: 'static + CreateSource> Executor for MergeSortExchangeExecutor<CS> {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Everytime `execute` is called, it tries to produce a chunk of size
    /// `K_PROCESSING_WINDOW_SIZE`. It is possible that the chunk's size is smaller than the
    /// `K_PROCESSING_WINDOW_SIZE` as the executor runs out of input from `sources`.
    async fn execute(&mut self) -> Result<ExecutorResult> {
        // If this is the first time execution, we first get one chunk from each source
        // and put one row of each chunk into the heap
        if self.first_execution {
            for source_idx in 0..self.proto_sources.len() {
                let new_source =
                    CS::create_source(self.env.clone(), &self.proto_sources[source_idx]).await?;
                let _ = self.sources.push(new_source);
                self.get_source_chunk(source_idx).await?;
                self.push_row_into_heap(source_idx, 0);
            }
            self.first_execution = false;
        }

        // If there is no rows in the heap,
        // we run out of input data chunks and emit `Done`.
        if self.min_heap.is_empty() {
            return Ok(ExecutorResult::Done);
        }

        // It is possible that we cannot produce this much as
        // we may run out of input data chunks from sources.
        let mut want_to_produce = K_PROCESSING_WINDOW_SIZE;

        let mut builders = self
            .schema()
            .fields
            .iter()
            .map(|field| {
                field
                    .data_type
                    .clone()
                    .create_array_builder(K_PROCESSING_WINDOW_SIZE)
            })
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;
        while want_to_produce > 0 && !self.min_heap.is_empty() {
            let top_elem = self.min_heap.pop().unwrap();
            let child_idx = top_elem.chunk_idx;
            let cur_chunk = top_elem.chunk;
            let row_idx = top_elem.elem_idx;
            for (idx, builder) in builders.iter_mut().enumerate() {
                let chunk_arr = cur_chunk.column_at(idx)?.array();
                let chunk_arr = chunk_arr.as_ref();
                let datum = chunk_arr.value_at(row_idx).to_owned_datum();
                builder.append_datum(&datum)?;
            }
            want_to_produce -= 1;
            // check whether we have another row from the same chunk being popped
            if row_idx == (cur_chunk.capacity() - 1) {
                self.get_source_chunk(child_idx).await?;
                self.push_row_into_heap(child_idx, 0);
            } else {
                self.push_row_into_heap(child_idx, row_idx + 1);
            }
        }

        let columns = self
            .schema()
            .fields
            .iter()
            .zip(builders)
            .map(|(field, builder)| {
                Ok(Column::new(
                    Arc::new(builder.finish()?),
                    field.data_type.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let chunk = DataChunk::builder().columns(columns).build();
        Ok(ExecutorResult::Batch(chunk))
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<CS: 'static + CreateSource> BoxedExecutorBuilder for MergeSortExchangeExecutor<CS> {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::MergeSortExchange);
        let plan_node = source.plan_node();
        let server_addr = *source.env.server_address();
        let sort_merge_node: MergeSortExchangeNode =
            MergeSortExchangeNode::decode(&(plan_node).get_body().value[..]).map_err(ProstError)?;
        let order_pairs = Arc::new(fetch_orders(sort_merge_node.get_column_orders()).unwrap());

        let exchange_node = sort_merge_node.get_exchange_node();
        let proto_sources: Vec<ProstExchangeSource> = exchange_node.get_sources().to_vec();
        ensure!(!exchange_node.get_sources().is_empty());
        let input_schema: Vec<ExchangeNodeField> = exchange_node.get_input_schema().to_vec();
        let fields = input_schema
            .iter()
            .map(|f| Field {
                data_type: type_build_from_prost(f.get_data_type()).unwrap(),
            })
            .collect::<Vec<Field>>();

        let num_sources = proto_sources.len();
        Ok(Box::new(Self {
            server_addr,
            env: source.env.clone(),
            source_inputs: vec![None; num_sources],
            order_pairs,
            min_heap: BinaryHeap::new(),
            proto_sources,
            sources: vec![],
            source_creator: PhantomData,
            schema: Schema { fields },
            first_execution: true,
        }))
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::Array;
    use risingwave_common::array::{DataChunk, I32Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::Int32Type;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;

    #[tokio::test]
    async fn test_exchange_multiple_sources() {
        struct FakeExchangeSource {
            chunk: Option<DataChunk>,
        }

        #[async_trait::async_trait]
        impl ExchangeSource for FakeExchangeSource {
            async fn take_data(&mut self) -> Result<Option<DataChunk>> {
                let chunk = self.chunk.take();
                Ok(chunk)
            }
        }

        struct FakeCreateSource {}

        #[async_trait::async_trait]
        impl CreateSource for FakeCreateSource {
            async fn create_source(
                _: GlobalTaskEnv,
                _: &ProstExchangeSource,
            ) -> Result<Box<dyn ExchangeSource>> {
                let chunk = DataChunk::builder()
                    .columns(vec![Column::new(
                        Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into()),
                        Int32Type::create(false),
                    )])
                    .build();
                Ok(Box::new(FakeExchangeSource { chunk: Some(chunk) }))
            }
        }

        let mut proto_sources: Vec<ProstExchangeSource> = vec![];
        let num_sources = 2;
        for _ in 0..num_sources {
            proto_sources.push(ProstExchangeSource::default());
        }
        let input_ref_1 = InputRefExpression::new(Int32Type::create(false), 0usize);
        let order_pairs = Arc::new(vec![OrderPair {
            order: Box::new(input_ref_1),
            order_type: OrderType::Ascending,
        }]);

        let mut executor = MergeSortExchangeExecutor::<FakeCreateSource> {
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            env: GlobalTaskEnv::for_test(),
            source_inputs: vec![None; proto_sources.len()],
            order_pairs,
            min_heap: BinaryHeap::new(),
            proto_sources,
            sources: vec![],
            source_creator: PhantomData,
            schema: Schema {
                fields: vec![Field {
                    data_type: Int32Type::create(false),
                }],
            },
            first_execution: true,
        };

        let res = executor.execute().await.unwrap();
        assert!(matches!(res, ExecutorResult::Batch(_)));
        if let ExecutorResult::Batch(res) = res {
            assert_eq!(res.capacity(), 3 * num_sources);
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_int32().value_at(0), Some(1));
            assert_eq!(col0.array().as_int32().value_at(1), Some(1));
            assert_eq!(col0.array().as_int32().value_at(2), Some(2));
            assert_eq!(col0.array().as_int32().value_at(3), Some(2));
            assert_eq!(col0.array().as_int32().value_at(4), Some(3));
            assert_eq!(col0.array().as_int32().value_at(5), Some(3));
        }
        assert!(matches!(
            executor.execute().await.unwrap(),
            ExecutorResult::Done
        ));
    }
}
