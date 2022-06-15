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

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::iter::Iterator;
use std::sync::Arc;
use std::vec::Vec;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_common::util::encoding_for_comparison::{encode_chunk, is_type_encodable};
use risingwave_common::util::sort_util::{compare_two_row, HeapElem, OrderPair};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct OrderByExecutor {
    child: Option<BoxedExecutor>,
    sorted_indices: Vec<Vec<usize>>,
    chunks: Vec<DataChunk>,
    vis_indices: Vec<usize>,
    min_heap: BinaryHeap<HeapElem>,
    order_pairs: Arc<Vec<OrderPair>>,
    encoded_keys: Vec<Arc<Vec<Vec<u8>>>>,
    encodable: bool,
    disable_encoding: bool,
    identity: String,
    chunk_size: usize,
    schema: Schema,
}

#[expect(clippy::too_many_arguments)]
impl OrderByExecutor {
    fn new(
        child: BoxedExecutor,
        sorted_indices: Vec<Vec<usize>>,
        chunks: Vec<DataChunk>,
        vis_indices: Vec<usize>,
        min_heap: BinaryHeap<HeapElem>,
        order_pairs: Arc<Vec<OrderPair>>,
        encoded_keys: Vec<Arc<Vec<Vec<u8>>>>,
        encodable: bool,
        disable_encoding: bool,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child: Some(child),
            sorted_indices,
            chunks,
            vis_indices,
            min_heap,
            order_pairs,
            encoded_keys,
            encodable,
            disable_encoding,
            identity,
            chunk_size,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for OrderByExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 1,
            "OrderByExecutor should have only 1 child!"
        );

        let order_by_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::OrderBy
        )?;

        let order_pairs = order_by_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        Ok(Box::new(OrderByExecutor::new(
            inputs.remove(0),
            vec![],
            vec![],
            vec![],
            BinaryHeap::new(),
            Arc::new(order_pairs),
            vec![],
            false,
            false,
            source.plan_node().get_identity().clone(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        )))
    }
}

impl OrderByExecutor {
    fn push_heap_for_chunk(&mut self, idx: usize) {
        while self.vis_indices[idx] < self.chunks[idx].cardinality() {
            let skip: bool = match self.chunks[idx].visibility() {
                Some(visibility) => visibility
                    .is_set(self.sorted_indices[idx][self.vis_indices[idx]])
                    .map(|b| !b)
                    .unwrap_or(false),
                None => false,
            };
            if !skip {
                let elem_idx = self.sorted_indices[idx][self.vis_indices[idx]];
                let elem = HeapElem {
                    order_pairs: self.order_pairs.clone(),
                    chunk: self.chunks[idx].clone(),
                    chunk_idx: idx,
                    elem_idx,
                    encoded_chunk: if self.encodable && !self.disable_encoding {
                        Some(self.encoded_keys[idx].clone())
                    } else {
                        None
                    },
                };
                self.min_heap.push(elem);
                self.vis_indices[idx] += 1;
                break;
            }
            self.vis_indices[idx] += 1;
        }
    }

    fn get_order_index_from(&self, idx: usize) -> Vec<usize> {
        let mut index: Vec<usize> = (0..self.chunks[idx].cardinality()).collect();
        index.sort_by(|ia, ib| {
            if self.disable_encoding || !self.encodable {
                compare_two_row(
                    self.order_pairs.as_ref(),
                    &self.chunks[idx],
                    *ia,
                    &self.chunks[idx],
                    *ib,
                )
                .unwrap_or(Ordering::Equal)
            } else {
                let lhs_key = self.encoded_keys[idx][*ia].as_slice();
                let rhs_key = self.encoded_keys[idx][*ib].as_slice();
                lhs_key.cmp(rhs_key)
            }
        });
        index
    }

    async fn collect_child_data(&mut self) -> Result<()> {
        let mut stream = self.child.take().unwrap().execute();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if !self.disable_encoding && self.encodable {
                self.encoded_keys
                    .push(encode_chunk(&chunk, self.order_pairs.clone()));
            }
            self.chunks.push(chunk);
            self.sorted_indices
                .push(self.get_order_index_from(self.chunks.len() - 1));
        }
        self.vis_indices = vec![0usize; self.chunks.len()];
        for idx in 0..self.chunks.len() {
            self.push_heap_for_chunk(idx);
        }
        Ok(())
    }
}

impl Executor for OrderByExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl OrderByExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        if !self.disable_encoding {
            let schema = self.schema();
            self.encodable = self
                .order_pairs
                .iter()
                .map(|pair| schema.fields[pair.column_idx].data_type.clone())
                .all(is_type_encodable)
        }

        self.collect_child_data().await?;

        loop {
            let mut array_builders = self.schema().create_array_builders(self.chunk_size)?;

            let mut chunk_size = 0usize;
            while !self.min_heap.is_empty() && chunk_size < self.chunk_size {
                let top = self.min_heap.pop().unwrap();
                for (idx, builder) in array_builders.iter_mut().enumerate() {
                    let chunk_arr = self.chunks[top.chunk_idx].column_at(idx).array();
                    let chunk_arr = chunk_arr.as_ref();
                    macro_rules! gen_match {
                        ($b: ident, $a: ident, [$( $tt: ident), *]) => {
                            match ($b, $a) {
                                $((ArrayBuilderImpl::$tt($b), ArrayImpl::$tt($a)) => Ok($b.append($a.value_at(top.elem_idx))),)*
                                    _ => Err(InternalError(String::from("Unmatched array and array builder types"))),
                            }?
                        }
                    }
                    let _ = gen_match!(
                        builder,
                        chunk_arr,
                        [
                            Int16,
                            Int32,
                            Int64,
                            Float32,
                            Float64,
                            Utf8,
                            Bool,
                            Decimal,
                            Interval,
                            NaiveDate,
                            NaiveTime,
                            NaiveDateTime
                        ]
                    );
                }
                chunk_size += 1;
                self.push_heap_for_chunk(top.chunk_idx);
            }
            if chunk_size == 0 {
                break;
            }
            let columns = array_builders
                .into_iter()
                .map(|b| Ok(Column::new(Arc::new(b.finish()?))))
                .collect::<Result<Vec<_>>>()?;
            let chunk = DataChunk::new(columns, chunk_size);
            yield chunk
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::DataChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    #[tokio::test]
    async fn test_simple_order_by_executor() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i
             1 3
             2 2
             3 1",
        ));
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];

        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            vec![],
            vec![],
            vec![],
            BinaryHeap::new(),
            Arc::new(order_pairs),
            vec![],
            false,
            false,
            "OrderByExecutor2".to_string(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
            assert_eq!(col0.array().as_int32().value_at(2), Some(1));
        }
    }

    #[tokio::test]
    async fn test_encoding_for_float() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Float32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            " f    F
             -2.2  3.3
             -1.1  2.2
              1.1  1.1
              2.2 -1.1
              3.3 -2.2",
        ));
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            vec![],
            vec![],
            vec![],
            BinaryHeap::new(),
            Arc::new(order_pairs),
            vec![],
            false,
            false,
            "OrderByExecutor2".to_string(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Float32);
        assert_eq!(fields[1].data_type, DataType::Float64);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_float32().value_at(0), Some(3.3.into()));
            assert_eq!(col0.array().as_float32().value_at(1), Some(2.2.into()));
            assert_eq!(col0.array().as_float32().value_at(2), Some(1.1.into()));
            assert_eq!(col0.array().as_float32().value_at(3), Some((-1.1).into()));
            assert_eq!(col0.array().as_float32().value_at(4), Some((-2.2).into()));
        }
    }

    #[tokio::test]
    async fn test_bsc_for_string() {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(DataChunk::from_pretty(
            "T   T
             1.1 3.3
             2.2 2.2
             3.3 1.1",
        ));
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let order_by_executor = Box::new(OrderByExecutor::new(
            Box::new(mock_executor),
            vec![],
            vec![],
            vec![],
            BinaryHeap::new(),
            Arc::new(order_pairs),
            vec![],
            false,
            false,
            "OrderByExecutor2".to_string(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        ));
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Varchar);
        assert_eq!(fields[1].data_type, DataType::Varchar);

        let mut stream = order_by_executor.execute();
        let res = stream.next().await;
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let res = res.unwrap();
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_utf8().value_at(0), Some("3.3"));
            assert_eq!(col0.array().as_utf8().value_at(1), Some("2.2"));
            assert_eq!(col0.array().as_utf8().value_at(2), Some("1.1"));
        }
    }

    // TODO: enable benches

    // fn benchmark_1e4(b: &mut Bencher, enable_encoding: bool) {
    //     // gen random vec for i16 float bool and str
    //     let scale = 10000;
    //     let width = 10;
    //     let int_vec: Vec<Option<i16>> = rand::thread_rng()
    //         .sample_iter(Standard)
    //         .take(scale)
    //         .map(Some)
    //         .collect_vec();
    //     let bool_vec: Vec<Option<bool>> = rand::thread_rng()
    //         .sample_iter(Standard)
    //         .take(scale)
    //         .map(Some)
    //         .collect_vec();
    //     let float_vec: Vec<Option<f32>> = rand::thread_rng()
    //         .sample_iter(Standard)
    //         .take(scale)
    //         .map(Some)
    //         .collect_vec();
    //     let mut str_vec = Vec::<Option<String>>::new();
    //     for _ in 0..scale {
    //         let len = rand::thread_rng().sample(Uniform::<usize>::new(1, width));
    //         let s: String = rand::thread_rng()
    //             .sample_iter(&Alphanumeric)
    //             .take(len)
    //             .map(char::from)
    //             .collect();
    //         str_vec.push(Some(s));
    //     }
    //     b.iter(|| {
    //         let col0 = create_column_i16(int_vec.as_slice()).unwrap();
    //         let col1 = create_column_bool(bool_vec.as_slice()).unwrap();
    //         let col2 = create_column_f32(float_vec.as_slice()).unwrap();
    //         let col3 = create_column_string(str_vec.as_slice()).unwrap();
    //         let data_chunk = DataChunk::builder()
    //             .columns([col0, col1, col2, col3].to_vec())
    //             .build();
    //         let schema = Schema {
    //             fields: vec![
    //                 Field::unnamed(DataType::Int16),
    //                 Field::unnamed(DataType::Boolean),
    //                 Field::unnamed(DataType::Float32),
    //                 Field::unnamed(DataType::Varchar),
    //             ],
    //         };
    //         let mut mock_executor = MockExecutor::new(schema);
    //         mock_executor.add(data_chunk);
    //         let order_pairs = vec![
    //             OrderPair {
    //                 column_idx: 1,
    //                 order_type: OrderType::Ascending,
    //             },
    //             OrderPair {
    //                 column_idx: 0,
    //                 order_type: OrderType::Descending,
    //             },
    //             OrderPair {
    //                 column_idx: 3,
    //                 order_type: OrderType::Descending,
    //             },
    //             OrderPair {
    //                 column_idx: 2,
    //                 order_type: OrderType::Ascending,
    //             },
    //         ];
    //         let mut order_by_executor = OrderByExecutor {
    //             order_pairs: Arc::new(order_pairs),
    //             child: Box::new(mock_executor),
    //             vis_indices: vec![],
    //             chunks: vec![],
    //             sorted_indices: vec![],
    //             min_heap: BinaryHeap::new(),
    //             encoded_keys: vec![],
    //             encodable: false,
    //             disable_encoding: !enable_encoding,
    //             identity: "OrderByExecutor".to_string(),
    //         };
    //         let future = order_by_executor.open();
    //         tokio_test::block_on(future).unwrap();
    //         let future = order_by_executor.next();
    //         let res = tokio_test::block_on(future).unwrap();
    //         assert!(matches!(res, Some(_)));
    //     });
    // }

    // #[bench]
    // fn benchmark_bsc_1e4(b: &mut Bencher) {
    //     benchmark_1e4(b, true);
    // }

    // #[bench]
    // fn benchmark_baseline_1e4(b: &mut Bencher) {
    //     benchmark_1e4(b, false);
    // }
}
