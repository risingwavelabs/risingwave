use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::iter::Iterator;
use std::sync::Arc;
use std::vec::Vec;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, DataChunkRef,
};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::expr::Expression;
use risingwave_common::util::encoding_for_comparison::{encode_chunk, is_type_encodable};
use risingwave_common::util::sort_util::{
    compare_two_row, fetch_orders, HeapElem, OrderPair, K_PROCESSING_WINDOW_SIZE,
};
use risingwave_pb::plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub(super) struct OrderByExecutor {
    child: BoxedExecutor,
    sorted_indices: Vec<Vec<usize>>,
    chunks: Vec<DataChunkRef>,
    vis_indices: Vec<usize>,
    min_heap: BinaryHeap<HeapElem>,
    order_pairs: Arc<Vec<OrderPair>>,
    encoded_keys: Vec<Arc<Vec<Vec<u8>>>>,
    encodable: bool,
    disable_encoding: bool,
    identity: String,
}

impl BoxedExecutorBuilder for OrderByExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);

        let order_by_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::OrderBy
        )?;

        let order_pairs = fetch_orders(order_by_node.get_column_orders()).unwrap();
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build()?;
            return Ok(Box::new(Self {
                order_pairs: Arc::new(order_pairs),
                child,
                vis_indices: vec![],
                chunks: vec![],
                sorted_indices: vec![],
                min_heap: BinaryHeap::new(),
                encoded_keys: vec![],
                encodable: false,
                disable_encoding: false,
                identity: source.plan_node().get_identity().clone(),
            }));
        }
        Err(InternalError("OrderBy must have one child".to_string()).into())
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
                    self.chunks[idx].as_ref(),
                    *ia,
                    self.chunks[idx].as_ref(),
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
        while let Some(chunk) = self.child.next().await? {
            if !self.disable_encoding && self.encodable {
                self.encoded_keys
                    .push(encode_chunk(&chunk, self.order_pairs.clone()));
            }
            self.chunks.push(Arc::new(chunk));
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

#[async_trait::async_trait]
impl Executor for OrderByExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;

        if !self.disable_encoding {
            let schema = self.schema();
            self.encodable = self
                .order_pairs
                .iter()
                .map(|pair| schema.fields[pair.column_idx].data_type)
                .all(|t| is_type_encodable(t))
        }

        self.collect_child_data().await?;

        self.child.close().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let data_types = self
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type)
            .collect_vec();
        let mut builders = data_types
            .iter()
            .map(|t| t.create_array_builder(K_PROCESSING_WINDOW_SIZE))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;
        let mut chunk_size = 0usize;
        while !self.min_heap.is_empty() && chunk_size < K_PROCESSING_WINDOW_SIZE {
            let top = self.min_heap.pop().unwrap();
            for (idx, builder) in builders.iter_mut().enumerate() {
                let chunk_arr = self.chunks[top.chunk_idx].column_at(idx)?.array();
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
            return Ok(None);
        }
        let columns = builders
            .into_iter()
            .map(|b| Ok(Column::new(Arc::new(b.finish()?))))
            .collect::<Result<Vec<_>>>()?;
        let chunk = DataChunk::builder().columns(columns).build();
        Ok(Some(chunk))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use rand::distributions::{Alphanumeric, Standard, Uniform};
    use rand::Rng;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{BoolArray, DataChunk, PrimitiveArray, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::{DataType, OrderedF32, OrderedF64};
    use risingwave_common::util::sort_util::OrderType;
    use test::Bencher;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_column_i32(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    fn create_column_i16(vec: &[Option<i16>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    fn create_column_bool(vec: &[Option<bool>]) -> Result<Column> {
        let array = BoolArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    fn create_column_f32(vec: &[Option<f32>]) -> Result<Column> {
        let vec = vec.iter().map(|o| o.map(OrderedF32::from)).collect_vec();
        let array = PrimitiveArray::from_slice(&vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    fn create_column_f64(vec: &[Option<f64>]) -> Result<Column> {
        let vec = vec.iter().map(|o| o.map(OrderedF64::from)).collect_vec();
        let array = PrimitiveArray::from_slice(&vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    fn create_column_string(vec: &[Option<String>]) -> Result<Column> {
        let str_vec = vec
            .iter()
            .map(|s| s.as_ref().map(|s| s.as_str()))
            .collect_vec();
        let array = Utf8Array::from_slice(&str_vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    #[tokio::test]
    async fn test_simple_order_by_executor() {
        let col0 = create_column_i32(&[Some(1), Some(2), Some(3)]).unwrap();
        let col1 = create_column_i32(&[Some(3), Some(2), Some(1)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col0, col1].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
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
        let mut order_by_executor = OrderByExecutor {
            order_pairs: Arc::new(order_pairs),
            child: Box::new(mock_executor),
            vis_indices: vec![],
            chunks: vec![],
            sorted_indices: vec![],
            min_heap: BinaryHeap::new(),
            encoded_keys: vec![],
            encodable: false,
            disable_encoding: false,
            identity: "OrderByExecutor".to_string(),
        };
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        order_by_executor.open().await.unwrap();
        let res = order_by_executor.next().await.unwrap();
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
            assert_eq!(col0.array().as_int32().value_at(2), Some(1));
        }
        order_by_executor.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_encoding_for_float() {
        let col0 =
            create_column_f32(&[Some(-2.2), Some(-1.1), Some(1.1), Some(2.2), Some(3.3)]).unwrap();
        let col1 =
            create_column_f64(&[Some(3.3), Some(2.2), Some(1.1), Some(-1.1), Some(-2.2)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col0, col1].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Float32),
                Field::unnamed(DataType::Float64),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
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
        let mut order_by_executor = OrderByExecutor {
            order_pairs: Arc::new(order_pairs),
            child: Box::new(mock_executor),
            vis_indices: vec![],
            chunks: vec![],
            sorted_indices: vec![],
            min_heap: BinaryHeap::new(),
            encoded_keys: vec![],
            encodable: false,
            disable_encoding: false,
            identity: "OrderByExecutor".to_string(),
        };
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Float32);
        assert_eq!(fields[1].data_type, DataType::Float64);
        order_by_executor.open().await.unwrap();
        let res = order_by_executor.next().await.unwrap();
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_float32().value_at(0), Some(3.3.into()));
            assert_eq!(col0.array().as_float32().value_at(1), Some(2.2.into()));
            assert_eq!(col0.array().as_float32().value_at(2), Some(1.1.into()));
            assert_eq!(col0.array().as_float32().value_at(3), Some((-1.1).into()));
            assert_eq!(col0.array().as_float32().value_at(4), Some((-2.2).into()));
        }
    }

    #[tokio::test]
    async fn test_bsc_for_string() {
        let col0 = create_column_string(&[
            Some("1.1".to_string()),
            Some("2.2".to_string()),
            Some("3.3".to_string()),
        ])
        .unwrap();
        let col1 = create_column_string(&[
            Some("3.3".to_string()),
            Some("2.2".to_string()),
            Some("1.1".to_string()),
        ])
        .unwrap();
        let data_chunk = DataChunk::builder().columns([col0, col1].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Varchar),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
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
        let mut order_by_executor = OrderByExecutor {
            order_pairs: Arc::new(order_pairs),
            child: Box::new(mock_executor),
            vis_indices: vec![],
            chunks: vec![],
            sorted_indices: vec![],
            min_heap: BinaryHeap::new(),
            encoded_keys: vec![],
            encodable: false,
            disable_encoding: false,
            identity: "OrderByExecutor".to_string(),
        };
        let fields = &order_by_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Varchar);
        assert_eq!(fields[1].data_type, DataType::Varchar);
        order_by_executor.open().await.unwrap();
        let res = order_by_executor.next().await.unwrap();
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            let col0 = res.column_at(0).unwrap();
            assert_eq!(col0.array().as_utf8().value_at(0), Some("3.3"));
            assert_eq!(col0.array().as_utf8().value_at(1), Some("2.2"));
            assert_eq!(col0.array().as_utf8().value_at(2), Some("1.1"));
        }
    }

    fn benchmark_1e4(b: &mut Bencher, enable_encoding: bool) {
        // gen random vec for i16 float bool and str
        let scale = 10000;
        let width = 10;
        let int_vec: Vec<Option<i16>> = rand::thread_rng()
            .sample_iter(Standard)
            .take(scale)
            .map(Some)
            .collect_vec();
        let bool_vec: Vec<Option<bool>> = rand::thread_rng()
            .sample_iter(Standard)
            .take(scale)
            .map(Some)
            .collect_vec();
        let float_vec: Vec<Option<f32>> = rand::thread_rng()
            .sample_iter(Standard)
            .take(scale)
            .map(Some)
            .collect_vec();
        let mut str_vec = Vec::<Option<String>>::new();
        for _ in 0..scale {
            let len = rand::thread_rng().sample(Uniform::<usize>::new(1, width));
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(len)
                .map(char::from)
                .collect();
            str_vec.push(Some(s));
        }
        b.iter(|| {
            let col0 = create_column_i16(int_vec.as_slice()).unwrap();
            let col1 = create_column_bool(bool_vec.as_slice()).unwrap();
            let col2 = create_column_f32(float_vec.as_slice()).unwrap();
            let col3 = create_column_string(str_vec.as_slice()).unwrap();
            let data_chunk = DataChunk::builder()
                .columns([col0, col1, col2, col3].to_vec())
                .build();
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int16),
                    Field::unnamed(DataType::Boolean),
                    Field::unnamed(DataType::Float32),
                    Field::unnamed(DataType::Varchar),
                ],
            };
            let mut mock_executor = MockExecutor::new(schema);
            mock_executor.add(data_chunk);
            let order_pairs = vec![
                OrderPair {
                    column_idx: 1,
                    order_type: OrderType::Ascending,
                },
                OrderPair {
                    column_idx: 0,
                    order_type: OrderType::Descending,
                },
                OrderPair {
                    column_idx: 3,
                    order_type: OrderType::Descending,
                },
                OrderPair {
                    column_idx: 2,
                    order_type: OrderType::Ascending,
                },
            ];
            let mut order_by_executor = OrderByExecutor {
                order_pairs: Arc::new(order_pairs),
                child: Box::new(mock_executor),
                vis_indices: vec![],
                chunks: vec![],
                sorted_indices: vec![],
                min_heap: BinaryHeap::new(),
                encoded_keys: vec![],
                encodable: false,
                disable_encoding: !enable_encoding,
                identity: "OrderByExecutor".to_string(),
            };
            let future = order_by_executor.open();
            tokio_test::block_on(future).unwrap();
            let future = order_by_executor.next();
            let res = tokio_test::block_on(future).unwrap();
            assert!(matches!(res, Some(_)));
        });
    }

    #[bench]
    fn benchmark_bsc_1e4(b: &mut Bencher) {
        benchmark_1e4(b, true);
    }

    #[bench]
    fn benchmark_baseline_1e4(b: &mut Bencher) {
        benchmark_1e4(b, false);
    }
}
