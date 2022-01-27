use core::default::Default as CoreDefault;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::util::downcast_arc;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::{Column, DataType};
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::ConstantValue;
use risingwave_pb::plan::exchange_info::{Distribution, DistributionMode};
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::values_node::ExprTuple;
use risingwave_pb::plan::{
    exchange_info, ColumnDesc, CreateTableNode, ExchangeInfo, Field as NodeField, InsertNode,
    PlanFragment, PlanNode, QueryId, SeqScanNode, StageId, TaskId as ProstTaskId,
    TaskSinkId as ProstSinkId, ValuesNode,
};
use risingwave_pb::task_service::GetDataResponse;
use risingwave_storage::bummock::BummockTable;
use risingwave_storage::Table;

use super::*;
use crate::rpc::service::exchange::ExchangeWriter;

fn get_num_sinks(plan: &PlanFragment) -> Result<u32> {
    Ok(match plan.get_exchange_info()?.get_mode()? {
        DistributionMode::Single => 1,
        DistributionMode::Hash => match plan.get_exchange_info()?.distribution {
            Some(Distribution::HashInfo(ref v)) => v.output_count,
            _ => exchange_info::HashInfo::default().output_count,
        },
        DistributionMode::Broadcast => match plan.get_exchange_info()?.distribution {
            Some(Distribution::BroadcastInfo(ref v)) => v.count,
            _ => exchange_info::BroadcastInfo::default().count,
        },
    })
}

/// Write the execution results into a buffer for testing.
/// In a real server, the results will be flushed into a grpc sink.
struct FakeExchangeWriter {
    messages: Vec<GetDataResponse>,
}

#[async_trait::async_trait]
impl ExchangeWriter for FakeExchangeWriter {
    async fn write(&mut self, data: risingwave_pb::task_service::GetDataResponse) -> Result<()> {
        self.messages.push(data);
        Ok(())
    }
}

pub struct TestRunner {
    tid: ProstTaskId,
    env: BatchTaskEnv,
}

impl TestRunner {
    pub fn new() -> Self {
        let tid = ProstTaskId {
            stage_id: Some(StageId {
                query_id: Some(QueryId {
                    trace_id: "".to_string(),
                }),
                stage_id: 0,
            }),
            task_id: 0,
        };
        Self {
            tid,
            env: BatchTaskEnv::for_test(),
        }
    }

    pub fn prepare_table(&mut self) -> TableBuilder {
        TableBuilder::new(self)
    }

    pub fn prepare_scan(&mut self) -> SelectBuilder {
        SelectBuilder::new(self)
    }

    pub async fn run(&mut self, plan: PlanFragment) -> Result<Vec<Vec<GetDataResponse>>> {
        self.run_task(&plan)?;
        self.collect_task_output(&plan).await
    }

    pub fn run_task(&mut self, plan: &PlanFragment) -> Result<()> {
        let task_manager = self.env.task_manager();
        task_manager.fire_task(self.env.clone(), &self.tid, plan.clone())
    }

    pub async fn collect_task_output(
        &mut self,
        plan: &PlanFragment,
    ) -> Result<Vec<Vec<GetDataResponse>>> {
        let task_manager = self.env.task_manager();
        let mut res = Vec::new();
        let sink_ids = 0..get_num_sinks(plan)?;
        for sink_id in sink_ids {
            let proto_sink_id = ProstSinkId {
                task_id: Some(self.tid.clone()),
                sink_id,
            };
            let mut task_sink = task_manager.take_sink(&proto_sink_id)?;
            let mut writer = FakeExchangeWriter { messages: vec![] };
            task_sink.take_data(&mut writer).await.unwrap();
            res.push(writer.messages);
        }
        // In test, we remove the task manually, while in production,
        // it should be removed by the requests from the leader node.
        task_manager.remove_task(&self.tid)?;
        Ok(res)
    }

    fn get_global_env(&self) -> BatchTaskEnv {
        self.env.clone()
    }

    fn validate_insert_result(result: &[GetDataResponse], inserted_rows: usize) {
        ResultChecker::new()
            .add_i64_column(false, &[inserted_rows as i64])
            .check_result(result)
    }
}

pub struct TableBuilder<'a> {
    runner: &'a mut TestRunner,

    col_types: Vec<DataType>,
    tuples: Vec<Vec<ConstantValue>>,
}

impl<'a> TableBuilder<'a> {
    pub fn new(runner: &'a mut TestRunner) -> Self {
        Self {
            runner,
            col_types: vec![],
            tuples: vec![],
        }
    }

    pub fn create_table(mut self, col_types: &[TypeName]) -> Self {
        // the implicit row_id column
        self.col_types.push(DataType {
            type_name: TypeName::Int64 as i32,
            is_nullable: false,
            ..CoreDefault::default()
        });
        for type_name in col_types {
            let typ = DataType {
                type_name: *type_name as i32,
                is_nullable: false,
                ..CoreDefault::default()
            };
            self.col_types.push(typ);
        }
        self
    }

    pub fn create_table_int32s(self, col_num: usize) -> Self {
        let mut col_types = vec![];
        for _ in 0..col_num {
            col_types.push(TypeName::Int32);
        }
        self.create_table(&col_types)
    }

    pub fn set_nullable(&mut self, col_idx: usize) -> &mut Self {
        self.col_types.get_mut(col_idx).unwrap().is_nullable = true;
        self
    }

    pub fn insert_i32s(mut self, i32s: &[i32]) -> Self {
        assert_eq!(i32s.len(), self.col_types.len() - 1);
        let mut tuple = ConstantBuilder::new();
        for v in i32s {
            tuple.add_i32(v);
        }
        self.tuples.push(tuple.build());
        self
    }

    pub async fn run(self) {
        let inserted_rows = self.tuples.len();
        let create = self.build_create_table_plan();
        let insert = self
            .build_insert_values_plan()
            .expect("failed to create insert plan");
        assert_eq!(self.runner.run(create).await.unwrap()[0].len(), 0);
        TestRunner::validate_insert_result(
            &self.runner.run(insert).await.unwrap()[0],
            inserted_rows,
        );
    }

    fn build_create_table_plan(&self) -> PlanFragment {
        let create = CreateTableNode {
            table_ref_id: None,
            column_descs: self
                .col_types
                .iter()
                .enumerate()
                .map(|(i, typ)| ColumnDesc {
                    column_type: Some(typ.clone()),
                    column_id: i as i32, // use index as column_id
                    ..CoreDefault::default()
                })
                .collect_vec(),
            v2: false,
            is_materialized_view: false,
            ..Default::default()
        };

        PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                node_body: Some(NodeBody::CreateTable(create)),
            }),

            exchange_info: Some(ExchangeInfo {
                mode: 0,
                distribution: None,
            }),
        }
    }

    fn build_insert_values_plan(&self) -> Result<PlanFragment> {
        let insert = InsertNode {
            table_ref_id: None,
            column_ids: vec![0; self.col_types.len()],
        };

        let tuples = self
            .tuples
            .iter()
            .map(|tuple| TableBuilder::build_values(tuple.clone()))
            .collect_vec();
        let fields = tuples
            .first()
            .unwrap()
            .cells
            .iter()
            .map(|cell| {
                Ok(NodeField {
                    data_type: Some(cell.get_return_type()?.clone()),
                    name: String::new(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(PlanFragment {
            root: Some(PlanNode {
                children: vec![PlanNode {
                    children: vec![],
                    node_body: Some(NodeBody::Values(ValuesNode { tuples, fields })),
                }],
                node_body: Some(NodeBody::Insert(insert)),
            }),

            exchange_info: Some(ExchangeInfo {
                mode: 0,
                distribution: None,
            }),
        })
    }

    fn build_values(constants: Vec<ConstantValue>) -> ExprTuple {
        use risingwave_pb::expr::expr_node::Type;
        use risingwave_pb::expr::ExprNode;
        ExprTuple {
            cells: constants
                .into_iter()
                .map(|constant| ExprNode {
                    expr_type: Type::ConstantValue as i32,
                    return_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..CoreDefault::default()
                    }),
                    rex_node: Some(RexNode::Constant(ConstantValue {
                        body: constant.body,
                    })),
                })
                .collect_vec(),
        }
    }
}

pub struct ConstantBuilder {
    values: Vec<ConstantValue>,
}

impl ConstantBuilder {
    fn new() -> Self {
        Self { values: vec![] }
    }

    fn add_i32(&mut self, v: &i32) -> &mut Self {
        self.values.push(ConstantValue {
            body: Vec::from(v.to_be_bytes()),
        });

        self
    }

    fn add_i64(&mut self, v: &i64) -> &mut Self {
        self.values.push(ConstantValue {
            body: Vec::from(v.to_be_bytes()),
        });

        self
    }

    fn build(self) -> Vec<ConstantValue> {
        self.values
    }
}

pub struct SelectBuilder<'a> {
    runner: &'a mut TestRunner,
    plan: PlanFragment,
}

impl<'a> SelectBuilder<'a> {
    fn new(runner: &'a mut TestRunner) -> Self {
        Self {
            runner,
            plan: PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    node_body: None,
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: 0,
                    distribution: None,
                }),
            },
        }
    }

    // select * from t;
    pub async fn scan_all(mut self) -> SelectBuilder<'a> {
        let table_ref = self
            .runner
            .get_global_env()
            .table_manager_ref()
            .get_table(&TableId::default())
            .unwrap();
        if let Ok(column_table_ref) = downcast_arc::<BummockTable>(table_ref.into_any()) {
            let column_ids = column_table_ref.get_column_ids();
            let scan = SeqScanNode {
                table_ref_id: None,
                column_ids,
                fields: vec![],
            };

            self.plan = PlanFragment {
                root: Some(PlanNode {
                    children: vec![],
                    node_body: Some(NodeBody::SeqScan(scan)),
                }),
                exchange_info: Some(ExchangeInfo {
                    mode: 0,
                    distribution: None,
                }),
            };
            self
        } else {
            todo!()
        }
    }

    pub fn run_task(&mut self) -> &mut Self {
        self.runner.run_task(&self.plan).unwrap();
        self
    }

    pub async fn collect_task_output(&mut self) -> Vec<Vec<GetDataResponse>> {
        self.runner.collect_task_output(&self.plan).await.unwrap()
    }

    pub async fn run_and_collect_multiple_output(mut self) -> Vec<Vec<GetDataResponse>> {
        self.run_task().collect_task_output().await
    }

    pub async fn run_and_collect_single_output(self) -> Vec<GetDataResponse> {
        self.run_and_collect_multiple_output()
            .await
            .drain(0..=0)
            .next()
            .unwrap()
    }

    pub fn get_mut_plan(&mut self) -> &mut PlanFragment {
        &mut self.plan
    }
}

pub struct ResultChecker {
    col_types: Vec<DataType>,
    columns: Vec<Vec<ConstantValue>>,
}

impl ResultChecker {
    pub fn new() -> Self {
        Self {
            col_types: vec![],
            columns: vec![],
        }
    }

    // We still do not support nullable testing very well.
    // TODO: vals &[i32] => vals &[Option<i32>]
    pub fn add_i32_column(&mut self, is_nullable: bool, vals: &[i32]) -> &mut ResultChecker {
        self.col_types.push(DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable,
            ..CoreDefault::default()
        });
        let mut constants = ConstantBuilder::new();
        for v in vals {
            constants.add_i32(v);
        }
        self.columns.push(constants.build());
        self
    }

    pub fn add_i64_column(&mut self, is_nullable: bool, vals: &[i64]) -> &mut ResultChecker {
        self.col_types.push(DataType {
            type_name: TypeName::Int64 as i32,
            is_nullable,
            ..CoreDefault::default()
        });
        let mut constants = ConstantBuilder::new();
        for v in vals {
            constants.add_i64(v);
        }
        self.columns.push(constants.build());
        self
    }

    pub fn check_result(&mut self, actual: &[GetDataResponse]) {
        // Ensure the testing data itself is correct.
        assert_eq!(self.columns.len(), self.col_types.len());
        for col in &self.columns {
            assert_eq!(col.len(), self.cardinality());
        }
        self.try_check_result(actual).unwrap();
    }

    fn cardinality(&self) -> usize {
        self.columns.first().unwrap().len()
    }

    fn try_check_result(&mut self, actual: &[GetDataResponse]) -> Result<()> {
        if self.cardinality() == 0 {
            assert_eq!(actual.len(), 0);
        } else {
            assert_eq!(actual.len(), 1);
            let chunk = actual.get(0).unwrap().get_record_batch()?;
            assert_eq!(chunk.get_cardinality(), self.cardinality() as u32);
            assert_eq!(chunk.get_columns().len(), self.col_types.len());

            for i in 0..chunk.get_columns().len() {
                let col = &chunk.get_columns()[i];

                self.check_column_null_bitmap(col)?;

                // TODO: Write an iterator for FixedWidthColumn
                let value_width = Self::get_value_width(col)?;
                let column_bytes = col.get_array()?.get_values()[0].get_body();
                for j in 0..self.cardinality() {
                    let actual_value = &column_bytes[j * value_width..(j + 1) * value_width];
                    let expected_value = self.columns[i][j].get_body();
                    assert_eq!(expected_value, actual_value);
                }
            }
        }
        Ok(())
    }

    fn get_value_width(col: &Column) -> Result<usize> {
        use risingwave_pb::data::ArrayType;
        Ok(match col.get_array()?.get_array_type()? {
            ArrayType::Int32 => 4,
            ArrayType::Int64 => 8,
            _ => 0,
        })
    }

    // We assume that currently no column is nullable.
    fn check_column_null_bitmap(&self, col: &Column) -> Result<()> {
        let null_bytes = col.get_array()?.get_null_bitmap()?.get_body();
        assert_eq!(null_bytes.len(), self.cardinality());
        for b in null_bytes {
            // 0 for null. 1 for non-null.
            assert_eq!(b.clone(), 1u8);
        }
        Ok(())
    }
}
