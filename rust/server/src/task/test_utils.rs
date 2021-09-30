use super::*;

use crate::catalog::TableId;
use crate::error::{ErrorCode, Result};
use crate::service::ExchangeWriter;
use crate::storage::TableRef;
use pb_convert::FromProtobuf;
use protobuf::well_known_types::Any;
use protobuf::Message;
use risingwave_proto::data::{Column, DataType, DataType_TypeName};
use risingwave_proto::expr::{ConstantValue, ExprNode, ExprNode_Type};
use risingwave_proto::plan::{
    ColumnDesc, CreateTableNode, InsertValueNode, InsertValueNode_ExprTuple, PlanFragment,
    PlanNode_PlanNodeType as PlanNodeType, SeqScanNode, TableRefId,
};
use risingwave_proto::task_service::{TaskData, TaskId as ProtoTaskId, TaskSinkId};

// Write the execution results into a buffer for testing.
// In a real server, the results will be flushed into a grpc sink.
struct FakeExchangeWriter {
    messages: Vec<TaskData>,
}

#[async_trait::async_trait]
impl ExchangeWriter for FakeExchangeWriter {
    async fn write(&mut self, data: TaskData) -> Result<()> {
        self.messages.push(data);
        Ok(())
    }
}

pub struct TestRunner {
    tsid: TaskSinkId,
    env: GlobalTaskEnv,
}

impl TestRunner {
    pub fn new() -> Self {
        let tid = ProtoTaskId::default();
        let mut tsid = TaskSinkId::default();
        tsid.set_task_id(tid);
        Self {
            env: GlobalTaskEnv::for_test(),
            tsid,
        }
    }

    pub fn prepare_table(&mut self) -> TableBuilder {
        TableBuilder::new(self)
    }

    pub fn prepare_scan(&mut self) -> SelectBuilder {
        SelectBuilder::new(self)
    }

    pub fn run(&mut self, plan: PlanFragment) -> Result<Vec<TaskData>> {
        let task_manager = self.env.task_manager();
        task_manager.fire_task(self.env.clone(), self.tsid.get_task_id(), plan)?;
        let mut task_sink = task_manager.take_sink(&self.tsid).unwrap();
        let mut writer = FakeExchangeWriter { messages: vec![] };
        let messages = futures::executor::block_on(async move {
            task_sink.take_data(&mut writer).await.unwrap();
            writer.messages
        });
        task_manager.remove_task(self.tsid.get_task_id()).unwrap();
        Ok(messages)
    }

    fn get_global_env(&self) -> GlobalTaskEnv {
        self.env.clone()
    }

    fn validate_insert_result(result: Vec<TaskData>, inserted_rows: usize) {
        ResultChecker::new()
            .add_i32_column(false, &[inserted_rows as i32])
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

    pub fn create_table(mut self, col_types: &[DataType_TypeName]) -> Self {
        for type_name in col_types {
            let mut typ = DataType::new();
            typ.set_type_name(*type_name);
            typ.set_is_nullable(false);
            self.col_types.push(typ);
        }
        self
    }

    pub fn create_table_int32s(self, col_num: usize) -> Self {
        let mut col_types = vec![];
        for _ in 0..col_num {
            col_types.push(DataType_TypeName::INT32);
        }
        self.create_table(col_types.as_slice())
    }

    pub fn set_nullable(&mut self, col_idx: usize) -> &mut Self {
        self.col_types
            .get_mut(col_idx)
            .unwrap()
            .set_is_nullable(true);
        self
    }

    pub fn insert_i32s(mut self, i32s: &[i32]) -> Self {
        assert_eq!(i32s.len(), self.col_types.len());
        let mut tuple = ConstantBuilder::new();
        for v in i32s {
            tuple.add_i32(v);
        }
        self.tuples.push(tuple.build());
        self
    }

    pub fn run(self) {
        let inserted_rows = self.tuples.len();
        let create = self.build_create_table_plan();
        let insert = self.build_insert_values_plan();
        assert_eq!(self.runner.run(create).unwrap().len(), 0);
        TestRunner::validate_insert_result(self.runner.run(insert).unwrap(), inserted_rows);
    }

    fn build_create_table_plan(&self) -> PlanFragment {
        let mut plan = PlanFragment::default();

        let mut create = CreateTableNode::default();
        for typ in self.col_types.iter() {
            let mut col = ColumnDesc::default();
            col.set_column_type(typ.clone());
            create.mut_column_descs().push(col);
        }

        plan.mut_root().set_body(Any::pack(&create).unwrap());
        plan.mut_root().set_node_type(PlanNodeType::CREATE_TABLE);
        plan
    }

    fn build_insert_values_plan(&self) -> PlanFragment {
        let mut plan = PlanFragment::default();
        let mut insert = InsertValueNode::default();
        let col_num = self.col_types.len();
        for _ in 0..col_num {
            insert.mut_column_ids().push(0);
        }
        for tuple in self.tuples.iter() {
            insert
                .insert_tuples
                .push(TableBuilder::build_insert(tuple.clone()));
        }
        plan.mut_root().set_body(Any::pack(&insert).unwrap());
        plan.mut_root().set_node_type(PlanNodeType::INSERT_VALUE);
        plan
    }

    fn build_insert(constants: Vec<ConstantValue>) -> InsertValueNode_ExprTuple {
        let mut tuple = InsertValueNode_ExprTuple::default();
        for constant in constants {
            let mut node = ExprNode::default();

            node.set_expr_type(ExprNode_Type::CONSTANT_VALUE);
            let mut typ = DataType::new();
            typ.set_type_name(DataType_TypeName::INT32);
            node.set_return_type(typ);
            node.set_body(Any::pack(&constant).unwrap());

            tuple.mut_cells().push(node);
        }
        tuple
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
        let mut constant = ConstantValue::default();
        constant.set_body(Vec::from(v.to_be_bytes()));
        self.values.push(constant);
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
            plan: PlanFragment::default(),
        }
    }

    // select * from t;
    pub fn scan_all(mut self) -> Self {
        let mut scan = SeqScanNode::default();
        let table_ref = self
            .runner
            .get_global_env()
            .storage_manager_ref()
            .get_table(&TableId::from_protobuf(&TableRefId::default()).unwrap())
            .unwrap();
        if let TableRef::Columnar(column_table_ref) = table_ref {
            let column_ids = column_table_ref.get_column_ids().unwrap();
            for col_id in column_ids.iter() {
                scan.mut_column_ids().push(*col_id);
            }
            self.plan.mut_root().set_body(Any::pack(&scan).unwrap());
            self.plan.mut_root().set_node_type(PlanNodeType::SEQ_SCAN);
            self
        } else {
            todo!()
        }
    }

    pub fn run(self) -> Vec<TaskData> {
        self.runner.run(self.plan).unwrap()
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
    pub fn add_i32_column(mut self, is_nullable: bool, vals: &[i32]) -> ResultChecker {
        let mut typ = DataType::default();
        typ.set_type_name(DataType_TypeName::INT32);
        typ.set_is_nullable(is_nullable);
        self.col_types.push(typ);
        let mut constants = ConstantBuilder::new();
        for v in vals {
            constants.add_i32(v);
        }
        self.columns.push(constants.build());
        self
    }

    pub fn check_result(self, actual: Vec<TaskData>) {
        // Ensure the testing data itself is correct.
        assert_eq!(self.columns.len(), self.col_types.len());
        for col in self.columns.iter() {
            assert_eq!(col.len(), self.cardinality());
        }
        self.try_check_result(actual).unwrap();
    }

    fn cardinality(&self) -> usize {
        self.columns.first().unwrap().len()
    }

    fn try_check_result(self, actual: Vec<TaskData>) -> Result<()> {
        assert_eq!(actual.len(), 1);
        let chunk = actual.get(0).unwrap().get_record_batch();
        assert_eq!(chunk.get_cardinality(), self.cardinality() as u32);
        assert_eq!(chunk.get_columns().len(), self.col_types.len());

        for i in 0..chunk.get_columns().len() {
            let col = unpack_from_any!(chunk.get_columns()[i], Column);
            self.check_column_meta(i, &col);
            self.check_column_null_bitmap(&col);

            // TODO: Write an iterator for FixedWidthColumn
            // let value_width = col.get_value_width() as usize;
            let value_width = Self::get_value_width(&col);
            assert_eq!(value_width, 4); // Temporarily hard-coded.
            let column_bytes = col.get_values()[0].get_body();
            for j in 0..self.cardinality() {
                let actual_value = &column_bytes[j * value_width..(j + 1) * value_width];
                let expected_value = self.columns[i][j].get_body();
                assert_eq!(expected_value, actual_value);
            }
        }

        Ok(())
    }

    fn get_value_width(col: &Column) -> usize {
        match col.get_column_type().get_type_name() {
            DataType_TypeName::INT32 => 4,
            _ => 0,
        }
    }
    fn check_column_meta(&self, col_idx: usize, column: &Column) {
        assert_eq!(
            column.get_column_type().get_type_name(),
            self.col_types[col_idx].get_type_name()
        );
        assert_eq!(
            column.get_column_type().get_is_nullable(),
            self.col_types[col_idx].get_is_nullable()
        );
    }

    // We assume that currently no column is nullable.
    fn check_column_null_bitmap(&self, col: &Column) {
        let null_bytes = col.get_null_bitmap().get_body();
        assert_eq!(null_bytes.len(), self.cardinality());
        for b in null_bytes {
            // 0 for null. 1 for non-null.
            assert_eq!(b.clone(), 1u8);
        }
    }
}
