// use super::BoxedExecutor;
// use crate::array::{BoolArray, DataChunk};
// use crate::buffer::Bitmap;
// use crate::error::ErrorCode::{InternalError, ProtobufError};
// use crate::error::{Result, RwError};
// use crate::executor::ExecutorResult::{Batch, Done};
// use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
// use crate::expr::{build_from_proto, BoxedExpression};
// use crate::util::downcast_ref;
// use protobuf::Message;
// use risingwave_proto::expr::ExprNode;
// use risingwave_proto::plan::PlanNode_PlanNodeType;
// use std::convert::TryFrom;
// use std::sync::Arc;
// pub(super) struct FilterExecutor {
//   expr: BoxedExpression,
//   child: BoxedExecutor,
// }
//
// fn filter(sel: &BoolArray, data_chunk: &DataChunk) -> Result<DataChunk> {
//   let mut new_vis = Bitmap::from_bool_array(sel)?;
//   if let Some(vis) = data_chunk.visibility() {
//     new_vis = ((&new_vis) & (vis))?;
//   }
//   Ok(data_chunk.with_visibility(new_vis))
// }
//
// impl Executor for FilterExecutor {
//   fn init(&mut self) -> Result<()> {
//     self.child.init()?;
//     Ok(())
//   }
//
//   fn execute(&mut self) -> Result<ExecutorResult> {
//     let res = self.child.execute()?;
//     if let Batch(data_chunk) = res {
//       let sel = self.expr.eval(data_chunk.as_ref())?;
//       let sel: &BoolArray = downcast_ref(sel.as_ref())?;
//       let data_chunk = filter(sel, data_chunk.as_ref())?;
//       return Ok(Batch(Arc::new(data_chunk)));
//     }
//     Ok(Done)
//   }
//
//   fn clean(&mut self) -> Result<()> {
//     self.child.clean()?;
//     Ok(())
//   }
// }
//
// impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for FilterExecutor {
//   type Error = RwError;
//   fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
//     ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::FILTER);
//     ensure!(source.plan_node().get_children().len() == 1);
//     let expr_node = ExprNode::parse_from_bytes(source.plan_node().get_body().get_value())
//       .map_err(ProtobufError)?;
//     let expr = build_from_proto(&expr_node)?;
//     if let Some(child_plan) = source.plan_node.get_children().get(0) {
//       let child = ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
//       return Ok(Self { expr, child });
//     }
//     Err(InternalError("Filter must have one children".to_string()).into())
//   }
// }
//
// #[cfg(test)]
// mod tests {
//   use super::*;
//   use crate::array::{BoolArray, DataChunk};
//   use crate::buffer::Bitmap;
//   use crate::util::downcast_ref;
//   // some expression has not yet been implemented
//   // use protobuf::RepeatedField;
//   // use risingwave_proto::expr::ExprNode_ExprNodeType;
//   // use risingwave_proto::plan::{PlanNode};
//   // use crate::task::GlobalTaskEnv;
//   // #[test]
//   // fn test_filter_executor() {
//   //   let mut plan_node = PlanNode::new();
//   //   plan_node.set_node_type(PlanNode_PlanNodeType::FILTER);
//   //   let childeren = RepeatedField::from_vec(vec![PlanNode::new()]);
//   //   plan_node.set_children(childeren);
//   //   let env = GlobalTaskEnv::for_test();
//   //   let _exec = ExecutorBuilder::new(&plan_node, env ).build().unwrap();
//   // }
//   #[test]
//   fn test_filter() {
//     let input1 = vec![Some(true), Some(false), None, Some(false), None];
//     let input2 = [true, true, false, false];
//
//     let bool_array = BoolArray::from_values(&input1).expect("Failed to build bool array from vec");
//     let bool_array: &BoolArray = downcast_ref(&*bool_array).expect("Not bool array");
//     let data_chunk = DataChunk::default();
//     let data_chunk = data_chunk.with_visibility(Bitmap::from_vec(input2.to_vec()).unwrap());
//     let data_chunk = filter(&bool_array, &data_chunk).unwrap();
//     if let Some(bm) = data_chunk.visibility() {
//       assert!(bm.is_set(0).unwrap());
//       assert!(!bm.is_set(1).unwrap());
//       assert!(!bm.is_set(2).unwrap());
//       assert!(!bm.is_set(3).unwrap());
//     } else {
//       assert!(false);
//     }
//   }
// }
