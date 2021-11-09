use super::*;
use pb_construct::make_proto;
use protobuf::well_known_types::Any as AnyProto;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type as ProstExprType;
use risingwave_pb::expr::FunctionCall;
use risingwave_proto::data::DataType as DataTypeProto;
use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::expr::ExprNode;
use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
use risingwave_proto::expr::InputRefExpr;

pub fn make_expression(
    kind: ProstExprType,
    rets: &[DataType_TypeName],
    indices: &[i32],
) -> ProstExprNode {
    let mut exprs = Vec::new();
    for (idx, ret) in indices.iter().zip(rets.iter()) {
        exprs.push(make_inputref(*idx, *ret).to_prost::<ProstExprNode>());
    }
    let function_call = FunctionCall { children: exprs };
    let return_type = risingwave_pb::data::DataType {
        type_name: risingwave_pb::data::data_type::TypeName::Timestamp as i32,
        precision: 0,
        scale: 0,
        is_nullable: false,
        interval_type: 0,
    };
    ProstExprNode {
        expr_type: kind as i32,
        body: None,
        return_type: Some(return_type),
        rex_node: Some(RexNode::FuncCall(function_call)),
    }
}

pub fn make_inputref(idx: i32, ret: risingwave_proto::data::DataType_TypeName) -> ExprNode {
    make_proto!(ExprNode, {
      expr_type: INPUT_REF,
      body: AnyProto::pack(
        &make_proto!(InputRefExpr, {column_idx: idx})
      ).unwrap(),
      return_type: make_proto!(DataTypeProto, {
        type_name: ret
      })
    })
}
