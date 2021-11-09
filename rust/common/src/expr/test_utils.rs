use super::*;
use prost::Message;
use prost_types::Any;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as DataTypeProst;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type as ProstExprType;
use risingwave_pb::expr::expr_node::Type::InputRef;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::FunctionCall;
use risingwave_pb::expr::InputRefExpr;

pub fn make_expression(kind: ProstExprType, rets: &[TypeName], indices: &[i32]) -> ProstExprNode {
    let mut exprs = Vec::new();
    for (idx, ret) in indices.iter().zip(rets.iter()) {
        exprs.push(make_inputref(*idx, *ret));
    }
    let function_call = FunctionCall { children: exprs };
    let return_type = DataTypeProst {
        type_name: TypeName::Timestamp as i32,
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

pub fn make_inputref(idx: i32, ret: TypeName) -> ExprNode {
    ExprNode {
        expr_type: InputRef as i32,
        body: Some(Any {
            type_url: "/".to_string(),
            value: InputRefExpr { column_idx: idx }.encode_to_vec(),
        }),
        return_type: Some(DataType {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: None,
    }
}
