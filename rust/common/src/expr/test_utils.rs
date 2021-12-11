use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as DataTypeProst;
use risingwave_pb::expr::expr_node::Type::InputRef;
use risingwave_pb::expr::expr_node::{RexNode, Type as ProstExprType};
use risingwave_pb::expr::{ExprNode, FunctionCall, InputRefExpr};

use super::*;

pub fn make_expression(kind: ProstExprType, rets: &[TypeName], indices: &[i32]) -> ProstExprNode {
    let mut exprs = Vec::new();
    for (idx, ret) in indices.iter().zip(rets.iter()) {
        exprs.push(make_input_ref(*idx, *ret));
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
        return_type: Some(return_type),
        rex_node: Some(RexNode::FuncCall(function_call)),
    }
}

pub fn make_input_ref(idx: i32, ret: TypeName) -> ExprNode {
    ExprNode {
        expr_type: InputRef as i32,
        return_type: Some(DataTypeProst {
            type_name: ret as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::InputRef {
            0: InputRefExpr { column_idx: idx },
        }),
    }
}
