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

use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_pb::expr::expr_node::RexNode;

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, PartialEq)]
pub struct Literal {
    data: Datum,
    data_type: DataType,
}

impl std::fmt::Debug for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("Literal")
                .field("data", &self.data)
                .field("data_type", &self.data_type)
                .finish()
        } else {
            use risingwave_common::for_all_scalar_variants;
            use risingwave_common::types::ScalarImpl::*;
            macro_rules! scalar_write_inner {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match &self.data {
                    None => write!(f, "null"),
                    $( Some($variant_name(v)) => write!(f, "{:?}", v) ),*
                }?;
            };
        }
            for_all_scalar_variants! { scalar_write_inner }
            write!(f, ":{:?}", self.data_type)
        }
    }
}

impl Literal {
    pub fn new(data: Datum, data_type: DataType) -> Self {
        Literal { data, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::ConstantValue
    }
    pub fn get_data(&self) -> &Datum {
        &self.data
    }
}
impl Expr for Literal {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn to_protobuf(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: self.get_expr_type() as i32,
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: literal_to_protobuf(self.get_data()),
        }
    }
}

/// Convert a literal value (datum) into protobuf.
fn literal_to_protobuf(d: &Datum) -> Option<RexNode> {
    let Some(d) = d.as_ref() else {
        return None;
    };

    use risingwave_pb::expr::*;

    let body = match d {
        ScalarImpl::Int16(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Int32(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Int64(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Float32(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Float64(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Utf8(s) => s.as_bytes().to_vec(),
        ScalarImpl::Bool(v) => (*v as i8).to_be_bytes().to_vec(),
        ScalarImpl::Decimal(v) => v.to_string().as_bytes().to_vec(),
        ScalarImpl::Interval(v) => v.to_protobuf_owned(),
        ScalarImpl::NaiveDate(_) => todo!(),
        ScalarImpl::NaiveDateTime(_) => todo!(),
        ScalarImpl::NaiveTime(_) => todo!(),
        ScalarImpl::Struct(_) => todo!(),
        ScalarImpl::List(_) => todo!(),
    };
    Some(RexNode::Constant(ConstantValue { body }))
}
