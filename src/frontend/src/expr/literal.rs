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

use risingwave_common::types::{literal_type_match, DataType, Datum, ScalarImpl};
use risingwave_common::util::value_encoding::serialize_datum_to_bytes;
use risingwave_pb::expr::expr_node::RexNode;

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, Eq, PartialEq, Hash)]
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
            match &self.data {
                None => write!(f, "null"),
                // Add single quotation marks for string and interval literals
                Some(ScalarImpl::Utf8(v)) => write!(f, "'{}'", v),
                Some(ScalarImpl::Interval(v)) => write!(f, "'{}'", v),
                Some(v) => write!(f, "{}", v),
            }?;
            write!(f, ":{:?}", self.data_type)
        }
    }
}

impl Literal {
    pub fn new(data: Datum, data_type: DataType) -> Self {
        assert!(literal_type_match(&data_type, data.as_ref()));
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

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: self.get_expr_type() as i32,
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: literal_to_value_encoding(self.get_data()),
        }
    }
}

/// Convert a literal value (datum) into protobuf.
fn literal_to_value_encoding(d: &Datum) -> Option<RexNode> {
    if d.is_none() {
        return None;
    }
    use risingwave_pb::expr::*;
    let body = serialize_datum_to_bytes(d.as_ref());
    Some(RexNode::Constant(ConstantValue { body }))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{ListValue, StructValue};
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::value_encoding::deserialize_datum;
    use risingwave_pb::expr::expr_node::RexNode;

    use crate::expr::literal::literal_to_value_encoding;

    #[test]
    fn test_struct_to_value_encoding() {
        let value = StructValue::new(vec![
            Some(ScalarImpl::Utf8("".to_string())),
            Some(2.into()),
            Some(3.into()),
        ]);
        let data = Some(ScalarImpl::Struct(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node.as_ref().unwrap() {
            let data2 = deserialize_datum(
                prost.get_body().as_slice(),
                &DataType::new_struct(
                    vec![DataType::Varchar, DataType::Int32, DataType::Int32],
                    vec![],
                ),
            )
            .unwrap()
            .unwrap();
            assert_eq!(ScalarImpl::Struct(value), data2);
        }
    }

    #[test]
    fn test_list_to_value_encoding() {
        let value = ListValue::new(vec![
            Some(ScalarImpl::Utf8("1".to_owned())),
            Some(ScalarImpl::Utf8("2".to_owned())),
            Some(ScalarImpl::Utf8("".to_owned())),
        ]);
        let data = Some(ScalarImpl::List(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node.as_ref().unwrap() {
            let data2 = deserialize_datum(
                prost.get_body().as_slice(),
                &DataType::List {
                    datatype: Box::new(DataType::Varchar),
                },
            )
            .unwrap()
            .unwrap();
            assert_eq!(ScalarImpl::List(value), data2);
        }
    }
}
