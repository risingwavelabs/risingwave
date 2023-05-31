// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::list_array::display_for_explain;
use risingwave_common::types::{literal_type_match, DataType, Datum, ToText};
use risingwave_common::util::value_encoding::{deserialize_datum, serialize_datum};
use risingwave_pb::data::PbDatum;
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
                Some(v) => match self.data_type {
                    DataType::Boolean => write!(f, "{}", v.as_bool()),
                    DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Serial
                    | DataType::Decimal
                    | DataType::Float32
                    | DataType::Float64 => write!(f, "{}", v.as_scalar_ref_impl().to_text()),
                    DataType::Varchar
                    | DataType::Bytea
                    | DataType::Date
                    | DataType::Timestamp
                    | DataType::Timestamptz
                    | DataType::Time
                    | DataType::Interval
                    | DataType::Jsonb
                    | DataType::Int256
                    | DataType::Struct(_) => write!(
                        f,
                        "'{}'",
                        v.as_scalar_ref_impl().to_text_with_type(&self.data_type)
                    ),
                    DataType::List { .. } => write!(f, "{}", display_for_explain(v.as_list())),
                },
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

    pub(super) fn from_expr_proto(
        proto: &risingwave_pb::expr::ExprNode,
    ) -> risingwave_common::error::Result<Self> {
        let data_type = proto.get_return_type()?;
        Ok(Self {
            data: value_encoding_to_literal(&proto.rex_node, &data_type.into())?,
            data_type: data_type.into(),
        })
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
            rex_node: Some(literal_to_value_encoding(self.get_data())),
        }
    }
}

/// Convert a literal value (datum) into protobuf.
pub fn literal_to_value_encoding(d: &Datum) -> RexNode {
    let body = serialize_datum(d.as_ref());
    RexNode::Constant(PbDatum { body })
}

/// Convert protobuf into a literal value (datum).
fn value_encoding_to_literal(
    proto: &Option<RexNode>,
    ty: &DataType,
) -> risingwave_common::error::Result<Datum> {
    if let Some(rex_node) = proto {
        if let RexNode::Constant(prost_datum) = rex_node {
            let datum = deserialize_datum(prost_datum.body.as_ref(), ty)?;
            Ok(datum)
        } else {
            unreachable!()
        }
    } else {
        Ok(None)
    }
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
            Some(ScalarImpl::Utf8("".into())),
            Some(2.into()),
            Some(3.into()),
        ]);
        let data = Some(ScalarImpl::Struct(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node {
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
            Some(ScalarImpl::Utf8("1".into())),
            Some(ScalarImpl::Utf8("2".into())),
            Some(ScalarImpl::Utf8("".into())),
        ]);
        let data = Some(ScalarImpl::List(value.clone()));
        let node = literal_to_value_encoding(&data);
        if let RexNode::Constant(prost) = node {
            let data2 = deserialize_datum(
                prost.get_body().as_slice(),
                &DataType::List(Box::new(DataType::Varchar)),
            )
            .unwrap()
            .unwrap();
            assert_eq!(ScalarImpl::List(value), data2);
        }
    }
}
