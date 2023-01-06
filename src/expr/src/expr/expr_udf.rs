// Copyright 2023 Singularity Data
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

use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::array::{ArrayBuilder, ArrayBuilderImpl, ArrayRef, DataChunk};
use risingwave_common::for_all_variants;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{literal_type_match, DataType, Datum, Scalar, ScalarImpl};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use super::{build_from_prost, BoxedExpression};
use crate::expr::Expression;
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct UdfExpression {
    children: Vec<BoxedExpression>,
    name: String,
    arg_types: Vec<DataType>,
    return_type: DataType,
    // TODO: arrow flight client
}

impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        todo!("evaluate UDF")
    }

    fn eval_row(&self, _input: &OwnedRow) -> Result<Datum> {
        todo!("evaluate UDF")
    }
}

impl UdfExpression {}

impl<'a> TryFrom<&'a ExprNode> for UdfExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Udf);
        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::Udf(udf) = prost.get_rex_node().unwrap() else {
            bail!("expect UDF");
        };
        Ok(Self {
            children: udf.children.iter().map(build_from_prost).try_collect()?,
            name: udf.name.clone(),
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type: ret_type,
        })
    }
}
