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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_pb::expr::agg_call::Arg as ProstAggCallArg;
use risingwave_pb::expr::InputRefExpr;

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct InputRef {
    pub index: usize,
    pub data_type: DataType,
}

#[derive(Clone, Copy)]
pub struct RawInputRefDisplay(pub usize);

pub fn input_ref_to_column_indices(input_refs: &[InputRef]) -> Vec<usize> {
    input_refs.iter().map(|x| x.index()).collect_vec()
}

impl fmt::Display for RawInputRefDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self as &dyn fmt::Debug).fmt(f)
    }
}

impl fmt::Debug for RawInputRefDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.0)
    }
}

#[derive(Clone, Copy)]
pub struct InputRefDisplay<'a> {
    pub input_ref: &'a InputRef,
    pub input_schema: &'a Schema,
}

impl fmt::Display for InputRefDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self as &dyn fmt::Debug).fmt(f)
    }
}

impl fmt::Debug for InputRefDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.input_schema
                .fields
                .get(self.input_ref.index)
                .unwrap()
                .name
        )
    }
}

impl fmt::Display for InputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", RawInputRefDisplay(self.index))
    }
}

impl fmt::Debug for InputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            f.debug_struct("InputRef")
                .field("index", &self.index)
                .field("data_type", &self.data_type)
                .finish()
        } else {
            write!(f, "{}", RawInputRefDisplay(self.index))
        }
    }
}

impl InputRef {
    pub fn new(index: usize, data_type: DataType) -> Self {
        InputRef { index, data_type }
    }

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Shift the input ref's index with offset.
    pub fn shift_with_offset(&mut self, offset: isize) {
        self.index = (self.index as isize + offset) as usize;
    }

    /// Convert to [`InputRefExpr`].
    pub fn to_proto(&self) -> InputRefExpr {
        InputRefExpr {
            column_idx: self.index as i32,
        }
    }

    /// Convert [`InputRef`] to an arg of agg call.
    pub fn to_agg_arg_proto(&self) -> ProstAggCallArg {
        ProstAggCallArg {
            input: Some(self.to_proto()),
            r#type: Some(self.data_type.to_protobuf()),
        }
    }
}

impl Expr for InputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: ExprType::InputRef.into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::InputRef(self.to_proto())),
        }
    }
}
