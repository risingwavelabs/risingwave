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

use core::fmt;

use risingwave_common::types::DataType;

use super::Expr;

pub type Depth = usize;
pub type CorrelatedId = u32;

/// Relative `Depth` is the number of of nesting levels of the subquery relative to the referred
/// relation, and should be non-zero.
/// Absolute `CorrelatedId` is the id of the related Apply operator, and should be non-zero.
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum Position {
    Relative(Depth),
    Absolute(CorrelatedId),
}

/// A reference to a column outside the subquery.
///
/// `index` is the index in the referred relation.
/// `position` has two mode Relative and Absolute.
/// For binding we use relative position, for optimization we use absolute position.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct CorrelatedInputRef {
    index: usize,
    data_type: DataType,
    position: Position,
}

impl CorrelatedInputRef {
    pub fn new(index: usize, data_type: DataType, depth: usize) -> Self {
        CorrelatedInputRef {
            index,
            data_type,
            position: Position::Relative(depth),
        }
    }

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn depth(&self) -> usize {
        match self.position {
            Position::Relative(depth) => depth,
            Position::Absolute(_) => 0,
        }
    }

    pub fn set_correlated_id(&mut self, correlated_id: CorrelatedId) {
        self.position = Position::Absolute(correlated_id);
    }

    pub fn correlated_id(&self) -> CorrelatedId {
        match self.position {
            Position::Relative(_) => 0,
            Position::Absolute(correlated_id) => correlated_id,
        }
    }
}

impl Expr for CorrelatedInputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("CorrelatedInputRef {:?} has not been decorrelated", self)
    }
}

impl fmt::Debug for CorrelatedInputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrelatedInputRef")
            .field("index", &self.index)
            .field("correlated_id", &self.correlated_id())
            .finish()
    }
}
