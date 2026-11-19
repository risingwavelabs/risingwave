// Copyright 2022 RisingWave Labs
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

use core::fmt;

use risingwave_common::types::DataType;

use super::{Expr, ExprImpl, InputRef};
use crate::expr::ExprRewriter;

pub type CorrelatedId = u32;

/// Identifies a binder scope: a [`BindContext`](crate::binder::BindContext) whose columns can be
/// referenced from a nested scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct ScopeId(pub(crate) u32);

/// `Scope` is the binder scope that owns the referenced column.
/// `Absolute` is the id of the Apply operator providing that scope's columns, and is non-zero.
#[derive(Clone, Eq, PartialEq, Hash)]
pub enum Position {
    Scope(ScopeId),
    Absolute(CorrelatedId),
}

/// A reference to a column outside the subquery.
///
/// `index` is the index in the referred relation.
/// `position` has two modes: the binder records the owning scope, and the planner replaces it
/// with the id of the Apply operator that binds that scope.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct CorrelatedInputRef {
    index: usize,
    data_type: DataType,
    position: Position,
}

impl CorrelatedInputRef {
    pub fn new(index: usize, data_type: DataType, scope: ScopeId) -> Self {
        CorrelatedInputRef {
            index,
            data_type,
            position: Position::Scope(scope),
        }
    }

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The scope owning the referenced column, if no Apply operator has been assigned yet.
    pub fn scope(&self) -> Option<ScopeId> {
        match self.position {
            Position::Scope(scope) => Some(scope),
            Position::Absolute(_) => None,
        }
    }

    pub fn set_correlated_id(&mut self, correlated_id: CorrelatedId) {
        self.position = Position::Absolute(correlated_id);
    }

    pub fn correlated_id(&self) -> CorrelatedId {
        match self.position {
            Position::Scope(_) => 0,
            Position::Absolute(correlated_id) => correlated_id,
        }
    }
}

impl Expr for CorrelatedInputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn try_to_expr_proto(&self) -> Result<risingwave_pb::expr::ExprNode, String> {
        Err(format!(
            "CorrelatedInputRef {:?} has not been decorrelated",
            self
        ))
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

/// Rewrite `InputRef`s to columns of `scope` into `CorrelatedInputRef`s owned by it.
pub struct InputRefScopeRewriter {
    scope: ScopeId,
}

impl InputRefScopeRewriter {
    pub fn new(scope: ScopeId) -> Self {
        Self { scope }
    }
}

impl ExprRewriter for InputRefScopeRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        CorrelatedInputRef::new(input_ref.index(), input_ref.return_type(), self.scope).into()
    }
}
