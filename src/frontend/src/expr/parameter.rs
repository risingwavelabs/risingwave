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

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

use risingwave_common::types::DataType;

use super::Expr;
use crate::binder::ParameterTypes;

#[derive(Clone)]
pub struct Parameter {
    pub index: u64,
    param_types: ParameterTypes,
}

impl Debug for Parameter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Parameter(index: {}, type: {:?})",
            self.index,
            self.param_types.read_type(self.index)
        )
    }
}

impl PartialEq for Parameter {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for Parameter {}

impl Hash for Parameter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

impl Expr for Parameter {
    fn return_type(&self) -> DataType {
        self.param_types
            .read_type(self.index)
            .unwrap_or(DataType::Varchar)
    }

    fn to_expr_proto(&self) -> risingwave_pb::expr::ExprNode {
        unreachable!("Parameter should not be serialized to ExprNode")
    }
}

impl Parameter {
    pub fn new(index: u64, mut param_types: ParameterTypes) -> Self {
        param_types.record_new_param(index);
        Self { index, param_types }
    }

    pub fn has_infer(&self) -> bool {
        self.param_types.has_infer(self.index)
    }

    pub fn cast_infer_type(&mut self, data_type: DataType) {
        self.param_types.record_infer_type(self.index, data_type);
    }
}
