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

use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::function::PbKind;
use risingwave_pb::catalog::PbFunction;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FunctionCatalog {
    pub id: FunctionId,
    pub name: String,
    pub owner: u32,
    pub kind: FunctionKind,
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
    pub language: String,
    pub identifier: String,
    pub link: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FunctionKind {
    Scalar,
    Table,
    Aggregate,
}

impl From<&PbKind> for FunctionKind {
    fn from(prost: &PbKind) -> Self {
        use risingwave_pb::catalog::function::*;
        match prost {
            Kind::Scalar(ScalarFunction {}) => Self::Scalar,
            Kind::Table(TableFunction {}) => Self::Table,
            Kind::Aggregate(AggregateFunction {}) => Self::Aggregate,
        }
    }
}

impl From<&PbFunction> for FunctionCatalog {
    fn from(prost: &PbFunction) -> Self {
        FunctionCatalog {
            id: prost.id.into(),
            name: prost.name.clone(),
            owner: prost.owner,
            kind: prost.kind.as_ref().unwrap().into(),
            arg_types: prost.arg_types.iter().map(|arg| arg.into()).collect(),
            return_type: prost.return_type.as_ref().expect("no return type").into(),
            language: prost.language.clone(),
            identifier: prost.identifier.clone(),
            link: prost.link.clone(),
        }
    }
}
