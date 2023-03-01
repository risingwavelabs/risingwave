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
use risingwave_pb::catalog::function::FunctionType as ProstFunctionType;
use risingwave_pb::catalog::Function as ProstFunction;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FunctionCatalog {
    pub id: FunctionId,
    pub name: String,
    pub owner: u32,
    pub type_: FunctionType,
    pub arg_types: Vec<DataType>,
    pub return_types: Vec<DataType>,
    pub language: String,
    pub identifier: String,
    pub link: String,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum FunctionType {
    Scalar,
    Table,
    Aggregate,
}

impl From<ProstFunctionType> for FunctionType {
    fn from(prost: ProstFunctionType) -> Self {
        match prost {
            ProstFunctionType::Scalar => FunctionType::Scalar,
            ProstFunctionType::Table => FunctionType::Table,
            ProstFunctionType::Aggregate => FunctionType::Aggregate,
        }
    }
}

impl From<&ProstFunction> for FunctionCatalog {
    fn from(prost: &ProstFunction) -> Self {
        FunctionCatalog {
            id: prost.id.into(),
            name: prost.name.clone(),
            owner: prost.owner,
            type_: ProstFunctionType::from_i32(prost.r#type).unwrap().into(),
            arg_types: prost.arg_types.iter().map(|arg| arg.into()).collect(),
            return_types: prost.return_types.iter().map(|arg| arg.into()).collect(),
            language: prost.language.clone(),
            identifier: prost.identifier.clone(),
            link: prost.link.clone(),
        }
    }
}
