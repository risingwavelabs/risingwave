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

use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_pb::catalog::{
    Function as ProstFunction, FunctionArgument as ProstFunctionArgument,
};

#[derive(Clone, Debug)]
pub struct FunctionCatalog {
    pub id: FunctionId,
    pub name: String,
    pub owner: u32,
    pub arguments: Vec<FunctionArgument>,
    pub return_type: DataType,
    pub language: String,
    pub path: String,
}

#[derive(Clone, Debug)]
pub struct FunctionArgument {
    pub name: Option<String>,
    pub data_type: DataType,
}

impl From<&ProstFunction> for FunctionCatalog {
    fn from(prost: &ProstFunction) -> Self {
        FunctionCatalog {
            id: prost.id.into(),
            name: prost.name.clone(),
            owner: prost.owner,
            arguments: prost.arguments.iter().map(|arg| arg.into()).collect(),
            return_type: prost.return_type.as_ref().expect("no return type").into(),
            language: prost.language.clone(),
            path: prost.path.clone(),
        }
    }
}

impl From<&ProstFunctionArgument> for FunctionArgument {
    fn from(prost: &ProstFunctionArgument) -> Self {
        FunctionArgument {
            name: if prost.name.is_empty() {
                None
            } else {
                Some(prost.name.clone())
            },
            data_type: prost.r#type.as_ref().expect("no return type").into(),
        }
    }
}

impl FunctionCatalog {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SQL statement that can be used to create this view.
    pub fn create_sql(&self) -> String {
        todo!();
        format!("CREATE FUNCTION {}", self.name)
    }
}
