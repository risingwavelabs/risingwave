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

use risingwave_sqlparser::ast::{DropFunctionDesc, ReferentialAction};

use super::*;

pub async fn handle_drop_function(
    _handler_args: HandlerArgs,
    _if_exists: bool,
    _func_desc: Vec<DropFunctionDesc>,
    _option: Option<ReferentialAction>,
) -> Result<RwPgResponse> {
    Err(ErrorCode::NotImplemented("drop function".to_string(), None.into()).into())
}
