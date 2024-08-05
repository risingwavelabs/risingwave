// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::iter;

use risingwave_common::array::Op;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use crate::executor::prelude::*;

pub struct MergeProjectExecutor {
    _ctx: ActorContextRef,
    pub input: Executor,
    /// Maps input from the lhs to the output.
    pub lhs_mapping: ColIndexMapping,
    /// Maps input from the rhs to the output.
    pub rhs_mapping: ColIndexMapping,
}
