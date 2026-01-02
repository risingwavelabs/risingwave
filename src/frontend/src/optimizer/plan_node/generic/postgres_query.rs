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

use educe::Educe;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};

use super::GenericPlanNode;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct PostgresQuery {
    pub schema: Schema,
    pub hostname: String,
    pub port: String,
    pub username: String,
    pub password: String,
    pub database: String,
    pub query: String,

    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for PostgresQuery {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.schema.len())
    }
}

impl PostgresQuery {
    pub fn columns(&self) -> Vec<ColumnDesc> {
        self.schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                ColumnDesc::named(f.name.clone(), ColumnId::new(i as i32), f.data_type.clone())
            })
            .collect()
    }
}
