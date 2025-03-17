// Copyright 2025 RisingWave Labs
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

use std::hash::Hash;

use educe::Educe;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema, TableVersionId};
use risingwave_common::types::DataType;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Delete<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub returning: bool,
}

impl<PlanRef: GenericPlanRef> Delete<PlanRef> {
    pub fn output_len(&self) -> usize {
        if self.returning {
            self.input.schema().len()
        } else {
            1
        }
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Delete<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.output_len())
    }

    fn schema(&self) -> Schema {
        if self.returning {
            self.input.schema().clone()
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        if self.returning {
            Some(self.input.stream_key()?.to_vec())
        } else {
            Some(vec![])
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: Eq + Hash> Delete<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            returning,
        }
    }
}

impl<PlanRef: Eq + Hash> DistillUnit for Delete<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let mut vec = Vec::with_capacity(if self.returning { 2 } else { 1 });
        vec.push(("table", Pretty::from(self.table_name.clone())));
        if self.returning {
            vec.push(("returning", Pretty::display(&true)));
        }
        childless_record(name, vec)
    }
}
