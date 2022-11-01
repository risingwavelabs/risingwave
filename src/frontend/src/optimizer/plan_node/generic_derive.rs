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

use risingwave_common::catalog::Schema;

use super::generic::*;
use crate::session::OptimizerContextRef;

impl<PlanRef: GenericPlanRef> GenericBase for Project<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Agg<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for HopWindow<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Filter<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Join<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl GenericBase for Scan {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for TopN<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl GenericBase for Source {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for ProjectSet<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> GenericBase for Expand<PlanRef> {
    fn schema(&self) -> &Schema {
        todo!()
    }

    fn logical_pk(&self) -> &[usize] {
        todo!()
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!()
    }
}
