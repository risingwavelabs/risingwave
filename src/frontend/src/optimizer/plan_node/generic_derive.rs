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
