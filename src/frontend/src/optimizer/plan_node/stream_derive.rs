use risingwave_common::catalog::Schema;

use super::generic::GenericBase;
use super::stream::*;
use crate::session::OptimizerContextRef;

impl GenericBase for DynamicFilter {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for DynamicFilter {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Exchange {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for Exchange {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for DeltaJoin {
    fn schema(&self) -> &Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> &[usize] {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }
}

impl StreamBase for DeltaJoin {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Expand {
    fn schema(&self) -> &Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> &[usize] {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }
}

impl StreamBase for Expand {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Filter {
    fn schema(&self) -> &Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> &[usize] {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }
}

impl StreamBase for Filter {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for GlobalSimpleAgg {
    fn schema(&self) -> &Schema {
        self.core.schema()
    }

    fn logical_pk(&self) -> &[usize] {
        self.core.logical_pk()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.core.ctx()
    }
}

impl StreamBase for GlobalSimpleAgg {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for GroupTopN {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for GroupTopN {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for HashAgg {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for HashAgg {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for HashJoin {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for HashJoin {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for HopWindow {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for HopWindow {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for IndexScan {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for IndexScan {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for LocalSimpleAgg {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for LocalSimpleAgg {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Materialize {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for Materialize {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for ProjectSet {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for ProjectSet {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Project {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for Project {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Sink {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for Sink {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for Source {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for Source {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for TableScan {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for TableScan {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}

impl GenericBase for TopN {
    fn schema(&self) -> &Schema {
        todo!("new plan node derivation")
    }

    fn logical_pk(&self) -> &[usize] {
        todo!("new plan node derivation")
    }

    fn ctx(&self) -> OptimizerContextRef {
        todo!("new plan node derivation")
    }
}

impl StreamBase for TopN {
    fn distribution(&self) -> &crate::optimizer::property::Distribution {
        todo!()
    }

    fn append_only(&self) -> bool {
        todo!()
    }
}
