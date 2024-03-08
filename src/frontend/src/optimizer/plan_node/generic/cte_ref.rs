use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Weak;

use pretty_xmlish::StrAssocArr;
use risingwave_common::catalog::Schema;

use super::{impl_distill_unit_from_fields, GenericPlanNode, GenericPlanRef};
use crate::binder::ShareId;
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Clone, Debug)]
pub struct CteRef<PlanRef> {
    share_id: ShareId,
    pub r#ref: Weak<PlanRef>,
    base: PlanRef,
    derived_stream_key: RefCell<Option<Option<Vec<usize>>>>,
    deriving: RefCell<bool>,
}

impl<PlanRef> PartialEq for CteRef<PlanRef> {
    fn eq(&self, other: &Self) -> bool {
        self.share_id == other.share_id
    }
}

impl<PlanRef> Eq for CteRef<PlanRef> {}

impl<PlanRef> Hash for CteRef<PlanRef> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.share_id.hash(state);
    }
}

impl<PlanRef> CteRef<PlanRef> {
    pub fn new(share_id: ShareId, r#ref: Weak<PlanRef>, base: PlanRef) -> Self {
        Self {
            share_id,
            r#ref,
            base,
            derived_stream_key: RefCell::new(None),
            deriving: RefCell::new(false),
        }
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for CteRef<PlanRef> {
    fn schema(&self) -> Schema {
        self.base.schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        if let Some(derived_stream_key) = self.derived_stream_key.borrow().as_ref() {
            return derived_stream_key.clone();
        }
        if *self.deriving.borrow() {
            return self.base.stream_key().map(Into::into);
        }
        *self.deriving.borrow_mut() = true;
        let derived_stream_key = self.r#ref.upgrade().unwrap().stream_key().map(Into::into);
        *self.deriving.borrow_mut() = false;
        *self.derived_stream_key.borrow_mut() = Some(derived_stream_key.clone());
        derived_stream_key
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.r#ref.upgrade().unwrap().ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        todo!()
    }
}

impl<PlanRef: GenericPlanRef> CteRef<PlanRef> {
    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        vec![]
    }
}

impl_distill_unit_from_fields! {CteRef, GenericPlanRef}
