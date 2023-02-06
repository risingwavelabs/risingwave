use super::{BoxedRule, Rule};
use crate::catalog::IndexCatalog;
use crate::optimizer::plan_node::{LogicalTopN, LogicalScan};
use crate::optimizer::PlanRef;
use crate::optimizer::property::{FieldOrder, Order};
use crate::optimizer::property::Direction::Asc;

pub struct AggOnIndexRule {}

impl Rule for AggOnIndexRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_topn: &LogicalTopN = plan.as_logical_top_n()?;
        let logical_scan: LogicalScan = logical_topn.get_child().as_logical_scan()?.to_owned();
        let order = logical_topn.order();
        if order.field_order.is_empty() {
            return None;
        }
        let index = logical_scan.indexes().iter().find(|idx| {
            Order {
                field_order: idx
                    .index_item
                    .iter()
                    .map(|idx_item| FieldOrder {
                        index: idx_item.index,
                        direct: Asc,
                    })
                    .collect(),
            }
            .satisfies(order)
        })?;

        let p2s_mapping = index.primary_to_secondary_mapping();

        if logical_scan.required_col_idx()
        .iter()
        .all(|x| p2s_mapping.contains_key(x)) {
            
        } else {
            None
        }
    }
}

impl AggOnIndexRule {
    pub fn create() -> BoxedRule {
        Box::new(AggOnIndexRule {})
    }
}
