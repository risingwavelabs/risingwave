use std::collections::HashMap;
use std::rc::Rc;

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::{GenericPlanNode, GenericPlanRef};
use crate::catalog::source_catalog::SourceCatalog;
use crate::session::OptimizerContextRef;
use crate::TableCatalog;

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Source {
    pub catalog: Rc<SourceCatalog>,
}
impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        let fields = self
            .catalog
            .columns
            .iter()
            .map(|c| (&c.column_desc).into())
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let mut id_to_idx = HashMap::new();
        self.catalog
            .columns
            .iter()
            .enumerate()
            .for_each(|(idx, c)| {
                id_to_idx.insert(c.column_id(), idx);
            });
        self.catalog
            .pk_col_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied())
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        unimplemented!()
    }
}

impl Source {
    pub fn infer_internal_table_catalog(me: &impl GenericPlanRef) -> TableCatalog {
        // note that source's internal table is to store partition_id -> offset mapping and its
        // schema is irrelevant to input schema
        let mut builder =
            TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

        let key = Field {
            data_type: DataType::Varchar,
            name: "partition_id".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };
        let value = Field {
            data_type: DataType::Varchar,
            name: "offset".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::Ascending);

        builder.build(vec![])
    }
}
