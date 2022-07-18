// use risingwave_pb::plan_commoEn::ColumnDesc as ProstColumnDesc;
use risingwave_common::catalog::{ColumnDesc, OrderedColumnDesc};
use risingwave_common::types::DataType;
// use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;
// use risingwave_pb::plan_common::ColumnCatalog;
use risingwave_common::util::ordered::OrderedRowDeserializer;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::Table;

trait SliceTransform<'a> {
    fn transfer(&self, full_key: &'a [u8]) -> &'a [u8];
}

pub struct SchemaSliceTransform {
    // table_catalog: Table,
    read_pattern_prefix_column: u32,
    // data_types: Vec<DataType>,
    // order_types: Vec<OrderType>,
    deserializer: OrderedRowDeserializer,
}

impl<'a> SliceTransform<'a> for SchemaSliceTransform {
    fn transfer(&self, full_key: &'a [u8]) -> &'a [u8] {
        let prefix_len = self
            .deserializer
            .deserialize_prefix_len_with_column_indices(
                &full_key,
                (0..self.read_pattern_prefix_column as usize).collect(),
            )
            .unwrap();
        &full_key[0..prefix_len]
    }
}

impl SchemaSliceTransform {
    fn new(table_catalog: &Table) -> Self {
        let data_types: Vec<DataType> = table_catalog
            .columns
            .iter()
            .map(|col| ColumnDesc::from(col.column_desc.as_ref().unwrap()).data_type)
            .collect();

        let order_types: Vec<OrderType> = table_catalog
            .order_key
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();

        Self {
            read_pattern_prefix_column: table_catalog.read_pattern_prefix_column,
            deserializer: OrderedRowDeserializer::new(data_types, order_types),
        }
    }
}
