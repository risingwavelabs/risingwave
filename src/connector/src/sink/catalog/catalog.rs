use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId, DatabaseId, SchemaId, TableId};
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::catalog::Sink as SinkCatalogProst;
use risingwave_pb::plan_common::ColumnCatalog;

use super::SinkConfig;
use crate::sink::catalog::SinkId;

#[derive(Debug, Clone)]
pub struct SinkCatalog {
    /// Id of the sink
    pub id: SinkId,
    /// Schema of the sink
    pub schema_id: SchemaId,
    /// Database of the sink
    pub database_id: DatabaseId,
    /// Name of the sink
    pub name: String,
    /// the sql of the sink
    pub definition: String,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,
    /// The primiary key of the sink, now our sinks do not care about the field's order(ASC/DESC)
    pub pk: Vec<OrderPair>,
    /// Distribution keys of this table, which corresponds to the corresponding column of the
    /// index. e.g., if `distribution_key = [1, 2]`, then `columns[1]` and `columns[2]` are used
    /// as distribution key.
    pub distribution_key: Vec<usize>,
    /// Whether the sink is append-only
    pub appendonly: bool,
    /// the config/properties of the sink
    pub config: SinkConfig,
    // TODO: common::UserId
    /// the owner of the sink
    pub owner: u32,
    // TODO: common::RelationId
    pub dependent_relations: Vec<u32>,
}

impl SinkCatalog {
    pub fn to_proto(&self) -> SinkCatalogProst {
        SinkCatalogProst {
            id: self.id.into(),
            schema_id: self.schema_id.inner() as u32,
            database_id: self.database_id.inner() as u32,
            name: self.name.clone(),
            columns: self.columns.iter().map(|c| c.to_protobuf()).collect(),
            pk: self.pk.iter().map(|o| o.to_protobuf()).collect(),
            dependent_relations: self.dependent_relations.clone(),
            distribution_key: self
                .distribution_key
                .iter()
                .map(|k| *k as i32)
                .collect_vec(),
            appendonly: todo!(),
            owner: todo!(),
            properties: todo!(),
            definition: todo!(),
        }
    }
}
