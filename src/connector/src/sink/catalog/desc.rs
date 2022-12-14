use risingwave_common::catalog::{ColumnDesc, DatabaseId, SchemaId};
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::stream_plan::SinkDesc as SinkDescProst;

use super::{SinkCatalog, SinkConfig};
use crate::sink::catalog::SinkId;

#[derive(Debug, Clone)]
pub struct SinkDesc {
    /// Id of the sink, just for debug now.
    pub id: SinkId,
    /// Id of the sink, just for debug now.
    pub name: String,
    /// the sql of the sink, just for debug now.
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
}

impl SinkDesc {
    pub fn into_catalog(
        self,
        schema_id: SchemaId,
        database_id: DatabaseId,
        owner: u32,
        dependent_relations: Vec<u32>,
    ) -> SinkCatalog {
        SinkCatalog {
            id: self.id,
            schema_id,
            database_id,
            name: self.name,
            definition: self.definition,
            columns: self.columns,
            pk: self.pk,
            distribution_key: self.distribution_key,
            appendonly: self.appendonly,
            config: self.config,
            owner,
            dependent_relations,
        }
    }

    pub fn to_proto(&self) -> SinkDescProst {
        SinkDescProst {
            id: self.id.into(),
            name: self.name.clone(),
            definition: self.definition.clone(),
            columns: self.columns.iter().map(Into::into).collect(),
            pk: self.pk.iter().map(|v| v.to_protobuf()).collect(),
            distribution_key: self.distribution_key.iter().map(|k| *k as u32).collect(),
            appendonly: self.appendonly,
            sink_config: self.config.clone().into_hashmap(),
        }
    }
}

impl TryFrom<SinkDescProst> for SinkDesc {
    type Error = crate::sink::SinkError;

    fn try_from(proto: SinkDescProst) -> Result<Self, Self::Error> {
        Ok(Self {
            id: proto.id.into(),
            name: proto.name,
            definition: proto.definition,
            columns: proto.columns.into_iter().map(Into::into).collect(),
            pk: proto.pk.iter().map(OrderPair::from_prost).collect(),
            distribution_key: proto
                .distribution_key
                .into_iter()
                .map(|k| k as usize)
                .collect(),
            appendonly: proto.appendonly,
            config: SinkConfig::from_hashmap(proto.sink_config)?,
        })
    }
}
