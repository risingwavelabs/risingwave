pub mod desc;

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, DatabaseId, SchemaId, TableId, UserId};
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::catalog::Sink as ProstSink;

#[derive(Clone, Copy, Debug, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct SinkId {
    pub sink_id: u32,
}

impl SinkId {
    pub const fn new(sink_id: u32) -> Self {
        SinkId { sink_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        SinkId {
            sink_id: u32::MAX - 1,
        }
    }

    pub fn sink_id(&self) -> u32 {
        self.sink_id
    }
}

impl From<u32> for SinkId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<SinkId> for u32 {
    fn from(id: SinkId) -> Self {
        id.sink_id
    }
}

#[derive(Clone, Debug)]
pub struct SinkCatalog {
    /// Id of the sink.
    pub id: SinkId,

    /// Schema of the sink.
    pub schema_id: SchemaId,

    /// Database of the sink.
    pub database_id: DatabaseId,

    /// Name of the sink.
    pub name: String,

    /// The full `CREATE SINK` definition of the sink.
    pub definition: String,

    /// All columns of the sink. Note that this is NOT sorted by columnId in the vector.
    pub columns: Vec<ColumnCatalog>,

    /// Primiary keys of the sink (connector). Now the sink does not care about a field's
    /// order (ASC/DESC).
    pub pk: Vec<OrderPair>,

    /// Primary key indices of the corresponding sink operator's output.
    pub stream_key: Vec<usize>,

    /// Distribution key indices of the sink. For example, if `distribution_key = [1, 2]`, then the
    /// distribution keys will be `columns[1]` and `columns[2]`.
    pub distribution_key: Vec<usize>,

    /// Whether the sink is append-only.
    pub append_only: bool,

    /// The properties of the sink.
    pub properties: HashMap<String, String>,

    /// Owner of the sink.
    pub owner: UserId,

    // Relations on which the sink depends.
    pub dependent_relations: Vec<TableId>,
}

impl SinkCatalog {
    pub fn to_proto(&self) -> ProstSink {
        ProstSink {
            id: self.id.into(),
            schema_id: self.schema_id.schema_id,
            database_id: self.database_id.database_id,
            name: self.name.clone(),
            definition: self.definition.clone(),
            columns: self.columns.iter().map(|c| c.to_protobuf()).collect_vec(),
            pk: self.pk.iter().map(|o| o.to_protobuf()).collect(),
            stream_key: self.stream_key.iter().map(|idx| *idx as i32).collect_vec(),
            dependent_relations: self
                .dependent_relations
                .iter()
                .map(|id| id.table_id)
                .collect_vec(),
            distribution_key: self
                .distribution_key
                .iter()
                .map(|k| *k as i32)
                .collect_vec(),
            append_only: self.append_only,
            owner: self.owner.into(),
            properties: self.properties.clone(),
        }
    }
}

impl From<ProstSink> for SinkCatalog {
    fn from(pb: ProstSink) -> Self {
        SinkCatalog {
            id: pb.id.into(),
            name: pb.name.clone(),
            schema_id: pb.schema_id.into(),
            database_id: pb.database_id.into(),
            definition: pb.definition.clone(),
            columns: pb
                .columns
                .into_iter()
                .map(ColumnCatalog::from)
                .collect_vec(),
            pk: pb.pk.iter().map(OrderPair::from_prost).collect_vec(),
            stream_key: pb.stream_key.iter().map(|k| *k as _).collect_vec(),
            distribution_key: pb.distribution_key.iter().map(|k| *k as _).collect_vec(),
            append_only: pb.append_only,
            properties: pb.properties.clone(),
            owner: pb.owner.into(),
            dependent_relations: pb
                .dependent_relations
                .into_iter()
                .map(TableId::from)
                .collect_vec(),
        }
    }
}

impl From<&ProstSink> for SinkCatalog {
    fn from(pb: &ProstSink) -> Self {
        pb.clone().into()
    }
}
