use crate::util::ProtobufConvert;
use protobuf_convert::ProtobufConvert;

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, ProtobufConvert)]
#[protobuf_convert(source = "risingwave_proto::plan::DatabaseRefId")]
pub(crate) struct DatabaseId {
    database_id: i32,
}

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, ProtobufConvert)]
#[protobuf_convert(source = "risingwave_proto::plan::SchemaRefId")]
pub(crate) struct SchemaId {
    database_ref_id: DatabaseId,
    schema_id: i32,
}

#[derive(Clone, Debug, Hash, PartialOrd, PartialEq, Eq, ProtobufConvert)]
#[protobuf_convert(source = "risingwave_proto::plan::TableRefId")]
pub(crate) struct TableId {
    schema_ref_id: SchemaId,
    table_id: i32,
}
