use anyhow::Error;

pub(crate) trait ProtobufConvert: Sized {
    /// Type of the protobuf clone of Self
    type ProtoStruct;

    /// Struct -> ProtoStruct
    fn to_pb(&self) -> Self::ProtoStruct;

    /// ProtoStruct -> Struct
    fn from_pb(pb: Self::ProtoStruct) -> Result<Self, Error>;
}

impl ProtobufConvert for i32 {
    type ProtoStruct = i32;

    fn to_pb(&self) -> i32 {
        *self
    }

    fn from_pb(pb: i32) -> Result<Self, Error> {
        Ok(pb)
    }
}
