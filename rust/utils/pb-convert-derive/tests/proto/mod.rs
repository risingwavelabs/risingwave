use pb_convert_derive::FromProtobuf;
use pb_convert_derive::IntoProtobuf;

pub mod gen {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

#[derive(FromProtobuf, IntoProtobuf, PartialEq, Debug)]
#[pb_convert(pb_type = "crate::proto::gen::message::SubMessageProto")]
pub struct SubMessage {
    #[pb_convert(pb_field = "inner_id")]
    pub id: u32,
    #[pb_convert(pb_field = "inner_name")]
    pub name: String,
    #[pb_convert(skip = true)]
    pub other: String,
}

#[derive(FromProtobuf, IntoProtobuf, PartialEq, Debug)]
#[pb_convert(pb_type = "crate::proto::gen::message::OuterEnumProto")]
pub enum OuterEnum {
    INVALID,
    #[pb_convert(pb_variant = "OK")]
    OK2,
    #[pb_convert(pb_variant = "ERROR")]
    ERROR2,
}

#[derive(FromProtobuf, IntoProtobuf, PartialEq, Debug)]
#[pb_convert(pb_type = "crate::proto::gen::message::OuterMessageProto")]
pub struct OuterMessage {
    pub float64_data: f64,
    pub float32_data: f32,
    pub int32_data: i32,
    pub int64_data: i64,
    pub uint32_data: u32,
    pub uint64_data: u64,
    pub bool_data: bool,
    pub string_data: String,
    #[pb_convert(pb_field = "bytes_data")]
    pub bytes: Vec<u8>,
    #[pb_convert(pb_field = "message_data")]
    pub sub_message: SubMessage,
    pub enum_data: OuterEnum,
    #[pb_convert(skip = true)]
    pub other_skip: bool,
}
