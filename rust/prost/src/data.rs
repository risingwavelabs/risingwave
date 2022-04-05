#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Buffer {
    #[prost(enumeration = "buffer::CompressionType", tag = "1")]
    pub compression: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub body: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `Buffer`.
pub mod buffer {
    #[derive(
        prost_helpers::AnyPB,
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum CompressionType {
        Invalid = 0,
        None = 1,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DataType {
    #[prost(enumeration = "data_type::TypeName", tag = "1")]
    pub type_name: i32,
    /// Data length for char.
    /// Max data length for varchar.
    /// Precision for time, decimal.
    #[prost(uint32, tag = "2")]
    pub precision: u32,
    /// Scale for decimal.
    #[prost(uint32, tag = "3")]
    pub scale: u32,
    #[prost(bool, tag = "4")]
    pub is_nullable: bool,
    #[prost(enumeration = "data_type::IntervalType", tag = "5")]
    pub interval_type: i32,
}
/// Nested message and enum types in `DataType`.
pub mod data_type {
    #[derive(
        prost_helpers::AnyPB,
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum IntervalType {
        Invalid = 0,
        Year = 1,
        Month = 2,
        Day = 3,
        Hour = 4,
        Minute = 5,
        Second = 6,
        YearToMonth = 7,
        DayToHour = 8,
        DayToMinute = 9,
        DayToSecond = 10,
        HourToMinute = 11,
        HourToSecond = 12,
        MinuteToSecond = 13,
    }
    #[derive(
        prost_helpers::AnyPB,
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration,
    )]
    #[repr(i32)]
    pub enum TypeName {
        Int16 = 0,
        Int32 = 1,
        Int64 = 2,
        Float = 3,
        Double = 4,
        Boolean = 5,
        Char = 6,
        Varchar = 7,
        Decimal = 8,
        Time = 9,
        Timestamp = 10,
        Interval = 11,
        Date = 12,
        /// Timestamp type with timezone
        Timestampz = 13,
        Symbol = 14,
        Struct = 15,
        List = 16,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StructArrayData {
    #[prost(message, repeated, tag = "1")]
    pub children_array: ::prost::alloc::vec::Vec<Array>,
    #[prost(message, repeated, tag = "2")]
    pub children_type: ::prost::alloc::vec::Vec<DataType>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ListArrayData {
    #[prost(uint32, repeated, tag = "1")]
    pub offsets: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<Array>>,
    #[prost(message, optional, tag = "3")]
    pub value_type: ::core::option::Option<DataType>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Array {
    #[prost(enumeration = "ArrayType", tag = "1")]
    pub array_type: i32,
    #[prost(message, optional, tag = "2")]
    pub null_bitmap: ::core::option::Option<Buffer>,
    #[prost(message, repeated, tag = "3")]
    pub values: ::prost::alloc::vec::Vec<Buffer>,
    #[prost(message, optional, tag = "4")]
    pub struct_array_data: ::core::option::Option<StructArrayData>,
    #[prost(message, optional, boxed, tag = "5")]
    pub list_array_data: ::core::option::Option<::prost::alloc::boxed::Box<ListArrayData>>,
}
/// New column proto def to replace fixed width column. This def
/// aims to include all column type. Currently it do not support struct/array
/// but capable of extending in future by add other fields.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Column {
    #[prost(message, optional, tag = "2")]
    pub array: ::core::option::Option<Array>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DataChunk {
    #[prost(uint32, tag = "1")]
    pub cardinality: u32,
    #[prost(message, repeated, tag = "2")]
    pub columns: ::prost::alloc::vec::Vec<Column>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StreamMessage {
    #[prost(oneof = "stream_message::StreamMessage", tags = "1, 2")]
    pub stream_message: ::core::option::Option<stream_message::StreamMessage>,
}
/// Nested message and enum types in `StreamMessage`.
pub mod stream_message {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum StreamMessage {
        #[prost(message, tag = "1")]
        StreamChunk(super::StreamChunk),
        #[prost(message, tag = "2")]
        Barrier(super::Barrier),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StreamChunk {
    /// for Column::from_protobuf(), may not need later
    #[prost(uint32, tag = "1")]
    pub cardinality: u32,
    #[prost(enumeration = "Op", repeated, tag = "2")]
    pub ops: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, repeated, tag = "3")]
    pub columns: ::prost::alloc::vec::Vec<Column>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct NothingMutation {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StopMutation {
    #[prost(uint32, repeated, tag = "1")]
    pub actors: ::prost::alloc::vec::Vec<u32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Actors {
    #[prost(message, repeated, tag = "1")]
    pub info: ::prost::alloc::vec::Vec<super::common::ActorInfo>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UpdateMutation {
    #[prost(map = "uint32, message", tag = "1")]
    pub actors: ::std::collections::HashMap<u32, Actors>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AddMutation {
    #[prost(map = "uint32, message", tag = "1")]
    pub actors: ::std::collections::HashMap<u32, Actors>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Epoch {
    #[prost(uint64, tag = "1")]
    pub curr: u64,
    #[prost(uint64, tag = "2")]
    pub prev: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Barrier {
    #[prost(message, optional, tag = "1")]
    pub epoch: ::core::option::Option<Epoch>,
    #[prost(bytes = "vec", tag = "6")]
    pub span: ::prost::alloc::vec::Vec<u8>,
    #[prost(oneof = "barrier::Mutation", tags = "2, 3, 4, 5")]
    pub mutation: ::core::option::Option<barrier::Mutation>,
}
/// Nested message and enum types in `Barrier`.
pub mod barrier {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Mutation {
        #[prost(message, tag = "2")]
        Nothing(super::NothingMutation),
        #[prost(message, tag = "3")]
        Stop(super::StopMutation),
        #[prost(message, tag = "4")]
        Update(super::UpdateMutation),
        #[prost(message, tag = "5")]
        Add(super::AddMutation),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Terminate {}
#[derive(
    prost_helpers::AnyPB,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    ::prost::Enumeration,
)]
#[repr(i32)]
pub enum ArrayType {
    Int16 = 0,
    Int32 = 1,
    Int64 = 2,
    Float32 = 3,
    Float64 = 4,
    Utf8 = 5,
    Bool = 6,
    Decimal = 7,
    Date = 8,
    Time = 9,
    Timestamp = 10,
    Struct = 11,
    List = 12,
}
#[derive(
    prost_helpers::AnyPB,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    ::prost::Enumeration,
)]
#[repr(i32)]
pub enum Op {
    Insert = 0,
    Delete = 1,
    UpdateInsert = 2,
    UpdateDelete = 3,
}
