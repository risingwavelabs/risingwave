#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ComplexRecursiveMessage {
    #[prost(string, tag = "1")]
    pub node_name: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub node_id: i32,
    #[prost(message, repeated, tag = "3")]
    pub attributes: ::prost::alloc::vec::Vec<complex_recursive_message::Attributes>,
    #[prost(message, optional, tag = "4")]
    pub parent: ::core::option::Option<complex_recursive_message::Parent>,
    #[prost(message, repeated, tag = "5")]
    pub children: ::prost::alloc::vec::Vec<ComplexRecursiveMessage>,
}
/// Nested message and enum types in `ComplexRecursiveMessage`.
pub mod complex_recursive_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Attributes {
        #[prost(string, tag = "1")]
        pub key: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub value: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Parent {
        #[prost(string, tag = "1")]
        pub parent_name: ::prost::alloc::string::String,
        #[prost(int32, tag = "2")]
        pub parent_id: i32,
        #[prost(message, repeated, tag = "3")]
        pub siblings: ::prost::alloc::vec::Vec<super::ComplexRecursiveMessage>,
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AllTypes {
    /// standard types
    #[prost(double, tag = "1")]
    pub double_field: f64,
    #[prost(float, tag = "2")]
    pub float_field: f32,
    #[prost(int32, tag = "3")]
    pub int32_field: i32,
    #[prost(int64, tag = "4")]
    pub int64_field: i64,
    #[prost(uint32, tag = "5")]
    pub uint32_field: u32,
    #[prost(uint64, tag = "6")]
    pub uint64_field: u64,
    #[prost(sint32, tag = "7")]
    pub sint32_field: i32,
    #[prost(sint64, tag = "8")]
    pub sint64_field: i64,
    #[prost(fixed32, tag = "9")]
    pub fixed32_field: u32,
    #[prost(fixed64, tag = "10")]
    pub fixed64_field: u64,
    #[prost(sfixed32, tag = "11")]
    pub sfixed32_field: i32,
    #[prost(sfixed64, tag = "12")]
    pub sfixed64_field: i64,
    #[prost(bool, tag = "13")]
    pub bool_field: bool,
    #[prost(string, tag = "14")]
    pub string_field: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "15")]
    pub bytes_field: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration = "all_types::EnumType", tag = "16")]
    pub enum_field: i32,
    #[prost(message, optional, tag = "17")]
    pub nested_message_field: ::core::option::Option<all_types::NestedMessage>,
    /// repeated field
    #[prost(int32, repeated, tag = "18")]
    pub repeated_int_field: ::prost::alloc::vec::Vec<i32>,
    /// timestamp
    #[prost(message, optional, tag = "23")]
    pub timestamp_field: ::core::option::Option<::prost_types::Timestamp>,
    /// duration
    #[prost(message, optional, tag = "24")]
    pub duration_field: ::core::option::Option<::prost_types::Duration>,
    /// any
    #[prost(message, optional, tag = "25")]
    pub any_field: ::core::option::Option<::prost_types::Any>,
    /// wrapper types
    #[prost(message, optional, tag = "27")]
    pub int32_value_field: ::core::option::Option<i32>,
    #[prost(message, optional, tag = "28")]
    pub string_value_field: ::core::option::Option<::prost::alloc::string::String>,
    /// oneof field
    #[prost(oneof = "all_types::ExampleOneof", tags = "19, 20, 21")]
    pub example_oneof: ::core::option::Option<all_types::ExampleOneof>,
}
/// Nested message and enum types in `AllTypes`.
pub mod all_types {
    /// nested message
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct NestedMessage {
        #[prost(int32, tag = "1")]
        pub id: i32,
        #[prost(string, tag = "2")]
        pub name: ::prost::alloc::string::String,
    }
    /// enum
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum EnumType {
        Default = 0,
        Option1 = 1,
        Option2 = 2,
    }
    impl EnumType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EnumType::Default => "DEFAULT",
                EnumType::Option1 => "OPTION1",
                EnumType::Option2 => "OPTION2",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "DEFAULT" => Some(Self::Default),
                "OPTION1" => Some(Self::Option1),
                "OPTION2" => Some(Self::Option2),
                _ => None,
            }
        }
    }
    /// oneof field
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExampleOneof {
        #[prost(string, tag = "19")]
        OneofString(::prost::alloc::string::String),
        #[prost(int32, tag = "20")]
        OneofInt32(i32),
        #[prost(enumeration = "EnumType", tag = "21")]
        OneofEnum(i32),
    }
}
