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
