#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Status {
    #[prost(enumeration = "status::Code", tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
/// Nested message and enum types in `Status`.
pub mod status {
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
    pub enum Code {
        Ok = 0,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HostAddress {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub port: i32,
}
/// Encode which host machine an actor resides.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ActorInfo {
    #[prost(uint32, tag = "1")]
    pub actor_id: u32,
    #[prost(message, optional, tag = "2")]
    pub host: ::core::option::Option<HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ParallelUnit {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(enumeration = "ParallelUnitType", tag = "2")]
    pub r#type: i32,
    #[prost(uint32, tag = "3")]
    pub worker_node_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct WorkerNode {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(enumeration = "WorkerType", tag = "2")]
    pub r#type: i32,
    #[prost(message, optional, tag = "3")]
    pub host: ::core::option::Option<HostAddress>,
    #[prost(enumeration = "worker_node::State", tag = "4")]
    pub state: i32,
    /// a mapping from logical key to parallel unit, with logical key as the index of array
    #[prost(message, repeated, tag = "5")]
    pub parallel_units: ::prost::alloc::vec::Vec<ParallelUnit>,
}
/// Nested message and enum types in `WorkerNode`.
pub mod worker_node {
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
    pub enum State {
        Starting = 0,
        Running = 1,
    }
}
/// A cluster can be either a set of OLAP compute nodes, or a set of streaming compute nodes.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Cluster {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(message, repeated, tag = "2")]
    pub nodes: ::prost::alloc::vec::Vec<WorkerNode>,
    #[prost(map = "string, string", tag = "3")]
    pub config:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
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
pub enum WorkerType {
    Frontend = 0,
    ComputeNode = 1,
    RiseCtl = 2,
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
pub enum ParallelUnitType {
    Single = 0,
    Hash = 1,
}
