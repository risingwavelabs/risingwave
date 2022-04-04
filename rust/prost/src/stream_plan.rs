/// Hash mapping for compute node. Stores mapping from virtual key to actor id.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ActorMapping {
    #[prost(uint32, repeated, tag = "1")]
    pub hash_mapping: ::prost::alloc::vec::Vec<u32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SourceNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(int32, repeated, tag = "2")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
    #[prost(enumeration = "source_node::SourceType", tag = "3")]
    pub source_type: i32,
}
/// Nested message and enum types in `SourceNode`.
pub mod source_node {
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
    pub enum SourceType {
        Table = 0,
        Source = 1,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ProjectNode {
    #[prost(message, repeated, tag = "1")]
    pub select_list: ::prost::alloc::vec::Vec<super::expr::ExprNode>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct FilterNode {
    #[prost(message, optional, tag = "1")]
    pub search_condition: ::core::option::Option<super::expr::ExprNode>,
}
/// A materialized view is regarded as a table,
/// hence we copy the CreateTableNode definition in OLAP PlanNode.
/// In addition, we also specify primary key to MV for efficient point lookup during update and
/// deletion.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct MaterializeNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(message, optional, tag = "2")]
    pub associated_table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    /// Column indexes and orders of primary key
    #[prost(message, repeated, tag = "3")]
    pub column_orders: ::prost::alloc::vec::Vec<super::plan::ColumnOrder>,
    /// Column IDs of input schema
    #[prost(int32, repeated, tag = "4")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
    #[prost(int32, repeated, tag = "5")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
}
/// Remark by Yanghao: for both local and global we use the same node in the protobuf.
/// Local and global aggregator distinguish with each other in PlanNode definition.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SimpleAggNode {
    #[prost(message, repeated, tag = "1")]
    pub agg_calls: ::prost::alloc::vec::Vec<super::expr::AggCall>,
    #[prost(int32, repeated, tag = "2")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HashAggNode {
    #[prost(int32, repeated, tag = "1")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, repeated, tag = "2")]
    pub agg_calls: ::prost::alloc::vec::Vec<super::expr::AggCall>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TopNNode {
    #[prost(enumeration = "super::plan::OrderType", repeated, tag = "1")]
    pub order_types: ::prost::alloc::vec::Vec<i32>,
    /// 0 means no limit as limit of 0 means this node should be optimized away
    #[prost(uint64, tag = "2")]
    pub limit: u64,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(int32, repeated, tag = "4")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HashJoinNode {
    #[prost(enumeration = "super::plan::JoinType", tag = "1")]
    pub join_type: i32,
    #[prost(int32, repeated, tag = "2")]
    pub left_key: ::prost::alloc::vec::Vec<i32>,
    #[prost(int32, repeated, tag = "3")]
    pub right_key: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, optional, tag = "4")]
    pub condition: ::core::option::Option<super::expr::ExprNode>,
    #[prost(int32, repeated, tag = "5")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct MergeNode {
    #[prost(uint32, repeated, tag = "1")]
    pub upstream_actor_id: ::prost::alloc::vec::Vec<u32>,
    /// The schema of input columns.
    #[prost(message, repeated, tag = "2")]
    pub fields: ::prost::alloc::vec::Vec<super::plan::Field>,
}
/// passed from frontend to meta, used by fragmenter to generate `MergeNode`
/// and maybe `DispatcherNode` later.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ExchangeNode {
    /// The schema of input columns.
    #[prost(message, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<super::plan::Field>,
    #[prost(message, optional, tag = "2")]
    pub strategy: ::core::option::Option<DispatchStrategy>,
}
/// ChainNode is used for mv on mv.
/// ChainNode is like a "UNION" on mv snapshot and streaming. So it takes two inputs with fixed
/// order:   1. MergeNode (as a placeholder) for streaming read.
///   2. BatchPlanNode for snapshot read.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ChainNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    /// The schema of input stream, which will be used to build a MergeNode
    #[prost(message, repeated, tag = "2")]
    pub upstream_fields: ::prost::alloc::vec::Vec<super::plan::Field>,
    #[prost(int32, repeated, tag = "3")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
}
/// BatchPlanNode is used for mv on mv snapshot read.
/// BatchPlanNode is supposed to carry a batch plan that can be optimized with the streaming plan.
/// Currently, streaming to batch push down is not yet supported, BatchPlanNode is simply a table
/// scan.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct BatchPlanNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(message, repeated, tag = "2")]
    pub column_descs: ::prost::alloc::vec::Vec<super::plan::ColumnDesc>,
    #[prost(int32, repeated, tag = "3")]
    pub distribution_keys: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StreamNode {
    /// The id for the operator.
    #[prost(uint64, tag = "1")]
    pub operator_id: u64,
    /// Child node in plan aka. upstream nodes in the streaming DAG
    #[prost(message, repeated, tag = "3")]
    pub input: ::prost::alloc::vec::Vec<StreamNode>,
    #[prost(uint32, repeated, tag = "2")]
    pub pk_indices: ::prost::alloc::vec::Vec<u32>,
    #[prost(string, tag = "18")]
    pub identity: ::prost::alloc::string::String,
    #[prost(
        oneof = "stream_node::Node",
        tags = "4, 5, 6, 7, 16, 8, 9, 10, 11, 12, 13, 14, 15, 17"
    )]
    pub node: ::core::option::Option<stream_node::Node>,
}
/// Nested message and enum types in `StreamNode`.
pub mod stream_node {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag = "4")]
        SourceNode(super::SourceNode),
        #[prost(message, tag = "5")]
        ProjectNode(super::ProjectNode),
        #[prost(message, tag = "6")]
        FilterNode(super::FilterNode),
        #[prost(message, tag = "7")]
        MaterializeNode(super::MaterializeNode),
        #[prost(message, tag = "16")]
        LocalSimpleAggNode(super::SimpleAggNode),
        #[prost(message, tag = "8")]
        GlobalSimpleAggNode(super::SimpleAggNode),
        #[prost(message, tag = "9")]
        HashAggNode(super::HashAggNode),
        #[prost(message, tag = "10")]
        AppendOnlyTopNNode(super::TopNNode),
        #[prost(message, tag = "11")]
        HashJoinNode(super::HashJoinNode),
        #[prost(message, tag = "12")]
        TopNNode(super::TopNNode),
        #[prost(message, tag = "13")]
        MergeNode(super::MergeNode),
        #[prost(message, tag = "14")]
        ExchangeNode(super::ExchangeNode),
        #[prost(message, tag = "15")]
        ChainNode(super::ChainNode),
        #[prost(message, tag = "17")]
        BatchPlanNode(super::BatchPlanNode),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DispatchStrategy {
    #[prost(enumeration = "DispatcherType", tag = "1")]
    pub r#type: i32,
    #[prost(uint32, repeated, tag = "2")]
    pub column_indices: ::prost::alloc::vec::Vec<u32>,
}
/// A dispatcher redistribute messages.
/// We encode both the type and other usage information in the proto.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Dispatcher {
    #[prost(enumeration = "DispatcherType", tag = "1")]
    pub r#type: i32,
    #[prost(uint32, repeated, tag = "2")]
    pub column_indices: ::prost::alloc::vec::Vec<u32>,
    /// The hash mapping for consistent hash.
    #[prost(message, optional, tag = "3")]
    pub hash_mapping: ::core::option::Option<ActorMapping>,
    /// Number of downstreams decides how many endpoints a dispatcher should dispatch.
    #[prost(uint32, repeated, tag = "5")]
    pub downstream_actor_id: ::prost::alloc::vec::Vec<u32>,
}
/// A StreamActor is a running fragment of the overall stream graph,
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StreamActor {
    #[prost(uint32, tag = "1")]
    pub actor_id: u32,
    #[prost(uint32, tag = "2")]
    pub fragment_id: u32,
    #[prost(message, optional, tag = "3")]
    pub nodes: ::core::option::Option<StreamNode>,
    #[prost(message, repeated, tag = "4")]
    pub dispatcher: ::prost::alloc::vec::Vec<Dispatcher>,
    /// The actors that send messages to this actor.
    /// Note that upstream actor ids are also stored in the proto of merge nodes.
    /// It is painstaking to traverse through the node tree and get upstream actor id from the root
    /// StreamNode. We duplicate the information here to ease the parsing logic in stream
    /// manager.
    #[prost(uint32, repeated, tag = "6")]
    pub upstream_actor_id: ::prost::alloc::vec::Vec<u32>,
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
pub enum DispatcherType {
    Invalid = 0,
    Hash = 1,
    Broadcast = 2,
    Simple = 3,
}
