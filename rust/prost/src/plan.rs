/// Field is a column in the streaming or batch plan.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Field {
    #[prost(message, optional, tag = "1")]
    pub data_type: ::core::option::Option<super::data::DataType>,
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DatabaseRefId {
    #[prost(int32, tag = "1")]
    pub database_id: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SchemaRefId {
    #[prost(message, optional, tag = "1")]
    pub database_ref_id: ::core::option::Option<DatabaseRefId>,
    #[prost(int32, tag = "2")]
    pub schema_id: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TableRefId {
    #[prost(message, optional, tag = "1")]
    pub schema_ref_id: ::core::option::Option<SchemaRefId>,
    #[prost(int32, tag = "2")]
    pub table_id: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ColumnDesc {
    #[prost(message, optional, tag = "1")]
    pub column_type: ::core::option::Option<super::data::DataType>,
    #[prost(int32, tag = "2")]
    pub column_id: i32,
    /// we store the column name in column desc now just for debug, but in future we should store
    /// it in ColumnCatalog but not here
    #[prost(string, tag = "3")]
    pub name: ::prost::alloc::string::String,
    /// For STRUCT type.
    #[prost(message, repeated, tag = "4")]
    pub field_descs: ::prost::alloc::vec::Vec<ColumnDesc>,
    /// The user-defined type's name. Empty if the column type is a builtin type.
    /// For example, when the type is created from a protobuf schema file,
    /// this field will store the message name.
    #[prost(string, tag = "5")]
    pub type_name: ::prost::alloc::string::String,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct OrderedColumnDesc {
    #[prost(message, optional, tag = "1")]
    pub column_desc: ::core::option::Option<ColumnDesc>,
    #[prost(enumeration = "OrderType", tag = "2")]
    pub order: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ColumnCatalog {
    #[prost(message, optional, tag = "1")]
    pub column_desc: ::core::option::Option<ColumnDesc>,
    #[prost(bool, tag = "2")]
    pub is_hidden: bool,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CellBasedTableDesc {
    #[prost(uint32, tag = "1")]
    pub table_id: u32,
    #[prost(message, repeated, tag = "2")]
    pub pk: ::prost::alloc::vec::Vec<OrderedColumnDesc>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct StreamSourceInfo {
    #[prost(bool, tag = "1")]
    pub append_only: bool,
    #[prost(map = "string, string", tag = "2")]
    pub properties:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(enumeration = "RowFormatType", tag = "3")]
    pub row_format: i32,
    #[prost(string, tag = "4")]
    pub row_schema_location: ::prost::alloc::string::String,
    #[prost(int32, tag = "5")]
    pub row_id_index: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TableSourceInfo {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct MaterializedViewInfo {
    #[prost(message, optional, tag = "1")]
    pub associated_table_ref_id: ::core::option::Option<TableRefId>,
    #[prost(message, repeated, tag = "2")]
    pub column_orders: ::prost::alloc::vec::Vec<ColumnOrder>,
    #[prost(int32, repeated, tag = "3")]
    pub pk_indices: ::prost::alloc::vec::Vec<i32>,
    #[prost(message, repeated, tag = "4")]
    pub dependent_tables: ::prost::alloc::vec::Vec<TableRefId>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct RowSeqScanNode {
    #[prost(message, optional, tag = "1")]
    pub table_desc: ::core::option::Option<CellBasedTableDesc>,
    #[prost(message, repeated, tag = "2")]
    pub column_descs: ::prost::alloc::vec::Vec<ColumnDesc>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SourceScanNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
    /// timestamp_ms is used for offset synchronization of high level consumer groups, this field
    /// will be deprecated if a more elegant approach is available in the future
    #[prost(int64, tag = "2")]
    pub timestamp_ms: i64,
    #[prost(int32, repeated, tag = "3")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
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
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct FilterScanNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
    #[prost(int32, repeated, tag = "2")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct InsertNode {
    #[prost(message, optional, tag = "1")]
    pub table_source_ref_id: ::core::option::Option<TableRefId>,
    #[prost(int32, repeated, tag = "2")]
    pub column_ids: ::prost::alloc::vec::Vec<i32>,
    #[prost(bool, tag = "3")]
    pub frontend_v2: bool,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DeleteNode {
    #[prost(message, optional, tag = "1")]
    pub table_source_ref_id: ::core::option::Option<TableRefId>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ValuesNode {
    #[prost(message, repeated, tag = "1")]
    pub tuples: ::prost::alloc::vec::Vec<values_node::ExprTuple>,
    #[prost(message, repeated, tag = "2")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
/// Nested message and enum types in `ValuesNode`.
pub mod values_node {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct ExprTuple {
        #[prost(message, repeated, tag = "1")]
        pub cells: ::prost::alloc::vec::Vec<super::super::expr::ExprNode>,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateTableNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
    /// Other fields not included yet:
    /// primarykey_col_ids, dist_type, distkey_col_id, append_only
    #[prost(message, repeated, tag = "2")]
    pub column_descs: ::prost::alloc::vec::Vec<ColumnDesc>,
    /// We re-use the CreateTableNode for creating materialized views.
    #[prost(oneof = "create_table_node::Info", tags = "3, 4")]
    pub info: ::core::option::Option<create_table_node::Info>,
}
/// Nested message and enum types in `CreateTableNode`.
pub mod create_table_node {
    /// We re-use the CreateTableNode for creating materialized views.
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Info {
        #[prost(message, tag = "3")]
        TableSource(super::TableSourceInfo),
        #[prost(message, tag = "4")]
        MaterializedView(super::MaterializedViewInfo),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ColumnOrder {
    /// maybe other name
    #[prost(enumeration = "OrderType", tag = "1")]
    pub order_type: i32,
    #[prost(message, optional, tag = "2")]
    pub input_ref: ::core::option::Option<super::expr::InputRefExpr>,
    #[prost(message, optional, tag = "3")]
    pub return_type: ::core::option::Option<super::data::DataType>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct OrderByNode {
    #[prost(message, repeated, tag = "1")]
    pub column_orders: ::prost::alloc::vec::Vec<ColumnOrder>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TopNNode {
    #[prost(message, repeated, tag = "1")]
    pub column_orders: ::prost::alloc::vec::Vec<ColumnOrder>,
    #[prost(uint32, tag = "2")]
    pub limit: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct LimitNode {
    #[prost(uint32, tag = "1")]
    pub limit: u32,
    #[prost(uint32, tag = "2")]
    pub offset: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateSourceNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
    #[prost(message, repeated, tag = "2")]
    pub column_descs: ::prost::alloc::vec::Vec<ColumnDesc>,
    #[prost(message, optional, tag = "3")]
    pub info: ::core::option::Option<StreamSourceInfo>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropSourceNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropTableNode {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<TableRefId>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct NestedLoopJoinNode {
    #[prost(enumeration = "JoinType", tag = "1")]
    pub join_type: i32,
    #[prost(message, optional, tag = "2")]
    pub join_cond: ::core::option::Option<super::expr::ExprNode>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HashAggNode {
    #[prost(uint32, repeated, tag = "1")]
    pub group_keys: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, repeated, tag = "2")]
    pub agg_calls: ::prost::alloc::vec::Vec<super::expr::AggCall>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SortAggNode {
    #[prost(message, repeated, tag = "1")]
    pub group_keys: ::prost::alloc::vec::Vec<super::expr::ExprNode>,
    #[prost(message, repeated, tag = "2")]
    pub agg_calls: ::prost::alloc::vec::Vec<super::expr::AggCall>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HashJoinNode {
    #[prost(enumeration = "JoinType", tag = "1")]
    pub join_type: i32,
    #[prost(int32, repeated, tag = "2")]
    pub left_key: ::prost::alloc::vec::Vec<i32>,
    #[prost(int32, repeated, tag = "4")]
    pub right_key: ::prost::alloc::vec::Vec<i32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SortMergeJoinNode {
    #[prost(enumeration = "JoinType", tag = "1")]
    pub join_type: i32,
    #[prost(int32, repeated, tag = "2")]
    pub left_keys: ::prost::alloc::vec::Vec<i32>,
    #[prost(int32, repeated, tag = "3")]
    pub right_keys: ::prost::alloc::vec::Vec<i32>,
    #[prost(enumeration = "OrderType", tag = "4")]
    pub direction: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GenerateInt32SeriesNode {
    #[prost(int32, tag = "1")]
    pub start: i32,
    #[prost(int32, tag = "2")]
    pub stop: i32,
    #[prost(int32, tag = "3")]
    pub step: i32,
}
/// Task is a running instance of Stage.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TaskId {
    #[prost(string, tag = "1")]
    pub query_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub stage_id: u32,
    #[prost(uint32, tag = "3")]
    pub task_id: u32,
}
/// Every task will create N buffers (channels) for parent operators to fetch results from,
/// where N is the parallelism of parent stage.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TaskOutputId {
    #[prost(message, optional, tag = "1")]
    pub task_id: ::core::option::Option<TaskId>,
    /// The id of output channel to fetch from
    #[prost(uint32, tag = "2")]
    pub output_id: u32,
}
/// ExchangeSource describes where to read results from children operators
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ExchangeSource {
    #[prost(message, optional, tag = "1")]
    pub task_output_id: ::core::option::Option<TaskOutputId>,
    #[prost(message, optional, tag = "2")]
    pub host: ::core::option::Option<super::common::HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ExchangeNode {
    #[prost(message, repeated, tag = "1")]
    pub sources: ::prost::alloc::vec::Vec<ExchangeSource>,
    #[prost(message, repeated, tag = "3")]
    pub input_schema: ::prost::alloc::vec::Vec<Field>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct MergeSortExchangeNode {
    #[prost(message, optional, tag = "1")]
    pub exchange_node: ::core::option::Option<ExchangeNode>,
    #[prost(message, repeated, tag = "2")]
    pub column_orders: ::prost::alloc::vec::Vec<ColumnOrder>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PlanNode {
    #[prost(message, repeated, tag = "1")]
    pub children: ::prost::alloc::vec::Vec<PlanNode>,
    #[prost(string, tag = "24")]
    pub identity: ::prost::alloc::string::String,
    #[prost(
        oneof = "plan_node::NodeBody",
        tags = "2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23"
    )]
    pub node_body: ::core::option::Option<plan_node::NodeBody>,
}
/// Nested message and enum types in `PlanNode`.
pub mod plan_node {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum NodeBody {
        #[prost(message, tag = "2")]
        Insert(super::InsertNode),
        #[prost(message, tag = "3")]
        Delete(super::DeleteNode),
        #[prost(message, tag = "4")]
        Project(super::ProjectNode),
        #[prost(message, tag = "5")]
        CreateTable(super::CreateTableNode),
        #[prost(message, tag = "6")]
        DropTable(super::DropTableNode),
        #[prost(message, tag = "7")]
        HashAgg(super::HashAggNode),
        #[prost(message, tag = "8")]
        Filter(super::FilterNode),
        #[prost(message, tag = "9")]
        Exchange(super::ExchangeNode),
        #[prost(message, tag = "10")]
        OrderBy(super::OrderByNode),
        #[prost(message, tag = "11")]
        NestedLoopJoin(super::NestedLoopJoinNode),
        #[prost(message, tag = "12")]
        CreateSource(super::CreateSourceNode),
        #[prost(message, tag = "13")]
        SourceScan(super::SourceScanNode),
        #[prost(message, tag = "14")]
        TopN(super::TopNNode),
        #[prost(message, tag = "15")]
        SortAgg(super::SortAggNode),
        #[prost(message, tag = "16")]
        RowSeqScan(super::RowSeqScanNode),
        #[prost(message, tag = "17")]
        Limit(super::LimitNode),
        #[prost(message, tag = "18")]
        Values(super::ValuesNode),
        #[prost(message, tag = "19")]
        HashJoin(super::HashJoinNode),
        #[prost(message, tag = "20")]
        DropSource(super::DropSourceNode),
        #[prost(message, tag = "21")]
        MergeSortExchange(super::MergeSortExchangeNode),
        #[prost(message, tag = "22")]
        SortMergeJoin(super::SortMergeJoinNode),
        #[prost(message, tag = "23")]
        GenerateInt32Series(super::GenerateInt32SeriesNode),
    }
}
/// ExchangeInfo determines how to distribute results to tasks of next stage.
///
/// Note that the fragment itself does not know the where are the receivers. Instead, it prepares
/// results in N buffers and wait for parent operators (`Exchange` nodes) to pull data from a
/// specified buffer
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ExchangeInfo {
    #[prost(enumeration = "exchange_info::DistributionMode", tag = "1")]
    pub mode: i32,
    #[prost(oneof = "exchange_info::Distribution", tags = "2, 3")]
    pub distribution: ::core::option::Option<exchange_info::Distribution>,
}
/// Nested message and enum types in `ExchangeInfo`.
pub mod exchange_info {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct BroadcastInfo {
        #[prost(uint32, tag = "1")]
        pub count: u32,
    }
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct HashInfo {
        #[prost(uint32, tag = "1")]
        pub output_count: u32,
        #[prost(uint32, repeated, tag = "3")]
        pub keys: ::prost::alloc::vec::Vec<u32>,
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
    pub enum DistributionMode {
        /// No partitioning at all, used for root segment which aggregates query results
        Single = 0,
        Broadcast = 1,
        Hash = 2,
    }
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Distribution {
        #[prost(message, tag = "2")]
        BroadcastInfo(BroadcastInfo),
        #[prost(message, tag = "3")]
        HashInfo(HashInfo),
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PlanFragment {
    #[prost(message, optional, tag = "1")]
    pub root: ::core::option::Option<PlanNode>,
    #[prost(message, optional, tag = "2")]
    pub exchange_info: ::core::option::Option<ExchangeInfo>,
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
pub enum OrderType {
    Invalid = 0,
    Ascending = 1,
    Descending = 2,
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
pub enum RowFormatType {
    Json = 0,
    Protobuf = 1,
    DebeziumJson = 2,
    Avro = 3,
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
pub enum JoinType {
    /// Note that it comes from Calcite's JoinRelType.
    /// DO NOT HAVE direction for SEMI and ANTI now.
    Inner = 0,
    LeftOuter = 1,
    RightOuter = 2,
    FullOuter = 3,
    LeftSemi = 4,
    LeftAnti = 5,
    RightSemi = 6,
    RightAnti = 7,
}
