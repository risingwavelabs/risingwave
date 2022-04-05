/// Hash mapping for meta. Stores mapping from virtual key to parallel unit id.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ParallelUnitMapping {
    #[prost(uint32, repeated, tag = "1")]
    pub hash_mapping: ::prost::alloc::vec::Vec<u32>,
}
/// will be deprecated and replaced by catalog.Table and catalog.Source
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Table {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(string, tag = "2")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub column_descs: ::prost::alloc::vec::Vec<super::plan::ColumnDesc>,
    #[prost(uint64, tag = "4")]
    pub version: u64,
    #[prost(oneof = "table::Info", tags = "5, 6, 7")]
    pub info: ::core::option::Option<table::Info>,
}
/// Nested message and enum types in `Table`.
pub mod table {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Info {
        #[prost(message, tag = "5")]
        StreamSource(super::super::plan::StreamSourceInfo),
        #[prost(message, tag = "6")]
        TableSource(super::super::plan::TableSourceInfo),
        #[prost(message, tag = "7")]
        MaterializedView(super::super::plan::MaterializedViewInfo),
    }
}
/// will be deprecated and replaced by catalog.Database
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Database {
    #[prost(message, optional, tag = "1")]
    pub database_ref_id: ::core::option::Option<super::plan::DatabaseRefId>,
    #[prost(string, tag = "2")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
/// will be deprecated and replaced by catalog.Schema
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, optional, tag = "1")]
    pub schema_ref_id: ::core::option::Option<super::plan::SchemaRefId>,
    #[prost(string, tag = "2")]
    pub schema_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
// Below for epoch service.

#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetEpochRequest {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetEpochResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}
/// Below for catalog service.
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateRequest {
    #[prost(uint32, tag = "1")]
    pub node_id: u32,
    #[prost(oneof = "create_request::CatalogBody", tags = "2, 3, 4")]
    pub catalog_body: ::core::option::Option<create_request::CatalogBody>,
}
/// Nested message and enum types in `CreateRequest`.
pub mod create_request {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum CatalogBody {
        #[prost(message, tag = "2")]
        Database(super::Database),
        #[prost(message, tag = "3")]
        Schema(super::Schema),
        #[prost(message, tag = "4")]
        Table(super::Table),
    }
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(int32, tag = "2")]
    pub id: i32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropRequest {
    #[prost(uint32, tag = "1")]
    pub node_id: u32,
    #[prost(oneof = "drop_request::CatalogId", tags = "2, 3, 4")]
    pub catalog_id: ::core::option::Option<drop_request::CatalogId>,
}
/// Nested message and enum types in `DropRequest`.
pub mod drop_request {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum CatalogId {
        #[prost(message, tag = "2")]
        DatabaseId(super::super::plan::DatabaseRefId),
        #[prost(message, tag = "3")]
        SchemaId(super::super::plan::SchemaRefId),
        #[prost(message, tag = "4")]
        TableId(super::super::plan::TableRefId),
    }
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
/// will be deprecated and replaced by NotificationService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogRequest {}
/// will be deprecated and replaced by SubscribeFrontendSnapshot
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Catalog {
    #[prost(uint64, tag = "1")]
    pub version: u64,
    #[prost(message, repeated, tag = "2")]
    pub databases: ::prost::alloc::vec::Vec<Database>,
    #[prost(message, repeated, tag = "3")]
    pub schemas: ::prost::alloc::vec::Vec<Schema>,
    #[prost(message, repeated, tag = "4")]
    pub tables: ::prost::alloc::vec::Vec<Table>,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetCatalogResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub catalog: ::core::option::Option<Catalog>,
}
// Below for heartbeat.

#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatRequest {
    #[prost(uint32, tag = "1")]
    pub node_id: u32,
    #[prost(enumeration = "super::common::WorkerType", tag = "2")]
    pub worker_type: i32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HeartbeatResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
// Below for stream manager.

/// Fragments of a Materialized View
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TableFragments {
    #[prost(message, optional, tag = "1")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(map = "uint32, message", tag = "2")]
    pub fragments: ::std::collections::HashMap<u32, table_fragments::Fragment>,
    #[prost(map = "uint32, message", tag = "3")]
    pub actor_status: ::std::collections::HashMap<u32, table_fragments::ActorStatus>,
}
/// Nested message and enum types in `TableFragments`.
pub mod table_fragments {
    /// Runtime information of an actor
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct ActorStatus {
        /// Current on which node
        #[prost(uint32, tag = "1")]
        pub node_id: u32,
        /// Current state
        #[prost(enumeration = "ActorState", tag = "2")]
        pub state: i32,
    }
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct Fragment {
        #[prost(uint32, tag = "1")]
        pub fragment_id: u32,
        #[prost(enumeration = "fragment::FragmentType", tag = "2")]
        pub fragment_type: i32,
        #[prost(enumeration = "fragment::FragmentDistributionType", tag = "3")]
        pub distribution_type: i32,
        #[prost(message, repeated, tag = "4")]
        pub actors: ::prost::alloc::vec::Vec<super::super::stream_plan::StreamActor>,
    }
    /// Nested message and enum types in `Fragment`.
    pub mod fragment {
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
        pub enum FragmentType {
            Source = 0,
            Sink = 1,
            Others = 2,
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
        pub enum FragmentDistributionType {
            Single = 0,
            Hash = 1,
        }
    }
    /// Current state of actor
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
    pub enum ActorState {
        /// Initial state after creation
        Inactive = 0,
        /// Running normally
        Running = 1,
    }
}
/// TODO: remove this when dashboard refactored.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ActorLocation {
    #[prost(message, optional, tag = "1")]
    pub node: ::core::option::Option<super::common::WorkerNode>,
    #[prost(message, repeated, tag = "2")]
    pub actors: ::prost::alloc::vec::Vec<super::stream_plan::StreamActor>,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedViewRequest {
    #[prost(uint32, tag = "1")]
    pub node_id: u32,
    /// TODO: remove this, do catalog init in Meta and return in response.
    #[prost(message, optional, tag = "2")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(message, optional, tag = "3")]
    pub stream_node: ::core::option::Option<super::stream_plan::StreamNode>,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedViewResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedViewRequest {
    #[prost(uint32, tag = "1")]
    pub node_id: u32,
    #[prost(message, optional, tag = "2")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
}
/// will be deprecated and replaced by DdlService
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedViewResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct FlushRequest {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct FlushResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
// Below for cluster service.

#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AddWorkerNodeRequest {
    #[prost(enumeration = "super::common::WorkerType", tag = "1")]
    pub worker_type: i32,
    #[prost(message, optional, tag = "2")]
    pub host: ::core::option::Option<super::common::HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AddWorkerNodeResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub node: ::core::option::Option<super::common::WorkerNode>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ActivateWorkerNodeRequest {
    #[prost(message, optional, tag = "1")]
    pub host: ::core::option::Option<super::common::HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ActivateWorkerNodeResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DeleteWorkerNodeRequest {
    #[prost(message, optional, tag = "1")]
    pub host: ::core::option::Option<super::common::HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DeleteWorkerNodeResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ListAllNodesRequest {
    #[prost(enumeration = "super::common::WorkerType", tag = "1")]
    pub worker_type: i32,
    /// Whether to include nodes still starting
    #[prost(bool, tag = "2")]
    pub include_starting_nodes: bool,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ListAllNodesResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, repeated, tag = "2")]
    pub nodes: ::prost::alloc::vec::Vec<super::common::WorkerNode>,
}
/// Below for notification service.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SubscribeRequest {
    #[prost(enumeration = "super::common::WorkerType", tag = "1")]
    pub worker_type: i32,
    #[prost(message, optional, tag = "2")]
    pub host: ::core::option::Option<super::common::HostAddress>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct MetaSnapshot {
    #[prost(message, repeated, tag = "1")]
    pub nodes: ::prost::alloc::vec::Vec<super::common::WorkerNode>,
    #[prost(message, repeated, tag = "2")]
    pub database: ::prost::alloc::vec::Vec<super::catalog::Database>,
    #[prost(message, repeated, tag = "3")]
    pub schema: ::prost::alloc::vec::Vec<super::catalog::Schema>,
    #[prost(message, repeated, tag = "4")]
    pub source: ::prost::alloc::vec::Vec<super::catalog::Source>,
    #[prost(message, repeated, tag = "5")]
    pub table: ::prost::alloc::vec::Vec<super::catalog::Table>,
    #[prost(message, repeated, tag = "6")]
    pub view: ::prost::alloc::vec::Vec<super::catalog::VirtualTable>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SubscribeResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(enumeration = "subscribe_response::Operation", tag = "2")]
    pub operation: i32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
    #[prost(
        oneof = "subscribe_response::Info",
        tags = "4, 5, 6, 7, 8, 9, 10, 11, 12"
    )]
    pub info: ::core::option::Option<subscribe_response::Info>,
}
/// Nested message and enum types in `SubscribeResponse`.
pub mod subscribe_response {
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
    pub enum Operation {
        Invalid = 0,
        Add = 1,
        Delete = 2,
        Update = 3,
        Snapshot = 4,
    }
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Oneof)]
    pub enum Info {
        #[prost(message, tag = "4")]
        Node(super::super::common::WorkerNode),
        /// will be deprecated and replaced by database_v2
        #[prost(message, tag = "5")]
        Database(super::Database),
        /// will be deprecated and replaced by schema_v2
        #[prost(message, tag = "6")]
        Schema(super::Schema),
        /// will be deprecated and replaced by table_v2
        #[prost(message, tag = "7")]
        Table(super::Table),
        #[prost(message, tag = "8")]
        DatabaseV2(super::super::catalog::Database),
        #[prost(message, tag = "9")]
        SchemaV2(super::super::catalog::Schema),
        #[prost(message, tag = "10")]
        TableV2(super::super::catalog::Table),
        #[prost(message, tag = "11")]
        Source(super::super::catalog::Source),
        #[prost(message, tag = "12")]
        FeSnapshot(super::MetaSnapshot),
    }
}
#[doc = r" Generated client implementations."]
pub mod epoch_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct EpochServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl EpochServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> EpochServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> EpochServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            EpochServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get_epoch(
            &mut self,
            request: impl tonic::IntoRequest<super::GetEpochRequest>,
        ) -> Result<tonic::Response<super::GetEpochResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.EpochService/GetEpoch");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod catalog_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = " will be deprecated and replaced by DdlService"]
    #[derive(Debug, Clone)]
    pub struct CatalogServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CatalogServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CatalogServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CatalogServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            CatalogServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get_catalog(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCatalogRequest>,
        ) -> Result<tonic::Response<super::GetCatalogResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.CatalogService/GetCatalog");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateRequest>,
        ) -> Result<tonic::Response<super::CreateResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.CatalogService/Create");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop(
            &mut self,
            request: impl tonic::IntoRequest<super::DropRequest>,
        ) -> Result<tonic::Response<super::DropResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.CatalogService/Drop");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod heartbeat_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct HeartbeatServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HeartbeatServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> HeartbeatServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> HeartbeatServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            HeartbeatServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::HeartbeatRequest>,
        ) -> Result<tonic::Response<super::HeartbeatResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.HeartbeatService/Heartbeat");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod stream_manager_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct StreamManagerServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl StreamManagerServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> StreamManagerServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> StreamManagerServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            StreamManagerServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        #[doc = " will be deprecated and replaced by catalog.CreateMaterializedSource and catalog.CreateMaterializedView"]
        pub async fn create_materialized_view(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::CreateMaterializedViewResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/meta.StreamManagerService/CreateMaterializedView",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        #[doc = " will be deprecated and replaced by catalog.DropMaterializedSource and catalog.DropMaterializedView"]
        pub async fn drop_materialized_view(
            &mut self,
            request: impl tonic::IntoRequest<super::DropMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::DropMaterializedViewResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/meta.StreamManagerService/DropMaterializedView",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn flush(
            &mut self,
            request: impl tonic::IntoRequest<super::FlushRequest>,
        ) -> Result<tonic::Response<super::FlushResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.StreamManagerService/Flush");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod cluster_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ClusterServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ClusterServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ClusterServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ClusterServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            ClusterServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn add_worker_node(
            &mut self,
            request: impl tonic::IntoRequest<super::AddWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::AddWorkerNodeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.ClusterService/AddWorkerNode");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn activate_worker_node(
            &mut self,
            request: impl tonic::IntoRequest<super::ActivateWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::ActivateWorkerNodeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/meta.ClusterService/ActivateWorkerNode");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn delete_worker_node(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::DeleteWorkerNodeResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/meta.ClusterService/DeleteWorkerNode");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn list_all_nodes(
            &mut self,
            request: impl tonic::IntoRequest<super::ListAllNodesRequest>,
        ) -> Result<tonic::Response<super::ListAllNodesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.ClusterService/ListAllNodes");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated client implementations."]
pub mod notification_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct NotificationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl NotificationServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> NotificationServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> NotificationServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            NotificationServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn subscribe(
            &mut self,
            request: impl tonic::IntoRequest<super::SubscribeRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::SubscribeResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/meta.NotificationService/Subscribe");
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod epoch_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with EpochServiceServer."]
    #[async_trait]
    pub trait EpochService: Send + Sync + 'static {
        async fn get_epoch(
            &self,
            request: tonic::Request<super::GetEpochRequest>,
        ) -> Result<tonic::Response<super::GetEpochResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct EpochServiceServer<T: EpochService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: EpochService> EpochServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for EpochServiceServer<T>
    where
        T: EpochService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.EpochService/GetEpoch" => {
                    #[allow(non_camel_case_types)]
                    struct GetEpochSvc<T: EpochService>(pub Arc<T>);
                    impl<T: EpochService> tonic::server::UnaryService<super::GetEpochRequest> for GetEpochSvc<T> {
                        type Response = super::GetEpochResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetEpochRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_epoch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetEpochSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: EpochService> Clone for EpochServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: EpochService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: EpochService> tonic::transport::NamedService for EpochServiceServer<T> {
        const NAME: &'static str = "meta.EpochService";
    }
}
#[doc = r" Generated server implementations."]
pub mod catalog_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with CatalogServiceServer."]
    #[async_trait]
    pub trait CatalogService: Send + Sync + 'static {
        async fn get_catalog(
            &self,
            request: tonic::Request<super::GetCatalogRequest>,
        ) -> Result<tonic::Response<super::GetCatalogResponse>, tonic::Status>;
        async fn create(
            &self,
            request: tonic::Request<super::CreateRequest>,
        ) -> Result<tonic::Response<super::CreateResponse>, tonic::Status>;
        async fn drop(
            &self,
            request: tonic::Request<super::DropRequest>,
        ) -> Result<tonic::Response<super::DropResponse>, tonic::Status>;
    }
    #[doc = " will be deprecated and replaced by DdlService"]
    #[derive(Debug)]
    pub struct CatalogServiceServer<T: CatalogService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CatalogService> CatalogServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CatalogServiceServer<T>
    where
        T: CatalogService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.CatalogService/GetCatalog" => {
                    #[allow(non_camel_case_types)]
                    struct GetCatalogSvc<T: CatalogService>(pub Arc<T>);
                    impl<T: CatalogService> tonic::server::UnaryService<super::GetCatalogRequest> for GetCatalogSvc<T> {
                        type Response = super::GetCatalogResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetCatalogRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_catalog(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetCatalogSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.CatalogService/Create" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSvc<T: CatalogService>(pub Arc<T>);
                    impl<T: CatalogService> tonic::server::UnaryService<super::CreateRequest> for CreateSvc<T> {
                        type Response = super::CreateResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.CatalogService/Drop" => {
                    #[allow(non_camel_case_types)]
                    struct DropSvc<T: CatalogService>(pub Arc<T>);
                    impl<T: CatalogService> tonic::server::UnaryService<super::DropRequest> for DropSvc<T> {
                        type Response = super::DropResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: CatalogService> Clone for CatalogServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: CatalogService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CatalogService> tonic::transport::NamedService for CatalogServiceServer<T> {
        const NAME: &'static str = "meta.CatalogService";
    }
}
#[doc = r" Generated server implementations."]
pub mod heartbeat_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with HeartbeatServiceServer."]
    #[async_trait]
    pub trait HeartbeatService: Send + Sync + 'static {
        async fn heartbeat(
            &self,
            request: tonic::Request<super::HeartbeatRequest>,
        ) -> Result<tonic::Response<super::HeartbeatResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct HeartbeatServiceServer<T: HeartbeatService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: HeartbeatService> HeartbeatServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for HeartbeatServiceServer<T>
    where
        T: HeartbeatService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.HeartbeatService/Heartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct HeartbeatSvc<T: HeartbeatService>(pub Arc<T>);
                    impl<T: HeartbeatService> tonic::server::UnaryService<super::HeartbeatRequest> for HeartbeatSvc<T> {
                        type Response = super::HeartbeatResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::HeartbeatRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).heartbeat(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = HeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: HeartbeatService> Clone for HeartbeatServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: HeartbeatService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: HeartbeatService> tonic::transport::NamedService for HeartbeatServiceServer<T> {
        const NAME: &'static str = "meta.HeartbeatService";
    }
}
#[doc = r" Generated server implementations."]
pub mod stream_manager_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with StreamManagerServiceServer."]
    #[async_trait]
    pub trait StreamManagerService: Send + Sync + 'static {
        #[doc = " will be deprecated and replaced by catalog.CreateMaterializedSource and catalog.CreateMaterializedView"]
        async fn create_materialized_view(
            &self,
            request: tonic::Request<super::CreateMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::CreateMaterializedViewResponse>, tonic::Status>;
        #[doc = " will be deprecated and replaced by catalog.DropMaterializedSource and catalog.DropMaterializedView"]
        async fn drop_materialized_view(
            &self,
            request: tonic::Request<super::DropMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::DropMaterializedViewResponse>, tonic::Status>;
        async fn flush(
            &self,
            request: tonic::Request<super::FlushRequest>,
        ) -> Result<tonic::Response<super::FlushResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct StreamManagerServiceServer<T: StreamManagerService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: StreamManagerService> StreamManagerServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for StreamManagerServiceServer<T>
    where
        T: StreamManagerService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.StreamManagerService/CreateMaterializedView" => {
                    #[allow(non_camel_case_types)]
                    struct CreateMaterializedViewSvc<T: StreamManagerService>(pub Arc<T>);
                    impl<T: StreamManagerService>
                        tonic::server::UnaryService<super::CreateMaterializedViewRequest>
                        for CreateMaterializedViewSvc<T>
                    {
                        type Response = super::CreateMaterializedViewResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateMaterializedViewRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).create_materialized_view(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateMaterializedViewSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.StreamManagerService/DropMaterializedView" => {
                    #[allow(non_camel_case_types)]
                    struct DropMaterializedViewSvc<T: StreamManagerService>(pub Arc<T>);
                    impl<T: StreamManagerService>
                        tonic::server::UnaryService<super::DropMaterializedViewRequest>
                        for DropMaterializedViewSvc<T>
                    {
                        type Response = super::DropMaterializedViewResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropMaterializedViewRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_materialized_view(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropMaterializedViewSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.StreamManagerService/Flush" => {
                    #[allow(non_camel_case_types)]
                    struct FlushSvc<T: StreamManagerService>(pub Arc<T>);
                    impl<T: StreamManagerService> tonic::server::UnaryService<super::FlushRequest> for FlushSvc<T> {
                        type Response = super::FlushResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FlushRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).flush(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FlushSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: StreamManagerService> Clone for StreamManagerServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: StreamManagerService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: StreamManagerService> tonic::transport::NamedService for StreamManagerServiceServer<T> {
        const NAME: &'static str = "meta.StreamManagerService";
    }
}
#[doc = r" Generated server implementations."]
pub mod cluster_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with ClusterServiceServer."]
    #[async_trait]
    pub trait ClusterService: Send + Sync + 'static {
        async fn add_worker_node(
            &self,
            request: tonic::Request<super::AddWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::AddWorkerNodeResponse>, tonic::Status>;
        async fn activate_worker_node(
            &self,
            request: tonic::Request<super::ActivateWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::ActivateWorkerNodeResponse>, tonic::Status>;
        async fn delete_worker_node(
            &self,
            request: tonic::Request<super::DeleteWorkerNodeRequest>,
        ) -> Result<tonic::Response<super::DeleteWorkerNodeResponse>, tonic::Status>;
        async fn list_all_nodes(
            &self,
            request: tonic::Request<super::ListAllNodesRequest>,
        ) -> Result<tonic::Response<super::ListAllNodesResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ClusterServiceServer<T: ClusterService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ClusterService> ClusterServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ClusterServiceServer<T>
    where
        T: ClusterService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.ClusterService/AddWorkerNode" => {
                    #[allow(non_camel_case_types)]
                    struct AddWorkerNodeSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService> tonic::server::UnaryService<super::AddWorkerNodeRequest>
                        for AddWorkerNodeSvc<T>
                    {
                        type Response = super::AddWorkerNodeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddWorkerNodeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).add_worker_node(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddWorkerNodeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.ClusterService/ActivateWorkerNode" => {
                    #[allow(non_camel_case_types)]
                    struct ActivateWorkerNodeSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService>
                        tonic::server::UnaryService<super::ActivateWorkerNodeRequest>
                        for ActivateWorkerNodeSvc<T>
                    {
                        type Response = super::ActivateWorkerNodeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ActivateWorkerNodeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).activate_worker_node(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ActivateWorkerNodeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.ClusterService/DeleteWorkerNode" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteWorkerNodeSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService>
                        tonic::server::UnaryService<super::DeleteWorkerNodeRequest>
                        for DeleteWorkerNodeSvc<T>
                    {
                        type Response = super::DeleteWorkerNodeResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteWorkerNodeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).delete_worker_node(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeleteWorkerNodeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/meta.ClusterService/ListAllNodes" => {
                    #[allow(non_camel_case_types)]
                    struct ListAllNodesSvc<T: ClusterService>(pub Arc<T>);
                    impl<T: ClusterService> tonic::server::UnaryService<super::ListAllNodesRequest>
                        for ListAllNodesSvc<T>
                    {
                        type Response = super::ListAllNodesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListAllNodesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).list_all_nodes(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ListAllNodesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ClusterService> Clone for ClusterServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: ClusterService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ClusterService> tonic::transport::NamedService for ClusterServiceServer<T> {
        const NAME: &'static str = "meta.ClusterService";
    }
}
#[doc = r" Generated server implementations."]
pub mod notification_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with NotificationServiceServer."]
    #[async_trait]
    pub trait NotificationService: Send + Sync + 'static {
        #[doc = "Server streaming response type for the Subscribe method."]
        type SubscribeStream: futures_core::Stream<Item = Result<super::SubscribeResponse, tonic::Status>>
            + Send
            + 'static;
        async fn subscribe(
            &self,
            request: tonic::Request<super::SubscribeRequest>,
        ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct NotificationServiceServer<T: NotificationService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: NotificationService> NotificationServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for NotificationServiceServer<T>
    where
        T: NotificationService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/meta.NotificationService/Subscribe" => {
                    #[allow(non_camel_case_types)]
                    struct SubscribeSvc<T: NotificationService>(pub Arc<T>);
                    impl<T: NotificationService>
                        tonic::server::ServerStreamingService<super::SubscribeRequest>
                        for SubscribeSvc<T>
                    {
                        type Response = super::SubscribeResponse;
                        type ResponseStream = T::SubscribeStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SubscribeRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).subscribe(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SubscribeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.server_streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: NotificationService> Clone for NotificationServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: NotificationService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: NotificationService> tonic::transport::NamedService for NotificationServiceServer<T> {
        const NAME: &'static str = "meta.NotificationService";
    }
}
