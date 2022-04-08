#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SstableRefId {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SstableIdInfo {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Timestamp when the sstable id is created, in seconds.
    #[prost(uint64, tag = "2")]
    pub id_create_timestamp: u64,
    /// Timestamp when the sstable is tracked in meta node, in seconds.
    #[prost(uint64, tag = "3")]
    pub meta_create_timestamp: u64,
    /// Timestamp when the sstable is marked to delete, in seconds.
    #[prost(uint64, tag = "4")]
    pub meta_delete_timestamp: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SstableInfo {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(message, optional, tag = "2")]
    pub key_range: ::core::option::Option<KeyRange>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct Level {
    #[prost(enumeration = "LevelType", tag = "1")]
    pub level_type: i32,
    #[prost(uint64, repeated, tag = "2")]
    pub table_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UncommittedEpoch {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    /// We store SstableInfo here because key range information is needed to update
    /// CompactStatus.
    /// TODO: should we combine CompactStatus with HummockVersion?
    #[prost(message, repeated, tag = "2")]
    pub tables: ::prost::alloc::vec::Vec<SstableInfo>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockVersionRefId {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockVersion {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(message, repeated, tag = "2")]
    pub levels: ::prost::alloc::vec::Vec<Level>,
    #[prost(message, repeated, tag = "3")]
    pub uncommitted_epochs: ::prost::alloc::vec::Vec<UncommittedEpoch>,
    #[prost(uint64, tag = "4")]
    pub max_committed_epoch: u64,
    /// Snapshots with epoch less than the safe epoch have been GCed.
    /// Reads against such an epoch will fail.
    #[prost(uint64, tag = "5")]
    pub safe_epoch: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockSnapshot {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AddTablesRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(message, repeated, tag = "2")]
    pub tables: ::prost::alloc::vec::Vec<SstableInfo>,
    #[prost(uint64, tag = "3")]
    pub epoch: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AddTablesResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub version: ::core::option::Option<HummockVersion>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PinVersionRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(uint64, tag = "2")]
    pub last_pinned: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PinVersionResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub pinned_version: ::core::option::Option<HummockVersion>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UnpinVersionRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(uint64, repeated, tag = "2")]
    pub pinned_version_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UnpinVersionResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PinSnapshotRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(uint64, tag = "2")]
    pub last_pinned: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct PinSnapshotResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub snapshot: ::core::option::Option<HummockSnapshot>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UnpinSnapshotRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(message, repeated, tag = "2")]
    pub snapshots: ::prost::alloc::vec::Vec<HummockSnapshot>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UnpinSnapshotResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct LevelEntry {
    #[prost(uint32, tag = "1")]
    pub level_idx: u32,
    #[prost(message, optional, tag = "2")]
    pub level: ::core::option::Option<Level>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct KeyRange {
    #[prost(bytes = "vec", tag = "1")]
    pub left: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub right: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "3")]
    pub inf: bool,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct TableSetStatistics {
    #[prost(uint32, tag = "1")]
    pub level_idx: u32,
    #[prost(double, tag = "2")]
    pub size_gb: f64,
    #[prost(uint64, tag = "3")]
    pub cnt: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CompactMetrics {
    #[prost(message, optional, tag = "1")]
    pub read_level_n: ::core::option::Option<TableSetStatistics>,
    #[prost(message, optional, tag = "2")]
    pub read_level_nplus1: ::core::option::Option<TableSetStatistics>,
    #[prost(message, optional, tag = "3")]
    pub write: ::core::option::Option<TableSetStatistics>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CompactTask {
    /// SSTs to be compacted, which will be removed from LSM after compaction
    #[prost(message, repeated, tag = "1")]
    pub input_ssts: ::prost::alloc::vec::Vec<LevelEntry>,
    /// In ideal case, the compaction will generate `splits.len()` tables which have key range
    /// corresponding to that in \[`splits`\], respectively
    #[prost(message, repeated, tag = "2")]
    pub splits: ::prost::alloc::vec::Vec<KeyRange>,
    /// low watermark in 'ts-aware compaction'
    #[prost(uint64, tag = "3")]
    pub watermark: u64,
    /// compacion output, which will be added to \[`target_level`\] of LSM after compaction
    #[prost(message, repeated, tag = "4")]
    pub sorted_output_ssts: ::prost::alloc::vec::Vec<SstableInfo>,
    /// task id assigned by hummock storage service
    #[prost(uint64, tag = "5")]
    pub task_id: u64,
    /// compacion output will be added to \[`target_level`\] of LSM after compaction
    #[prost(uint32, tag = "6")]
    pub target_level: u32,
    #[prost(bool, tag = "7")]
    pub is_target_ultimate_and_leveling: bool,
    #[prost(message, optional, tag = "8")]
    pub metrics: ::core::option::Option<CompactMetrics>,
    #[prost(bool, tag = "9")]
    pub task_status: bool,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SstableStat {
    #[prost(message, optional, tag = "1")]
    pub key_range: ::core::option::Option<KeyRange>,
    #[prost(uint64, tag = "2")]
    pub table_id: u64,
    #[prost(message, optional, tag = "3")]
    pub compact_task: ::core::option::Option<sstable_stat::CompactTaskId>,
}
/// Nested message and enum types in `SstableStat`.
pub mod sstable_stat {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct CompactTaskId {
        #[prost(uint64, tag = "1")]
        pub id: u64,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct LevelHandler {
    #[prost(enumeration = "LevelType", tag = "1")]
    pub level_type: i32,
    #[prost(message, repeated, tag = "2")]
    pub ssts: ::prost::alloc::vec::Vec<SstableStat>,
    #[prost(message, repeated, tag = "3")]
    pub key_ranges: ::prost::alloc::vec::Vec<level_handler::KeyRangeTaskId>,
}
/// Nested message and enum types in `LevelHandler`.
pub mod level_handler {
    #[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
    pub struct KeyRangeTaskId {
        #[prost(message, optional, tag = "1")]
        pub key_range: ::core::option::Option<super::KeyRange>,
        #[prost(uint64, tag = "2")]
        pub task_id: u64,
        #[prost(uint64, tag = "3")]
        pub ssts: u64,
    }
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CompactStatus {
    #[prost(message, repeated, tag = "1")]
    pub level_handlers: ::prost::alloc::vec::Vec<LevelHandler>,
    #[prost(uint64, tag = "2")]
    pub next_compact_task_id: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CompactTaskAssignment {
    #[prost(message, optional, tag = "1")]
    pub compact_task: ::core::option::Option<CompactTask>,
    #[prost(uint32, tag = "2")]
    pub context_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CompactTaskRefId {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetCompactionTasksRequest {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetCompactionTasksResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(message, optional, tag = "2")]
    pub compact_task: ::core::option::Option<CompactTask>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ReportCompactionTasksRequest {
    #[prost(message, optional, tag = "1")]
    pub compact_task: ::core::option::Option<CompactTask>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ReportCompactionTasksResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockContextRefId {
    #[prost(uint32, tag = "1")]
    pub id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockPinnedVersion {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(uint64, repeated, tag = "2")]
    pub version_id: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockPinnedSnapshot {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
    #[prost(uint64, repeated, tag = "2")]
    pub snapshot_id: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HummockStaleSstables {
    #[prost(uint64, tag = "1")]
    pub version_id: u64,
    /// sstable ids
    #[prost(uint64, repeated, tag = "2")]
    pub id: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CommitEpochRequest {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CommitEpochResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AbortEpochRequest {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct AbortEpochResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetNewTableIdRequest {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct GetNewTableIdResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub table_id: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SubscribeCompactTasksRequest {
    #[prost(uint32, tag = "1")]
    pub context_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct SubscribeCompactTasksResponse {
    #[prost(message, optional, tag = "1")]
    pub compact_task: ::core::option::Option<CompactTask>,
    #[prost(message, optional, tag = "2")]
    pub vacuum_task: ::core::option::Option<VacuumTask>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct VacuumTask {
    #[prost(uint64, repeated, tag = "1")]
    pub sstable_ids: ::prost::alloc::vec::Vec<u64>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ReportVacuumTaskRequest {
    #[prost(message, optional, tag = "1")]
    pub vacuum_task: ::core::option::Option<VacuumTask>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ReportVacuumTaskResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
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
pub enum LevelType {
    Nonoverlapping = 0,
    Overlapping = 1,
}
/// Generated client implementations.
pub mod hummock_manager_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct HummockManagerServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HummockManagerServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> HummockManagerServiceClient<T>
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
        ) -> HummockManagerServiceClient<InterceptedService<T, F>>
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
            HummockManagerServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn pin_version(
            &mut self,
            request: impl tonic::IntoRequest<super::PinVersionRequest>,
        ) -> Result<tonic::Response<super::PinVersionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/PinVersion");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn unpin_version(
            &mut self,
            request: impl tonic::IntoRequest<super::UnpinVersionRequest>,
        ) -> Result<tonic::Response<super::UnpinVersionResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/UnpinVersion");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn add_tables(
            &mut self,
            request: impl tonic::IntoRequest<super::AddTablesRequest>,
        ) -> Result<tonic::Response<super::AddTablesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/AddTables");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn report_compaction_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportCompactionTasksRequest>,
        ) -> Result<tonic::Response<super::ReportCompactionTasksResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/hummock.HummockManagerService/ReportCompactionTasks",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn pin_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::PinSnapshotRequest>,
        ) -> Result<tonic::Response<super::PinSnapshotResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/PinSnapshot");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn unpin_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::UnpinSnapshotRequest>,
        ) -> Result<tonic::Response<super::UnpinSnapshotResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/hummock.HummockManagerService/UnpinSnapshot",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn commit_epoch(
            &mut self,
            request: impl tonic::IntoRequest<super::CommitEpochRequest>,
        ) -> Result<tonic::Response<super::CommitEpochResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/CommitEpoch");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn abort_epoch(
            &mut self,
            request: impl tonic::IntoRequest<super::AbortEpochRequest>,
        ) -> Result<tonic::Response<super::AbortEpochResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/hummock.HummockManagerService/AbortEpoch");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_new_table_id(
            &mut self,
            request: impl tonic::IntoRequest<super::GetNewTableIdRequest>,
        ) -> Result<tonic::Response<super::GetNewTableIdResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/hummock.HummockManagerService/GetNewTableId",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn subscribe_compact_tasks(
            &mut self,
            request: impl tonic::IntoRequest<super::SubscribeCompactTasksRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::SubscribeCompactTasksResponse>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/hummock.HummockManagerService/SubscribeCompactTasks",
            );
            self.inner
                .server_streaming(request.into_request(), path, codec)
                .await
        }
        pub async fn report_vacuum_task(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportVacuumTaskRequest>,
        ) -> Result<tonic::Response<super::ReportVacuumTaskResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/hummock.HummockManagerService/ReportVacuumTask",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod hummock_manager_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with
    /// HummockManagerServiceServer.
    #[async_trait]
    pub trait HummockManagerService: Send + Sync + 'static {
        async fn pin_version(
            &self,
            request: tonic::Request<super::PinVersionRequest>,
        ) -> Result<tonic::Response<super::PinVersionResponse>, tonic::Status>;
        async fn unpin_version(
            &self,
            request: tonic::Request<super::UnpinVersionRequest>,
        ) -> Result<tonic::Response<super::UnpinVersionResponse>, tonic::Status>;
        async fn add_tables(
            &self,
            request: tonic::Request<super::AddTablesRequest>,
        ) -> Result<tonic::Response<super::AddTablesResponse>, tonic::Status>;
        async fn report_compaction_tasks(
            &self,
            request: tonic::Request<super::ReportCompactionTasksRequest>,
        ) -> Result<tonic::Response<super::ReportCompactionTasksResponse>, tonic::Status>;
        async fn pin_snapshot(
            &self,
            request: tonic::Request<super::PinSnapshotRequest>,
        ) -> Result<tonic::Response<super::PinSnapshotResponse>, tonic::Status>;
        async fn unpin_snapshot(
            &self,
            request: tonic::Request<super::UnpinSnapshotRequest>,
        ) -> Result<tonic::Response<super::UnpinSnapshotResponse>, tonic::Status>;
        async fn commit_epoch(
            &self,
            request: tonic::Request<super::CommitEpochRequest>,
        ) -> Result<tonic::Response<super::CommitEpochResponse>, tonic::Status>;
        async fn abort_epoch(
            &self,
            request: tonic::Request<super::AbortEpochRequest>,
        ) -> Result<tonic::Response<super::AbortEpochResponse>, tonic::Status>;
        async fn get_new_table_id(
            &self,
            request: tonic::Request<super::GetNewTableIdRequest>,
        ) -> Result<tonic::Response<super::GetNewTableIdResponse>, tonic::Status>;
        /// Server streaming response type for the SubscribeCompactTasks method.
        type SubscribeCompactTasksStream: futures_core::Stream<Item = Result<super::SubscribeCompactTasksResponse, tonic::Status>>
            + Send
            + 'static;
        async fn subscribe_compact_tasks(
            &self,
            request: tonic::Request<super::SubscribeCompactTasksRequest>,
        ) -> Result<tonic::Response<Self::SubscribeCompactTasksStream>, tonic::Status>;
        async fn report_vacuum_task(
            &self,
            request: tonic::Request<super::ReportVacuumTaskRequest>,
        ) -> Result<tonic::Response<super::ReportVacuumTaskResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct HummockManagerServiceServer<T: HummockManagerService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: HummockManagerService> HummockManagerServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for HummockManagerServiceServer<T>
    where
        T: HummockManagerService,
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
                "/hummock.HummockManagerService/PinVersion" => {
                    #[allow(non_camel_case_types)]
                    struct PinVersionSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::PinVersionRequest> for PinVersionSvc<T>
                    {
                        type Response = super::PinVersionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PinVersionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).pin_version(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PinVersionSvc(inner);
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
                "/hummock.HummockManagerService/UnpinVersion" => {
                    #[allow(non_camel_case_types)]
                    struct UnpinVersionSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::UnpinVersionRequest>
                        for UnpinVersionSvc<T>
                    {
                        type Response = super::UnpinVersionResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UnpinVersionRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).unpin_version(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UnpinVersionSvc(inner);
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
                "/hummock.HummockManagerService/AddTables" => {
                    #[allow(non_camel_case_types)]
                    struct AddTablesSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::AddTablesRequest> for AddTablesSvc<T>
                    {
                        type Response = super::AddTablesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AddTablesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).add_tables(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AddTablesSvc(inner);
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
                "/hummock.HummockManagerService/ReportCompactionTasks" => {
                    #[allow(non_camel_case_types)]
                    struct ReportCompactionTasksSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::ReportCompactionTasksRequest>
                        for ReportCompactionTasksSvc<T>
                    {
                        type Response = super::ReportCompactionTasksResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportCompactionTasksRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).report_compaction_tasks(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReportCompactionTasksSvc(inner);
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
                "/hummock.HummockManagerService/PinSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct PinSnapshotSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::PinSnapshotRequest>
                        for PinSnapshotSvc<T>
                    {
                        type Response = super::PinSnapshotResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PinSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).pin_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PinSnapshotSvc(inner);
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
                "/hummock.HummockManagerService/UnpinSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct UnpinSnapshotSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::UnpinSnapshotRequest>
                        for UnpinSnapshotSvc<T>
                    {
                        type Response = super::UnpinSnapshotResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UnpinSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).unpin_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UnpinSnapshotSvc(inner);
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
                "/hummock.HummockManagerService/CommitEpoch" => {
                    #[allow(non_camel_case_types)]
                    struct CommitEpochSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::CommitEpochRequest>
                        for CommitEpochSvc<T>
                    {
                        type Response = super::CommitEpochResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CommitEpochRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).commit_epoch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CommitEpochSvc(inner);
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
                "/hummock.HummockManagerService/AbortEpoch" => {
                    #[allow(non_camel_case_types)]
                    struct AbortEpochSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::AbortEpochRequest> for AbortEpochSvc<T>
                    {
                        type Response = super::AbortEpochResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AbortEpochRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).abort_epoch(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AbortEpochSvc(inner);
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
                "/hummock.HummockManagerService/GetNewTableId" => {
                    #[allow(non_camel_case_types)]
                    struct GetNewTableIdSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::GetNewTableIdRequest>
                        for GetNewTableIdSvc<T>
                    {
                        type Response = super::GetNewTableIdResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetNewTableIdRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_new_table_id(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetNewTableIdSvc(inner);
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
                "/hummock.HummockManagerService/SubscribeCompactTasks" => {
                    #[allow(non_camel_case_types)]
                    struct SubscribeCompactTasksSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::ServerStreamingService<super::SubscribeCompactTasksRequest>
                        for SubscribeCompactTasksSvc<T>
                    {
                        type Response = super::SubscribeCompactTasksResponse;
                        type ResponseStream = T::SubscribeCompactTasksStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SubscribeCompactTasksRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).subscribe_compact_tasks(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = SubscribeCompactTasksSvc(inner);
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
                "/hummock.HummockManagerService/ReportVacuumTask" => {
                    #[allow(non_camel_case_types)]
                    struct ReportVacuumTaskSvc<T: HummockManagerService>(pub Arc<T>);
                    impl<T: HummockManagerService>
                        tonic::server::UnaryService<super::ReportVacuumTaskRequest>
                        for ReportVacuumTaskSvc<T>
                    {
                        type Response = super::ReportVacuumTaskResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportVacuumTaskRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).report_vacuum_task(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReportVacuumTaskSvc(inner);
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
    impl<T: HummockManagerService> Clone for HummockManagerServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: HummockManagerService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: HummockManagerService> tonic::transport::NamedService for HummockManagerServiceServer<T> {
        const NAME: &'static str = "hummock.HummockManagerService";
    }
}
