#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateDatabaseRequest {
    #[prost(message, optional, tag = "1")]
    pub db: ::core::option::Option<super::catalog::Database>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateDatabaseResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint32, tag = "2")]
    pub database_id: u32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropDatabaseRequest {
    #[prost(uint32, tag = "1")]
    pub database_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropDatabaseResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateSchemaRequest {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<super::catalog::Schema>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateSchemaResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint32, tag = "2")]
    pub schema_id: u32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropSchemaRequest {
    #[prost(uint32, tag = "1")]
    pub schema_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropSchemaResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub source: ::core::option::Option<super::catalog::Source>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateSourceResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint32, tag = "2")]
    pub source_id: u32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropSourceRequest {
    #[prost(uint32, tag = "1")]
    pub source_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropSourceResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedViewRequest {
    #[prost(message, optional, tag = "1")]
    pub materialized_view: ::core::option::Option<super::catalog::Table>,
    #[prost(message, optional, tag = "2")]
    pub stream_node: ::core::option::Option<super::stream_plan::StreamNode>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedViewResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint32, tag = "2")]
    pub table_id: u32,
    #[prost(uint64, tag = "3")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedViewRequest {
    #[prost(uint32, tag = "1")]
    pub table_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedViewResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedSourceRequest {
    #[prost(message, optional, tag = "1")]
    pub source: ::core::option::Option<super::catalog::Source>,
    #[prost(message, optional, tag = "2")]
    pub materialized_view: ::core::option::Option<super::catalog::Table>,
    #[prost(message, optional, tag = "3")]
    pub stream_node: ::core::option::Option<super::stream_plan::StreamNode>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct CreateMaterializedSourceResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint32, tag = "2")]
    pub source_id: u32,
    #[prost(uint32, tag = "3")]
    pub table_id: u32,
    #[prost(uint64, tag = "4")]
    pub version: u64,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedSourceRequest {
    #[prost(uint32, tag = "1")]
    pub source_id: u32,
    #[prost(uint32, tag = "2")]
    pub table_id: u32,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropMaterializedSourceResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
    #[prost(uint64, tag = "2")]
    pub version: u64,
}
/// Generated client implementations.
pub mod ddl_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct DdlServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl DdlServiceClient<tonic::transport::Channel> {
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
    impl<T> DdlServiceClient<T>
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
        ) -> DdlServiceClient<InterceptedService<T, F>>
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
            DdlServiceClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn create_database(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateDatabaseRequest>,
        ) -> Result<tonic::Response<super::CreateDatabaseResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/ddl_service.DdlService/CreateDatabase");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop_database(
            &mut self,
            request: impl tonic::IntoRequest<super::DropDatabaseRequest>,
        ) -> Result<tonic::Response<super::DropDatabaseResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/ddl_service.DdlService/DropDatabase");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSchemaRequest>,
        ) -> Result<tonic::Response<super::CreateSchemaResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/ddl_service.DdlService/CreateSchema");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop_schema(
            &mut self,
            request: impl tonic::IntoRequest<super::DropSchemaRequest>,
        ) -> Result<tonic::Response<super::DropSchemaResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/ddl_service.DdlService/DropSchema");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_source(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateSourceRequest>,
        ) -> Result<tonic::Response<super::CreateSourceResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/ddl_service.DdlService/CreateSource");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop_source(
            &mut self,
            request: impl tonic::IntoRequest<super::DropSourceRequest>,
        ) -> Result<tonic::Response<super::DropSourceResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/ddl_service.DdlService/DropSource");
            self.inner.unary(request.into_request(), path, codec).await
        }
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
                "/ddl_service.DdlService/CreateMaterializedView",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
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
                "/ddl_service.DdlService/DropMaterializedView",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_materialized_source(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateMaterializedSourceRequest>,
        ) -> Result<tonic::Response<super::CreateMaterializedSourceResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ddl_service.DdlService/CreateMaterializedSource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop_materialized_source(
            &mut self,
            request: impl tonic::IntoRequest<super::DropMaterializedSourceRequest>,
        ) -> Result<tonic::Response<super::DropMaterializedSourceResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/ddl_service.DdlService/DropMaterializedSource",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod ddl_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with
    /// DdlServiceServer.
    #[async_trait]
    pub trait DdlService: Send + Sync + 'static {
        async fn create_database(
            &self,
            request: tonic::Request<super::CreateDatabaseRequest>,
        ) -> Result<tonic::Response<super::CreateDatabaseResponse>, tonic::Status>;
        async fn drop_database(
            &self,
            request: tonic::Request<super::DropDatabaseRequest>,
        ) -> Result<tonic::Response<super::DropDatabaseResponse>, tonic::Status>;
        async fn create_schema(
            &self,
            request: tonic::Request<super::CreateSchemaRequest>,
        ) -> Result<tonic::Response<super::CreateSchemaResponse>, tonic::Status>;
        async fn drop_schema(
            &self,
            request: tonic::Request<super::DropSchemaRequest>,
        ) -> Result<tonic::Response<super::DropSchemaResponse>, tonic::Status>;
        async fn create_source(
            &self,
            request: tonic::Request<super::CreateSourceRequest>,
        ) -> Result<tonic::Response<super::CreateSourceResponse>, tonic::Status>;
        async fn drop_source(
            &self,
            request: tonic::Request<super::DropSourceRequest>,
        ) -> Result<tonic::Response<super::DropSourceResponse>, tonic::Status>;
        async fn create_materialized_view(
            &self,
            request: tonic::Request<super::CreateMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::CreateMaterializedViewResponse>, tonic::Status>;
        async fn drop_materialized_view(
            &self,
            request: tonic::Request<super::DropMaterializedViewRequest>,
        ) -> Result<tonic::Response<super::DropMaterializedViewResponse>, tonic::Status>;
        async fn create_materialized_source(
            &self,
            request: tonic::Request<super::CreateMaterializedSourceRequest>,
        ) -> Result<tonic::Response<super::CreateMaterializedSourceResponse>, tonic::Status>;
        async fn drop_materialized_source(
            &self,
            request: tonic::Request<super::DropMaterializedSourceRequest>,
        ) -> Result<tonic::Response<super::DropMaterializedSourceResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct DdlServiceServer<T: DdlService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: DdlService> DdlServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for DdlServiceServer<T>
    where
        T: DdlService,
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
                "/ddl_service.DdlService/CreateDatabase" => {
                    #[allow(non_camel_case_types)]
                    struct CreateDatabaseSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::CreateDatabaseRequest>
                        for CreateDatabaseSvc<T>
                    {
                        type Response = super::CreateDatabaseResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateDatabaseRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_database(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateDatabaseSvc(inner);
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
                "/ddl_service.DdlService/DropDatabase" => {
                    #[allow(non_camel_case_types)]
                    struct DropDatabaseSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::DropDatabaseRequest> for DropDatabaseSvc<T> {
                        type Response = super::DropDatabaseResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropDatabaseRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_database(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropDatabaseSvc(inner);
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
                "/ddl_service.DdlService/CreateSchema" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSchemaSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::CreateSchemaRequest> for CreateSchemaSvc<T> {
                        type Response = super::CreateSchemaResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSchemaRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_schema(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSchemaSvc(inner);
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
                "/ddl_service.DdlService/DropSchema" => {
                    #[allow(non_camel_case_types)]
                    struct DropSchemaSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::DropSchemaRequest> for DropSchemaSvc<T> {
                        type Response = super::DropSchemaResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropSchemaRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_schema(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropSchemaSvc(inner);
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
                "/ddl_service.DdlService/CreateSource" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSourceSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::CreateSourceRequest> for CreateSourceSvc<T> {
                        type Response = super::CreateSourceResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateSourceSvc(inner);
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
                "/ddl_service.DdlService/DropSource" => {
                    #[allow(non_camel_case_types)]
                    struct DropSourceSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService> tonic::server::UnaryService<super::DropSourceRequest> for DropSourceSvc<T> {
                        type Response = super::DropSourceResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropSourceSvc(inner);
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
                "/ddl_service.DdlService/CreateMaterializedView" => {
                    #[allow(non_camel_case_types)]
                    struct CreateMaterializedViewSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService>
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
                "/ddl_service.DdlService/DropMaterializedView" => {
                    #[allow(non_camel_case_types)]
                    struct DropMaterializedViewSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService>
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
                "/ddl_service.DdlService/CreateMaterializedSource" => {
                    #[allow(non_camel_case_types)]
                    struct CreateMaterializedSourceSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService>
                        tonic::server::UnaryService<super::CreateMaterializedSourceRequest>
                        for CreateMaterializedSourceSvc<T>
                    {
                        type Response = super::CreateMaterializedSourceResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateMaterializedSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).create_materialized_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateMaterializedSourceSvc(inner);
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
                "/ddl_service.DdlService/DropMaterializedSource" => {
                    #[allow(non_camel_case_types)]
                    struct DropMaterializedSourceSvc<T: DdlService>(pub Arc<T>);
                    impl<T: DdlService>
                        tonic::server::UnaryService<super::DropMaterializedSourceRequest>
                        for DropMaterializedSourceSvc<T>
                    {
                        type Response = super::DropMaterializedSourceResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropMaterializedSourceRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).drop_materialized_source(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropMaterializedSourceSvc(inner);
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
    impl<T: DdlService> Clone for DdlServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: DdlService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: DdlService> tonic::transport::NamedService for DdlServiceServer<T> {
        const NAME: &'static str = "ddl_service.DdlService";
    }
}
