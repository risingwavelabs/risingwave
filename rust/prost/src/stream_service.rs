#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct HangingChannel {
    #[prost(message, optional, tag = "1")]
    pub upstream: ::core::option::Option<super::common::ActorInfo>,
    #[prost(message, optional, tag = "2")]
    pub downstream: ::core::option::Option<super::common::ActorInfo>,
}
/// Describe the fragments which will be running on this node
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UpdateActorsRequest {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub actors: ::prost::alloc::vec::Vec<super::stream_plan::StreamActor>,
    #[prost(message, repeated, tag = "3")]
    pub hanging_channels: ::prost::alloc::vec::Vec<HangingChannel>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct UpdateActorsResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct BroadcastActorInfoTableRequest {
    #[prost(message, repeated, tag = "1")]
    pub info: ::prost::alloc::vec::Vec<super::common::ActorInfo>,
}
/// Create channels and gRPC connections for a fragment
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct BuildActorsRequest {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(uint32, repeated, tag = "2")]
    pub actor_id: ::prost::alloc::vec::Vec<u32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct BuildActorsResponse {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropActorsRequest {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub table_ref_id: ::core::option::Option<super::plan::TableRefId>,
    #[prost(uint32, repeated, tag = "3")]
    pub actor_ids: ::prost::alloc::vec::Vec<u32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct DropActorsResponse {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub status: ::core::option::Option<super::common::Status>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct InjectBarrierRequest {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub barrier: ::core::option::Option<super::data::Barrier>,
    #[prost(uint32, repeated, tag = "3")]
    pub actor_ids_to_send: ::prost::alloc::vec::Vec<u32>,
    #[prost(uint32, repeated, tag = "4")]
    pub actor_ids_to_collect: ::prost::alloc::vec::Vec<u32>,
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct InjectBarrierResponse {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub status: ::core::option::Option<super::common::Status>,
}
/// Before starting streaming, the leader node broadcast the actor-host table to needed workers.
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct BroadcastActorInfoTableResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
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
}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ShutdownRequest {}
#[derive(prost_helpers::AnyPB, Clone, PartialEq, ::prost::Message)]
pub struct ShutdownResponse {
    #[prost(message, optional, tag = "1")]
    pub status: ::core::option::Option<super::common::Status>,
}
/// Generated client implementations.
pub mod stream_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct StreamServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl StreamServiceClient<tonic::transport::Channel> {
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
    impl<T> StreamServiceClient<T>
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
        ) -> StreamServiceClient<InterceptedService<T, F>>
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
            StreamServiceClient::new(InterceptedService::new(inner, interceptor))
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
        pub async fn update_actors(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateActorsRequest>,
        ) -> Result<tonic::Response<super::UpdateActorsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/UpdateActors");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn build_actors(
            &mut self,
            request: impl tonic::IntoRequest<super::BuildActorsRequest>,
        ) -> Result<tonic::Response<super::BuildActorsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/BuildActors");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn broadcast_actor_info_table(
            &mut self,
            request: impl tonic::IntoRequest<super::BroadcastActorInfoTableRequest>,
        ) -> Result<tonic::Response<super::BroadcastActorInfoTableResponse>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/stream_service.StreamService/BroadcastActorInfoTable",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn drop_actors(
            &mut self,
            request: impl tonic::IntoRequest<super::DropActorsRequest>,
        ) -> Result<tonic::Response<super::DropActorsResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/DropActors");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn inject_barrier(
            &mut self,
            request: impl tonic::IntoRequest<super::InjectBarrierRequest>,
        ) -> Result<tonic::Response<super::InjectBarrierResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/InjectBarrier");
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
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/CreateSource");
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
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/DropSource");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn shutdown(
            &mut self,
            request: impl tonic::IntoRequest<super::ShutdownRequest>,
        ) -> Result<tonic::Response<super::ShutdownResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path =
                http::uri::PathAndQuery::from_static("/stream_service.StreamService/Shutdown");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod stream_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with
    /// StreamServiceServer.
    #[async_trait]
    pub trait StreamService: Send + Sync + 'static {
        async fn update_actors(
            &self,
            request: tonic::Request<super::UpdateActorsRequest>,
        ) -> Result<tonic::Response<super::UpdateActorsResponse>, tonic::Status>;
        async fn build_actors(
            &self,
            request: tonic::Request<super::BuildActorsRequest>,
        ) -> Result<tonic::Response<super::BuildActorsResponse>, tonic::Status>;
        async fn broadcast_actor_info_table(
            &self,
            request: tonic::Request<super::BroadcastActorInfoTableRequest>,
        ) -> Result<tonic::Response<super::BroadcastActorInfoTableResponse>, tonic::Status>;
        async fn drop_actors(
            &self,
            request: tonic::Request<super::DropActorsRequest>,
        ) -> Result<tonic::Response<super::DropActorsResponse>, tonic::Status>;
        async fn inject_barrier(
            &self,
            request: tonic::Request<super::InjectBarrierRequest>,
        ) -> Result<tonic::Response<super::InjectBarrierResponse>, tonic::Status>;
        async fn create_source(
            &self,
            request: tonic::Request<super::CreateSourceRequest>,
        ) -> Result<tonic::Response<super::CreateSourceResponse>, tonic::Status>;
        async fn drop_source(
            &self,
            request: tonic::Request<super::DropSourceRequest>,
        ) -> Result<tonic::Response<super::DropSourceResponse>, tonic::Status>;
        async fn shutdown(
            &self,
            request: tonic::Request<super::ShutdownRequest>,
        ) -> Result<tonic::Response<super::ShutdownResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct StreamServiceServer<T: StreamService> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: StreamService> StreamServiceServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for StreamServiceServer<T>
    where
        T: StreamService,
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
                "/stream_service.StreamService/UpdateActors" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateActorsSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::UpdateActorsRequest>
                        for UpdateActorsSvc<T>
                    {
                        type Response = super::UpdateActorsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateActorsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).update_actors(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = UpdateActorsSvc(inner);
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
                "/stream_service.StreamService/BuildActors" => {
                    #[allow(non_camel_case_types)]
                    struct BuildActorsSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::BuildActorsRequest>
                        for BuildActorsSvc<T>
                    {
                        type Response = super::BuildActorsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BuildActorsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).build_actors(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BuildActorsSvc(inner);
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
                "/stream_service.StreamService/BroadcastActorInfoTable" => {
                    #[allow(non_camel_case_types)]
                    struct BroadcastActorInfoTableSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService>
                        tonic::server::UnaryService<super::BroadcastActorInfoTableRequest>
                        for BroadcastActorInfoTableSvc<T>
                    {
                        type Response = super::BroadcastActorInfoTableResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::BroadcastActorInfoTableRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut =
                                async move { (*inner).broadcast_actor_info_table(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = BroadcastActorInfoTableSvc(inner);
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
                "/stream_service.StreamService/DropActors" => {
                    #[allow(non_camel_case_types)]
                    struct DropActorsSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::DropActorsRequest> for DropActorsSvc<T> {
                        type Response = super::DropActorsResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DropActorsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).drop_actors(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DropActorsSvc(inner);
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
                "/stream_service.StreamService/InjectBarrier" => {
                    #[allow(non_camel_case_types)]
                    struct InjectBarrierSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::InjectBarrierRequest>
                        for InjectBarrierSvc<T>
                    {
                        type Response = super::InjectBarrierResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InjectBarrierRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).inject_barrier(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = InjectBarrierSvc(inner);
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
                "/stream_service.StreamService/CreateSource" => {
                    #[allow(non_camel_case_types)]
                    struct CreateSourceSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::CreateSourceRequest>
                        for CreateSourceSvc<T>
                    {
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
                "/stream_service.StreamService/DropSource" => {
                    #[allow(non_camel_case_types)]
                    struct DropSourceSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::DropSourceRequest> for DropSourceSvc<T> {
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
                "/stream_service.StreamService/Shutdown" => {
                    #[allow(non_camel_case_types)]
                    struct ShutdownSvc<T: StreamService>(pub Arc<T>);
                    impl<T: StreamService> tonic::server::UnaryService<super::ShutdownRequest> for ShutdownSvc<T> {
                        type Response = super::ShutdownResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShutdownRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).shutdown(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ShutdownSvc(inner);
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
    impl<T: StreamService> Clone for StreamServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: StreamService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: StreamService> tonic::transport::NamedService for StreamServiceServer<T> {
        const NAME: &'static str = "stream_service.StreamService";
    }
}
