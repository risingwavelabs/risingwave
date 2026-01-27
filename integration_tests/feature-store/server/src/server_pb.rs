#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportActionRequest {
    #[prost(string, tag = "1")]
    pub userid: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub eventtype: ::prost::alloc::string::String,
    #[prost(int64, tag = "3")]
    pub changenum: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportActionResponse {
    #[prost(uint64, tag = "1")]
    pub timestamp: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFeatureRequest {
    #[prost(string, tag = "1")]
    pub userid: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetFeatureResponse {
    #[prost(uint64, tag = "1")]
    pub count: u64,
    #[prost(int64, tag = "2")]
    pub sum: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportTaxiActionRequest {
    #[prost(int32, tag = "1")]
    pub vendor_id: i32,
    #[prost(string, tag = "2")]
    pub lpep_pickup_datetime: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub lpep_dropoff_datetime: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub store_and_fwd_flag: bool,
    #[prost(double, tag = "5")]
    pub ratecode_id: f64,
    #[prost(int64, tag = "6")]
    pub pu_location_id: i64,
    #[prost(int64, tag = "7")]
    pub do_location_id: i64,
    #[prost(double, tag = "8")]
    pub passenger_count: f64,
    #[prost(double, tag = "9")]
    pub trip_distance: f64,
    #[prost(double, tag = "10")]
    pub fare_amount: f64,
    #[prost(double, tag = "11")]
    pub extra: f64,
    #[prost(double, tag = "12")]
    pub mta_tax: f64,
    #[prost(double, tag = "13")]
    pub tip_amount: f64,
    #[prost(double, tag = "14")]
    pub tolls_amount: f64,
    #[prost(double, tag = "15")]
    pub ehail_fee: f64,
    #[prost(double, tag = "16")]
    pub improvement_surcharge: f64,
    #[prost(double, tag = "17")]
    pub total_amount: f64,
    #[prost(double, tag = "18")]
    pub payment_type: f64,
    #[prost(double, tag = "19")]
    pub trip_type: f64,
    #[prost(double, tag = "20")]
    pub congestion_surcharge: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportTaxiActionResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTaxiAmountRequest {
    #[prost(int64, tag = "1")]
    pub do_location_id: i64,
    #[prost(int64, tag = "2")]
    pub pu_location_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetTaxiAmountResponse {
    #[prost(double, tag = "1")]
    pub fare_amount: f64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartTrainingRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartTrainingResponse {}
/// Generated client implementations.
pub mod server_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ServerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ServerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ServerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ServerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ServerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn get_feature(
            &mut self,
            request: impl tonic::IntoRequest<super::GetFeatureRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetFeatureResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/server_pb.Server/GetFeature",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("server_pb.Server", "GetFeature"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn report_action(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportActionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportActionResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/server_pb.Server/ReportAction",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("server_pb.Server", "ReportAction"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn report_taxi_action(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportTaxiActionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportTaxiActionResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/server_pb.Server/ReportTaxiAction",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("server_pb.Server", "ReportTaxiAction"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_taxi_amount(
            &mut self,
            request: impl tonic::IntoRequest<super::GetTaxiAmountRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetTaxiAmountResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/server_pb.Server/GetTaxiAmount",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("server_pb.Server", "GetTaxiAmount"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn start_training(
            &mut self,
            request: impl tonic::IntoRequest<super::StartTrainingRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StartTrainingResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/server_pb.Server/StartTraining",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("server_pb.Server", "StartTraining"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod server_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ServerServer.
    #[async_trait]
    pub trait Server: Send + Sync + 'static {
        async fn get_feature(
            &self,
            request: tonic::Request<super::GetFeatureRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetFeatureResponse>,
            tonic::Status,
        >;
        async fn report_action(
            &self,
            request: tonic::Request<super::ReportActionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportActionResponse>,
            tonic::Status,
        >;
        async fn report_taxi_action(
            &self,
            request: tonic::Request<super::ReportTaxiActionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportTaxiActionResponse>,
            tonic::Status,
        >;
        async fn get_taxi_amount(
            &self,
            request: tonic::Request<super::GetTaxiAmountRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetTaxiAmountResponse>,
            tonic::Status,
        >;
        async fn start_training(
            &self,
            request: tonic::Request<super::StartTrainingRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StartTrainingResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ServerServer<T: Server> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Server> ServerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ServerServer<T>
    where
        T: Server,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/server_pb.Server/GetFeature" => {
                    #[allow(non_camel_case_types)]
                    struct GetFeatureSvc<T: Server>(pub Arc<T>);
                    impl<T: Server> tonic::server::UnaryService<super::GetFeatureRequest>
                    for GetFeatureSvc<T> {
                        type Response = super::GetFeatureResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetFeatureRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::get_feature(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetFeatureSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/server_pb.Server/ReportAction" => {
                    #[allow(non_camel_case_types)]
                    struct ReportActionSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::ReportActionRequest>
                    for ReportActionSvc<T> {
                        type Response = super::ReportActionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportActionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::report_action(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReportActionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/server_pb.Server/ReportTaxiAction" => {
                    #[allow(non_camel_case_types)]
                    struct ReportTaxiActionSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::ReportTaxiActionRequest>
                    for ReportTaxiActionSvc<T> {
                        type Response = super::ReportTaxiActionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportTaxiActionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::report_taxi_action(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ReportTaxiActionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/server_pb.Server/GetTaxiAmount" => {
                    #[allow(non_camel_case_types)]
                    struct GetTaxiAmountSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::GetTaxiAmountRequest>
                    for GetTaxiAmountSvc<T> {
                        type Response = super::GetTaxiAmountResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetTaxiAmountRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::get_taxi_amount(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetTaxiAmountSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/server_pb.Server/StartTraining" => {
                    #[allow(non_camel_case_types)]
                    struct StartTrainingSvc<T: Server>(pub Arc<T>);
                    impl<
                        T: Server,
                    > tonic::server::UnaryService<super::StartTrainingRequest>
                    for StartTrainingSvc<T> {
                        type Response = super::StartTrainingResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StartTrainingRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Server>::start_training(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = StartTrainingSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Server> Clone for ServerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Server> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Server> tonic::server::NamedService for ServerServer<T> {
        const NAME: &'static str = "server_pb.Server";
    }
}
