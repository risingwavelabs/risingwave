// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::type_name;
use std::cmp::Ordering;
use std::future::Future;
use std::io::{Error, IoSlice};
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::FutureExt;
use http::Uri;
use hyper::client::connect::Connection;
use hyper::client::HttpConnector;
use hyper::service::Service;
use pin_project_lite::pin_project;
use prometheus::{
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::{Channel, Endpoint};
use tracing::{info, warn};

use crate::metrics::LabelGuardedIntCounterVec;
use crate::monitor::GLOBAL_METRICS_REGISTRY;
use crate::register_guarded_int_counter_vec_with_registry;

pub trait MonitorAsyncReadWrite {
    fn on_read(&mut self, _size: usize) {}
    fn on_eof(&mut self) {}
    fn on_read_err(&mut self, _err: &std::io::Error) {}

    fn on_write(&mut self, _size: usize) {}
    fn on_flush(&mut self) {}
    fn on_shutdown(&mut self) {}
    fn on_write_err(&mut self, _err: &std::io::Error) {}
}

pin_project! {
    #[derive(Clone)]
    pub struct MonitoredConnection<C, M> {
        #[pin]
        inner: C,
        monitor: M,
    }
}

impl<C, M> MonitoredConnection<C, M> {
    pub fn new(connector: C, monitor: M) -> Self {
        Self {
            inner: connector,
            monitor,
        }
    }

    fn project_into(this: Pin<&mut Self>) -> (Pin<&mut C>, &mut M) {
        let this = this.project();
        (this.inner, this.monitor)
    }
}

impl<C: AsyncRead, M: MonitorAsyncReadWrite> AsyncRead for MonitoredConnection<C, M> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before_buf_size = buf.filled().len();
        let (inner, monitor) = MonitoredConnection::project_into(self);
        let ret = inner.poll_read(cx, buf);
        match &ret {
            Poll::Ready(Ok(())) => {
                let after_buf_size = buf.filled().len();
                match after_buf_size.cmp(&before_buf_size) {
                    Ordering::Less => {
                        unreachable!(
                            "buf size decrease after poll read. Bad AsyncRead implementation on {}",
                            type_name::<C>()
                        );
                    }
                    Ordering::Equal => {
                        monitor.on_eof();
                    }
                    Ordering::Greater => {
                        monitor.on_read(after_buf_size - before_buf_size);
                    }
                }
            }
            Poll::Ready(Err(e)) => {
                monitor.on_read_err(e);
            }
            Poll::Pending => {}
        }
        ret
    }
}

impl<C: AsyncWrite, M: MonitorAsyncReadWrite> AsyncWrite for MonitoredConnection<C, M> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let (inner, monitor) = MonitoredConnection::project_into(self);
        let ret = inner.poll_write(cx, buf);
        match &ret {
            Poll::Ready(Ok(size)) => {
                monitor.on_write(*size);
            }
            Poll::Ready(Err(e)) => {
                monitor.on_write_err(e);
            }
            Poll::Pending => {}
        }
        ret
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let (inner, monitor) = MonitoredConnection::project_into(self);
        let ret = inner.poll_flush(cx);
        match &ret {
            Poll::Ready(Ok(())) => {
                monitor.on_flush();
            }
            Poll::Ready(Err(e)) => {
                monitor.on_write_err(e);
            }
            Poll::Pending => {}
        }
        ret
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let (inner, monitor) = MonitoredConnection::project_into(self);
        let ret = inner.poll_shutdown(cx);
        match &ret {
            Poll::Ready(result) => {
                monitor.on_shutdown();
                if let Err(e) = result {
                    monitor.on_write_err(e);
                }
            }
            Poll::Pending => {}
        }
        ret
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, Error>> {
        let (inner, monitor) = MonitoredConnection::project_into(self);
        let ret = inner.poll_write_vectored(cx, bufs);
        match &ret {
            Poll::Ready(Ok(size)) => {
                monitor.on_write(*size);
            }
            Poll::Ready(Err(e)) => {
                monitor.on_write_err(e);
            }
            Poll::Pending => {}
        }
        ret
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl<C: Connection, M> Connection for MonitoredConnection<C, M> {
    fn connected(&self) -> hyper::client::connect::Connected {
        self.inner.connected()
    }
}

#[cfg(not(madsim))]
impl<C: tonic::transport::server::Connected, M> tonic::transport::server::Connected
    for MonitoredConnection<C, M>
{
    type ConnectInfo = C::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

pub trait MonitorNewConnection {
    type ConnectionMonitor: MonitorAsyncReadWrite;

    fn new_connection_monitor(&self, endpoint: String) -> Self::ConnectionMonitor;
    fn on_err(&self, endpoint: String);
}

impl<C: Service<Uri>, M: MonitorNewConnection + Clone + 'static> Service<Uri>
    for MonitoredConnection<C, M>
where
    C::Future: 'static,
{
    type Error = C::Error;
    type Response = MonitoredConnection<C::Response, M::ConnectionMonitor>;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + 'static;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let ret = self.inner.poll_ready(cx);
        if let Poll::Ready(Err(_)) = &ret {
            self.monitor.on_err("<poll_ready>".to_string());
        }
        ret
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let endpoint = format!("{:?}", uri.host());
        let monitor = self.monitor.clone();
        self.inner
            .call(uri)
            .map(move |result: Result<_, _>| match result {
                Ok(resp) => Ok(MonitoredConnection::new(
                    resp,
                    monitor.new_connection_monitor(endpoint),
                )),
                Err(e) => {
                    monitor.on_err(endpoint);
                    Err(e)
                }
            })
    }
}

#[cfg(not(madsim))]
impl<Con, E, C: futures::Stream<Item = Result<Con, E>>, M: MonitorNewConnection> futures::Stream
    for MonitoredConnection<C, M>
where
    Con:
        tonic::transport::server::Connected<ConnectInfo = tonic::transport::server::TcpConnectInfo>,
{
    type Item = Result<MonitoredConnection<Con, M::ConnectionMonitor>, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (inner, monitor) = MonitoredConnection::project_into(self);
        inner.poll_next(cx).map(|opt| {
            opt.map(|result| {
                result.map(|conn| {
                    let remote_addr = conn.connect_info().remote_addr();
                    let endpoint = remote_addr
                        .map(|remote_addr| format!("{}", remote_addr.ip()))
                        .unwrap_or("unknown".to_string());
                    MonitoredConnection::new(conn, monitor.new_connection_monitor(endpoint))
                })
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[derive(Clone)]
pub struct ConnectionMetrics {
    connection_count: IntGaugeVec,
    connection_create_rate: IntCounterVec,
    connection_err_rate: IntCounterVec,

    read_rate: IntCounterVec,
    reader_count: IntGaugeVec,

    write_rate: IntCounterVec,
    writer_count: IntGaugeVec,

    io_err_rate: LabelGuardedIntCounterVec<4>,
}

pub static GLOBAL_CONNECTION_METRICS: LazyLock<ConnectionMetrics> =
    LazyLock::new(|| ConnectionMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl ConnectionMetrics {
    pub fn new(registry: &Registry) -> Self {
        let labels = ["connection_type", "uri"];
        let connection_count = register_int_gauge_vec_with_registry!(
            "connection_count",
            "The number of current existing connection",
            &labels,
            registry,
        )
        .unwrap();

        let connection_create_rate = register_int_counter_vec_with_registry!(
            "connection_create_rate",
            "Rate on creating new connection",
            &labels,
            registry,
        )
        .unwrap();

        let connection_err_rate = register_int_counter_vec_with_registry!(
            "connection_err_rate",
            "Error rate on creating new connection",
            &labels,
            registry,
        )
        .unwrap();

        let read_rate = register_int_counter_vec_with_registry!(
            "connection_read_rate",
            "Read rate of a connection",
            &labels,
            registry,
        )
        .unwrap();

        let reader_count = register_int_gauge_vec_with_registry!(
            "connection_reader_count",
            "The number of current existing reader",
            &labels,
            registry,
        )
        .unwrap();

        let write_rate = register_int_counter_vec_with_registry!(
            "connection_write_rate",
            "Write rate of a connection",
            &labels,
            registry,
        )
        .unwrap();

        let writer_count = register_int_gauge_vec_with_registry!(
            "connection_writer_count",
            "The number of current existing writer",
            &labels,
            registry,
        )
        .unwrap();

        let io_err_rate = register_guarded_int_counter_vec_with_registry!(
            "connection_io_err_rate",
            "IO err rate of a connection",
            &["connection_type", "uri", "op_type", "error_kind"],
            registry,
        )
        .unwrap();

        Self {
            connection_count,
            connection_create_rate,
            connection_err_rate,
            read_rate,
            reader_count,
            write_rate,
            writer_count,
            io_err_rate,
        }
    }
}

pub struct TcpConfig {
    pub tcp_nodelay: bool,
    pub keepalive_duration: Option<Duration>,
}

pub fn monitor_connector<C>(
    connector: C,
    connection_type: impl Into<String>,
) -> MonitoredConnection<C, MonitorNewConnectionImpl> {
    let connection_type = connection_type.into();
    info!(
        "monitoring connector {} with type {}",
        type_name::<C>(),
        connection_type
    );
    MonitoredConnection::new(connector, MonitorNewConnectionImpl { connection_type })
}

#[easy_ext::ext(EndpointExt)]
impl Endpoint {
    pub async fn monitored_connect(
        self,
        connection_type: impl Into<String>,
        config: TcpConfig,
    ) -> Result<Channel, tonic::transport::Error> {
        #[cfg(not(madsim))]
        {
            let mut http = HttpConnector::new();
            http.enforce_http(false);
            http.set_nodelay(config.tcp_nodelay);
            http.set_keepalive(config.keepalive_duration);

            let connector = monitor_connector(http, connection_type);
            self.connect_with_connector(connector).await
        }
        #[cfg(madsim)]
        {
            self.connect().await
        }
    }

    #[cfg(not(madsim))]
    pub fn monitored_connect_lazy(
        self,
        connection_type: impl Into<String>,
        config: TcpConfig,
    ) -> Channel {
        let mut http = HttpConnector::new();
        http.enforce_http(false);
        http.set_nodelay(config.tcp_nodelay);
        http.set_keepalive(config.keepalive_duration);

        let connector = monitor_connector(http, connection_type);
        self.connect_with_connector_lazy(connector)
    }
}

#[cfg(not(madsim))]
#[easy_ext::ext(RouterExt)]
impl<L> tonic::transport::server::Router<L> {
    pub async fn monitored_serve_with_shutdown<ResBody>(
        self,
        listen_addr: std::net::SocketAddr,
        connection_type: impl Into<String>,
        config: TcpConfig,
        signal: impl Future<Output = ()>,
    ) where
        L: tower_layer::Layer<tonic::transport::server::Routes>,
        L::Service: Service<
                http::request::Request<hyper::Body>,
                Response = http::response::Response<ResBody>,
            > + Clone
            + Send
            + 'static,
        <<L as tower_layer::Layer<tonic::transport::server::Routes>>::Service as Service<
            http::request::Request<hyper::Body>,
        >>::Future: Send + 'static,
        <<L as tower_layer::Layer<tonic::transport::server::Routes>>::Service as Service<
            http::request::Request<hyper::Body>,
        >>::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
        ResBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
        ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let incoming = tonic::transport::server::TcpIncoming::new(
            listen_addr,
            config.tcp_nodelay,
            config.keepalive_duration,
        )
        .unwrap();
        let incoming = MonitoredConnection::new(
            incoming,
            MonitorNewConnectionImpl {
                connection_type: connection_type.into(),
            },
        );
        self.serve_with_incoming_shutdown(incoming, signal)
            .await
            .unwrap()
    }
}

#[cfg(madsim)]
#[easy_ext::ext(RouterExt)]
impl<L> tonic::transport::server::Router<L> {
    pub async fn monitored_serve_with_shutdown(
        self,
        listen_addr: std::net::SocketAddr,
        connection_type: impl Into<String>,
        config: TcpConfig,
        signal: impl Future<Output = ()>,
    ) {
        self.serve_with_shutdown(listen_addr, signal).await.unwrap()
    }
}

#[cfg(not(madsim))]
pub fn monitored_tcp_incoming(
    listen_addr: std::net::SocketAddr,
    connection_type: impl Into<String>,
    config: TcpConfig,
) -> Result<
    MonitoredConnection<tonic::transport::server::TcpIncoming, MonitorNewConnectionImpl>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let incoming = tonic::transport::server::TcpIncoming::new(
        listen_addr,
        config.tcp_nodelay,
        config.keepalive_duration,
    )?;
    Ok(MonitoredConnection::new(
        incoming,
        MonitorNewConnectionImpl {
            connection_type: connection_type.into(),
        },
    ))
}

#[derive(Clone)]
pub struct MonitorNewConnectionImpl {
    connection_type: String,
}

impl MonitorNewConnection for MonitorNewConnectionImpl {
    type ConnectionMonitor = MonitorAsyncReadWriteImpl;

    fn new_connection_monitor(&self, endpoint: String) -> Self::ConnectionMonitor {
        let labels = [self.connection_type.as_str(), endpoint.as_str()];
        let read_rate = GLOBAL_CONNECTION_METRICS
            .read_rate
            .with_label_values(&labels);
        let reader_count = GLOBAL_CONNECTION_METRICS
            .reader_count
            .with_label_values(&labels);
        let write_rate = GLOBAL_CONNECTION_METRICS
            .write_rate
            .with_label_values(&labels);
        let writer_count = GLOBAL_CONNECTION_METRICS
            .writer_count
            .with_label_values(&labels);
        let connection_count = GLOBAL_CONNECTION_METRICS
            .connection_count
            .with_label_values(&labels);

        GLOBAL_CONNECTION_METRICS
            .connection_create_rate
            .with_label_values(&labels)
            .inc();

        MonitorAsyncReadWriteImpl::new(
            endpoint,
            self.connection_type.clone(),
            read_rate,
            reader_count,
            write_rate,
            writer_count,
            connection_count,
        )
    }

    fn on_err(&self, endpoint: String) {
        GLOBAL_CONNECTION_METRICS
            .connection_err_rate
            .with_label_values(&[self.connection_type.as_str(), endpoint.as_str()])
            .inc();
    }
}

const READ_WRITE_RATE_REPORT_INTERVAL: u64 = 1024;

pub struct MonitorAsyncReadWriteImpl {
    endpoint: String,
    connection_type: String,

    unreported_read_rate: u64,
    read_rate: IntCounter,
    reader_count_guard: IntGauge,
    is_eof: bool,

    unreported_write_rate: u64,
    write_rate: IntCounter,
    writer_count_guard: IntGauge,
    is_shutdown: bool,

    connection_count_guard: IntGauge,
}

impl MonitorAsyncReadWriteImpl {
    pub fn new(
        endpoint: String,
        connection_type: String,
        read_rate: IntCounter,
        reader_count: IntGauge,
        write_rate: IntCounter,
        writer_count: IntGauge,
        connection_count: IntGauge,
    ) -> Self {
        reader_count.inc();
        writer_count.inc();
        connection_count.inc();
        Self {
            endpoint,
            connection_type,
            unreported_read_rate: 0,
            read_rate,
            reader_count_guard: reader_count,
            is_eof: false,
            unreported_write_rate: 0,
            write_rate,
            writer_count_guard: writer_count,
            is_shutdown: false,
            connection_count_guard: connection_count,
        }
    }
}

impl Drop for MonitorAsyncReadWriteImpl {
    fn drop(&mut self) {
        if self.unreported_read_rate > 0 {
            self.read_rate.inc_by(self.unreported_read_rate);
        }
        if self.unreported_write_rate > 0 {
            self.write_rate.inc_by(self.unreported_write_rate);
        }
        if !self.is_eof {
            self.reader_count_guard.dec();
        }
        if !self.is_shutdown {
            self.writer_count_guard.dec();
        }
        self.connection_count_guard.dec();
    }
}

impl MonitorAsyncReadWrite for MonitorAsyncReadWriteImpl {
    fn on_read(&mut self, size: usize) {
        self.unreported_read_rate += size as u64;
        if self.unreported_read_rate >= READ_WRITE_RATE_REPORT_INTERVAL {
            self.read_rate.inc_by(self.unreported_read_rate);
            self.unreported_read_rate = 0;
        }
    }

    fn on_eof(&mut self) {
        if self.is_eof {
            warn!("get eof for multiple time");
            return;
        }
        self.is_eof = true;
        self.reader_count_guard.dec();
    }

    fn on_read_err(&mut self, err: &Error) {
        // No need to store the value returned from with_label_values
        // because it is reporting a single error.
        GLOBAL_CONNECTION_METRICS
            .io_err_rate
            .with_guarded_label_values(&[
                self.connection_type.as_str(),
                self.endpoint.as_str(),
                "read",
                err.kind().to_string().as_str(),
            ])
            .inc();
    }

    fn on_write(&mut self, size: usize) {
        self.unreported_write_rate += size as u64;
        if self.unreported_write_rate >= READ_WRITE_RATE_REPORT_INTERVAL {
            self.write_rate.inc_by(self.unreported_write_rate);
            self.unreported_write_rate = 0;
        }
    }

    fn on_shutdown(&mut self) {
        if self.is_shutdown {
            warn!("get shutdown for multiple time");
            return;
        }
        self.is_shutdown = true;
        self.writer_count_guard.dec();
    }

    fn on_write_err(&mut self, err: &Error) {
        // No need to store the value returned from with_label_values
        // because it is reporting a single error.
        GLOBAL_CONNECTION_METRICS
            .io_err_rate
            .with_guarded_label_values(&[
                self.connection_type.as_str(),
                self.endpoint.as_str(),
                "write",
                err.kind().to_string().as_str(),
            ])
            .inc();
    }
}
