// Copyright 2025 RisingWave Labs
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

use core::task;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::task::Poll;

use futures::{Future, FutureExt};
use openssl::error::ErrorStack;
use postgres_openssl::{MakeTlsConnector, TlsConnector, TlsStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::NoTls;
use tokio_postgres::tls::{self, MakeTlsConnect, NoTlsFuture, NoTlsStream, TlsConnect};

pub enum MaybeMakeTlsConnector {
    NoTls(NoTls),
    Tls(MakeTlsConnector),
}

impl<S> MakeTlsConnect<S> for MaybeMakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + core::fmt::Debug + 'static + Sync + Send,
{
    type Error = ErrorStack;
    type Stream = MaybeTlsStream<S>;
    type TlsConnect = MaybeTlsConnector;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        match self {
            MaybeMakeTlsConnector::NoTls(make_connector) => {
                let connector =
                    <NoTls as MakeTlsConnect<S>>::make_tls_connect(make_connector, domain)
                        .expect("make NoTls connector always success");
                Ok(MaybeTlsConnector::NoTls(connector))
            }
            MaybeMakeTlsConnector::Tls(make_connector) => {
                <MakeTlsConnector as MakeTlsConnect<S>>::make_tls_connect(make_connector, domain)
                    .map(MaybeTlsConnector::Tls)
            }
        }
    }
}

pub enum MaybeTlsConnector {
    NoTls(NoTls),
    Tls(TlsConnector),
}

impl<S> TlsConnect<S> for MaybeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    type Error = Box<dyn Error + Send + Sync>;
    type Future = MaybeTlsFuture<Self::Stream, Self::Error>;
    type Stream = MaybeTlsStream<S>;

    fn connect(self, stream: S) -> Self::Future {
        match self {
            MaybeTlsConnector::NoTls(connector) => MaybeTlsFuture::NoTls(connector.connect(stream)),
            MaybeTlsConnector::Tls(connector) => MaybeTlsFuture::Tls(Box::pin(
                connector
                    .connect(stream)
                    .map(|x| x.map(|x| MaybeTlsStream::Tls(x))),
            )),
        }
    }
}

pub enum MaybeTlsStream<S> {
    NoTls(NoTlsStream),
    Tls(TlsStream<S>),
}

impl<S> AsyncRead for MaybeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            MaybeTlsStream::NoTls(stream) => {
                <NoTlsStream as AsyncRead>::poll_read(Pin::new(stream), cx, buf)
            }
            MaybeTlsStream::Tls(stream) => {
                <TlsStream<S> as AsyncRead>::poll_read(Pin::new(stream), cx, buf)
            }
        }
    }
}

impl<S> AsyncWrite for MaybeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            MaybeTlsStream::NoTls(stream) => {
                <NoTlsStream as AsyncWrite>::poll_write(Pin::new(stream), cx, buf)
            }
            MaybeTlsStream::Tls(stream) => {
                <TlsStream<S> as AsyncWrite>::poll_write(Pin::new(stream), cx, buf)
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            MaybeTlsStream::NoTls(stream) => {
                <NoTlsStream as AsyncWrite>::poll_flush(Pin::new(stream), cx)
            }
            MaybeTlsStream::Tls(stream) => {
                <TlsStream<S> as AsyncWrite>::poll_flush(Pin::new(stream), cx)
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            MaybeTlsStream::NoTls(stream) => {
                <NoTlsStream as AsyncWrite>::poll_shutdown(Pin::new(stream), cx)
            }
            MaybeTlsStream::Tls(stream) => {
                <TlsStream<S> as AsyncWrite>::poll_shutdown(Pin::new(stream), cx)
            }
        }
    }
}

impl<S> tls::TlsStream for MaybeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> tls::ChannelBinding {
        match self {
            MaybeTlsStream::NoTls(stream) => stream.channel_binding(),
            MaybeTlsStream::Tls(stream) => stream.channel_binding(),
        }
    }
}

pub enum MaybeTlsFuture<S, E> {
    NoTls(NoTlsFuture),
    Tls(Pin<Box<dyn Future<Output = Result<S, E>> + Send>>),
}

impl<S, E> Future for MaybeTlsFuture<MaybeTlsStream<S>, E>
where
    MaybeTlsStream<S>: Sync + Send,
    E: std::convert::From<tokio_postgres::tls::NoTlsError>,
{
    type Output = Result<MaybeTlsStream<S>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            MaybeTlsFuture::NoTls(fut) => fut
                .poll_unpin(cx)
                .map(|x| x.map(|x| MaybeTlsStream::NoTls(x)))
                .map_err(|x| x.into()),
            MaybeTlsFuture::Tls(fut) => fut.poll_unpin(cx),
        }
    }
}
