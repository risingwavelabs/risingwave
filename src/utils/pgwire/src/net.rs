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

use std::io;
use std::net::SocketAddr as IpSocketAddr;
#[cfg(madsim)]
use std::os::unix::net::SocketAddr as UnixSocketAddr;
use std::sync::Arc;

#[cfg(not(madsim))]
use tokio::net::unix::SocketAddr as UnixSocketAddr;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

/// A wrapper of either [`TcpListener`] or [`UnixListener`].
pub(crate) enum Listener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

/// A wrapper of either [`TcpStream`] or [`UnixStream`].
#[auto_enums::enum_derive(tokio1::AsyncRead, tokio1::AsyncWrite)]
pub(crate) enum Stream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

/// A wrapper of either [`std::net::SocketAddr`] or [`tokio::net::unix::SocketAddr`].
pub enum Address {
    Tcp(IpSocketAddr),
    Unix(UnixSocketAddr),
}

pub type AddressRef = Arc<Address>;

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::Tcp(addr) => addr.fmt(f),
            Address::Unix(addr) => {
                if let Some(path) = addr.as_pathname() {
                    path.display().fmt(f)
                } else {
                    std::fmt::Debug::fmt(addr, f)
                }
            }
        }
    }
}

impl Listener {
    /// Creates a new [`Listener`] bound to the specified address.
    ///
    /// If the address starts with `unix:`, it will create a [`UnixListener`].
    /// Otherwise, it will create a [`TcpListener`].
    pub async fn bind(addr: &str) -> io::Result<Self> {
        if let Some(path) = addr.strip_prefix("unix:") {
            UnixListener::bind(path).map(Self::Unix)
        } else {
            TcpListener::bind(addr).await.map(Self::Tcp)
        }
    }

    /// Accepts a new incoming connection from this listener.
    ///
    /// Returns a tuple of the stream and the string representation of the peer address.
    pub async fn accept(&self) -> io::Result<(Stream, Address)> {
        match self {
            Self::Tcp(listener) => {
                let (stream, addr) = listener.accept().await?;
                stream.set_nodelay(true)?;
                Ok((Stream::Tcp(stream), Address::Tcp(addr)))
            }
            Self::Unix(listener) => {
                let (stream, addr) = listener.accept().await?;
                Ok((Stream::Unix(stream), Address::Unix(addr)))
            }
        }
    }
}
