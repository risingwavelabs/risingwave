// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::result::Result;
use std::sync::Arc;

use futures::Stream;
use tokio::net::TcpListener;

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_protocol::PgProtocol;
use crate::pg_response::{PgResponse, RowSetResult};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;
pub type SessionId = (i32, i32);
/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager<VS>: Send + Sync + 'static
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    type Session: Session<VS>;

    fn connect(&self, database: &str, user_name: &str) -> Result<Arc<Self::Session>, BoxedError>;

    fn cancel_queries_in_session(&self, session_id: SessionId);
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
///
/// format:
/// false: TEXT
/// true: BINARY
#[async_trait::async_trait]
pub trait Session<VS>: Send + Sync
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    async fn run_statement(
        self: Arc<Self>,
        sql: &str,
        format: bool,
    ) -> Result<PgResponse<VS>, BoxedError>;
    async fn infer_return_type(
        self: Arc<Self>,
        sql: &str,
    ) -> Result<Vec<PgFieldDescriptor>, BoxedError>;
    fn user_authenticator(&self) -> &UserAuthenticator;

    fn id(&self) -> SessionId;
}

#[derive(Debug, Clone)]
pub enum UserAuthenticator {
    // No need to authenticate.
    None,
    // raw password in clear-text form.
    ClearText(Vec<u8>),
    // password encrypted with random salt.
    Md5WithSalt {
        encrypted_password: Vec<u8>,
        salt: [u8; 4],
    },
}

impl UserAuthenticator {
    pub fn authenticate(&self, password: &[u8]) -> bool {
        match self {
            UserAuthenticator::None => true,
            UserAuthenticator::ClearText(text) => password == text,
            UserAuthenticator::Md5WithSalt {
                encrypted_password, ..
            } => encrypted_password == password,
        }
    }
}

/// Binds a Tcp listener at `addr`. Spawn a coroutine to serve every new connection.
pub async fn pg_serve<VS>(addr: &str, session_mgr: Arc<impl SessionManager<VS>>) -> io::Result<()>
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    let listener = TcpListener::bind(addr).await.unwrap();
    // accept connections and process them, spawning a new thread for each one
    tracing::info!("Server Listening at {}", addr);
    loop {
        let session_mgr = session_mgr.clone();
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                tracing::info!("New connection: {}", peer_addr);
                stream.set_nodelay(true)?;
                tokio::spawn(async move {
                    // connection succeeded
                    let mut pg_proto = PgProtocol::new(stream, session_mgr);
                    while !pg_proto.process().await {}
                    tracing::info!("Connection {} closed", peer_addr);
                });
            }

            Err(e) => {
                tracing::error!("Connection failure: {}", e);
            }
        }
    }
}
