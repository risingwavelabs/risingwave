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

use std::future::Future;
use std::io;
use std::result::Result;
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, TryFutureExt};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Statement;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tracing::debug;

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_protocol::{PgProtocol, TlsConfig};
use crate::pg_response::{PgResponse, RowSetResult};
use crate::types::Format;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;
pub type SessionId = (i32, i32);
/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager<VS, PS, PO>: Send + Sync + 'static
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
{
    type Session: Session<VS, PS, PO>;

    fn connect(&self, database: &str, user_name: &str) -> Result<Arc<Self::Session>, BoxedError>;

    fn cancel_queries_in_session(&self, session_id: SessionId);

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId);

    fn end_session(&self, session: &Self::Session);
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
#[async_trait::async_trait]
pub trait Session<VS, PS, PO>: Send + Sync
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
{
    /// The str sql can not use the unparse from AST: There is some problem when dealing with create
    /// view, see  https://github.com/risingwavelabs/risingwave/issues/6801.
    async fn run_one_query(
        self: Arc<Self>,
        sql: Statement,
        format: Format,
    ) -> Result<PgResponse<VS>, BoxedError>;

    fn parse(
        self: Arc<Self>,
        sql: Statement,
        params_types: Vec<DataType>,
    ) -> Result<PS, BoxedError>;

    // TODO: maybe this function should be async and return the notice more timely
    /// try to take the current notices from the session
    fn take_notices(self: Arc<Self>) -> Vec<String>;

    fn bind(
        self: Arc<Self>,
        prepare_statement: PS,
        params: Vec<Bytes>,
        param_formats: Vec<Format>,
        result_formats: Vec<Format>,
    ) -> Result<PO, BoxedError>;

    async fn execute(self: Arc<Self>, portal: PO) -> Result<PgResponse<VS>, BoxedError>;

    fn describe_statement(
        self: Arc<Self>,
        prepare_statement: PS,
    ) -> Result<(Vec<DataType>, Vec<PgFieldDescriptor>), BoxedError>;

    fn describe_portral(self: Arc<Self>, portal: PO) -> Result<Vec<PgFieldDescriptor>, BoxedError>;

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
pub async fn pg_serve<VS, PS, PO>(
    addr: &str,
    session_mgr: Arc<impl SessionManager<VS, PS, PO>>,
    ssl_config: Option<TlsConfig>,
) -> io::Result<()>
where
    VS: Stream<Item = RowSetResult> + Unpin + Send + 'static,
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
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
                let ssl_config = ssl_config.clone();
                let fut = handle_connection(stream, session_mgr, ssl_config);
                tokio::spawn(fut.inspect_err(|e| debug!("error handling connection: {e}")));
            }

            Err(e) => {
                tracing::error!("Connection failure: {}", e);
            }
        }
    }
}

#[tracing::instrument(level = "debug", skip_all)]
pub fn handle_connection<S, SM, VS, PS, PO>(
    stream: S,
    session_mgr: Arc<SM>,
    tls_config: Option<TlsConfig>,
) -> impl Future<Output = Result<(), anyhow::Error>>
where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager<VS, PS, PO>,
    VS: Stream<Item = RowSetResult> + Unpin + Send + 'static,
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
{
    let mut pg_proto = PgProtocol::new(stream, session_mgr, tls_config);
    async {
        loop {
            let msg = pg_proto.read_message().await?;
            tracing::trace!("Received message: {:?}", msg);
            let ret = pg_proto.process(msg).await;
            if ret {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use risingwave_common::types::DataType;
    use risingwave_sqlparser::ast::Statement;
    use tokio_postgres::NoTls;

    use crate::pg_field_descriptor::PgFieldDescriptor;
    use crate::pg_response::{PgResponse, RowSetResult, StatementType};
    use crate::pg_server::{
        pg_serve, BoxedError, Session, SessionId, SessionManager, UserAuthenticator,
    };
    use crate::types;
    use crate::types::Row;

    struct MockSessionManager {}
    struct MockSession {}

    impl SessionManager<BoxStream<'static, RowSetResult>, String, String> for MockSessionManager {
        type Session = MockSession;

        fn connect(
            &self,
            _database: &str,
            _user_name: &str,
        ) -> Result<Arc<Self::Session>, Box<dyn Error + Send + Sync>> {
            Ok(Arc::new(MockSession {}))
        }

        fn cancel_queries_in_session(&self, _session_id: SessionId) {
            todo!()
        }

        fn cancel_creating_jobs_in_session(&self, _session_id: SessionId) {
            todo!()
        }

        fn end_session(&self, _session: &Self::Session) {}
    }

    #[async_trait::async_trait]
    impl Session<BoxStream<'static, RowSetResult>, String, String> for MockSession {
        async fn run_one_query(
            self: Arc<Self>,
            _sql: Statement,
            _format: types::Format,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, BoxedError> {
            Ok(PgResponse::new_for_stream(
                StatementType::SELECT,
                None,
                futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])]).boxed(),
                vec![
                    // 1043 is the oid of varchar type.
                    // -1 is the type len of varchar type.
                    PgFieldDescriptor::new("".to_string(), 1043, -1);
                    1
                ],
            ))
        }

        fn parse(
            self: Arc<Self>,
            _sql: Statement,
            _params_types: Vec<DataType>,
        ) -> Result<String, BoxedError> {
            Ok(String::new())
        }

        fn bind(
            self: Arc<Self>,
            _prepare_statement: String,
            _params: Vec<Bytes>,
            _param_formats: Vec<types::Format>,
            _result_formats: Vec<types::Format>,
        ) -> Result<String, BoxedError> {
            Ok(String::new())
        }

        async fn execute(
            self: Arc<Self>,
            _portal: String,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, BoxedError> {
            Ok(PgResponse::new_for_stream(
                StatementType::SELECT,
                None,
                futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])]).boxed(),
                vec![
                    // 1043 is the oid of varchar type.
                    // -1 is the type len of varchar type.
                    PgFieldDescriptor::new("".to_string(), 1043, -1);
                    1
                ],
            ))
        }

        fn describe_statement(
            self: Arc<Self>,
            _statement: String,
        ) -> Result<(Vec<DataType>, Vec<PgFieldDescriptor>), BoxedError> {
            Ok((
                vec![],
                vec![PgFieldDescriptor::new("".to_string(), 1043, -1)],
            ))
        }

        fn describe_portral(
            self: Arc<Self>,
            _portal: String,
        ) -> Result<Vec<PgFieldDescriptor>, BoxedError> {
            Ok(vec![PgFieldDescriptor::new("".to_string(), 1043, -1)])
        }

        fn user_authenticator(&self) -> &UserAuthenticator {
            &UserAuthenticator::None
        }

        fn id(&self) -> SessionId {
            (0, 0)
        }

        fn take_notices(self: Arc<Self>) -> Vec<String> {
            vec![]
        }
    }

    #[tokio::test]
    async fn test_query() {
        let session_mgr = Arc::new(MockSessionManager {});
        tokio::spawn(async move { pg_serve("127.0.0.1:10000", session_mgr, None).await });
        // wait for server to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=10000", NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client
            .simple_query("SELECT ''")
            .await
            .expect("Error executing query");
        // Row + CommandComplete
        assert_eq!(rows.len(), 2);

        let rows = client
            .query("SELECT ''", &[])
            .await
            .expect("Error executing query");
        assert_eq!(rows.len(), 1);
    }
}
