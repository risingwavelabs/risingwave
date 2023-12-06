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
use std::time::Instant;

use bytes::Bytes;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Statement;
use thiserror_ext::AsReport;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::net::{AddressRef, Listener};
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::TransactionStatus;
use crate::pg_protocol::{PgProtocol, TlsConfig};
use crate::pg_response::{PgResponse, ValuesStream};
use crate::types::Format;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;
type ProcessId = i32;
type SecretKey = i32;
pub type SessionId = (ProcessId, SecretKey);

/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager: Send + Sync + 'static {
    type Session: Session;

    fn connect(
        &self,
        database: &str,
        user_name: &str,
        peer_addr: AddressRef,
    ) -> Result<Arc<Self::Session>, BoxedError>;

    fn cancel_queries_in_session(&self, session_id: SessionId);

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId);

    fn end_session(&self, session: &Self::Session);
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
pub trait Session: Send + Sync {
    type ValuesStream: ValuesStream;
    type PreparedStatement: Send + Clone + 'static;
    type Portal: Send + Clone + std::fmt::Display + 'static;

    /// The str sql can not use the unparse from AST: There is some problem when dealing with create
    /// view, see <https://github.com/risingwavelabs/risingwave/issues/6801>.
    fn run_one_query(
        self: Arc<Self>,
        stmt: Statement,
        format: Format,
    ) -> impl Future<Output = Result<PgResponse<Self::ValuesStream>, BoxedError>> + Send;

    fn parse(
        self: Arc<Self>,
        sql: Option<Statement>,
        params_types: Vec<Option<DataType>>,
    ) -> Result<Self::PreparedStatement, BoxedError>;

    // TODO: maybe this function should be async and return the notice more timely
    /// try to take the current notices from the session
    fn take_notices(self: Arc<Self>) -> Vec<String>;

    fn bind(
        self: Arc<Self>,
        prepare_statement: Self::PreparedStatement,
        params: Vec<Option<Bytes>>,
        param_formats: Vec<Format>,
        result_formats: Vec<Format>,
    ) -> Result<Self::Portal, BoxedError>;

    fn execute(
        self: Arc<Self>,
        portal: Self::Portal,
    ) -> impl Future<Output = Result<PgResponse<Self::ValuesStream>, BoxedError>> + Send;

    fn describe_statement(
        self: Arc<Self>,
        prepare_statement: Self::PreparedStatement,
    ) -> Result<(Vec<DataType>, Vec<PgFieldDescriptor>), BoxedError>;

    fn describe_portal(
        self: Arc<Self>,
        portal: Self::Portal,
    ) -> Result<Vec<PgFieldDescriptor>, BoxedError>;

    fn user_authenticator(&self) -> &UserAuthenticator;

    fn id(&self) -> SessionId;

    fn set_config(&self, key: &str, value: String) -> Result<(), BoxedError>;

    fn transaction_status(&self) -> TransactionStatus;

    fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard;
}

/// Each session could run different SQLs multiple times.
/// `ExecContext` represents the lifetime of a running SQL in the current session.
pub struct ExecContext {
    pub running_sql: Arc<str>,
    /// The instant of the running sql
    pub last_instant: Instant,
}

/// `ExecContextGuard` holds a `Arc` pointer. Once `ExecContextGuard` is dropped,
/// the inner `Arc<ExecContext>` should not be referred anymore, so that its `Weak` reference (used in `SessionImpl`) will be the same lifecycle of the running sql execution context.
pub struct ExecContextGuard(Arc<ExecContext>);

impl ExecContextGuard {
    pub fn new(exec_context: Arc<ExecContext>) -> Self {
        Self(exec_context)
    }
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

/// Binds a Tcp or Unix listener at `addr`. Spawn a coroutine to serve every new connection.
pub async fn pg_serve(
    addr: &str,
    session_mgr: Arc<impl SessionManager>,
    tls_config: Option<TlsConfig>,
) -> io::Result<()> {
    let listener = Listener::bind(addr).await?;
    tracing::info!(addr, "server started");

    loop {
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                tracing::info!(%peer_addr, "accept connection");
                tokio::spawn(handle_connection(
                    stream,
                    session_mgr.clone(),
                    tls_config.clone(),
                    Arc::new(peer_addr),
                ));
            }

            Err(e) => {
                tracing::error!(error = %e.as_report(), "failed to accept connection",);
            }
        }
    }
}

pub async fn handle_connection<S, SM>(
    stream: S,
    session_mgr: Arc<SM>,
    tls_config: Option<TlsConfig>,
    peer_addr: AddressRef,
) where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager,
{
    let mut pg_proto = PgProtocol::new(stream, session_mgr, tls_config, peer_addr);
    loop {
        let msg = match pg_proto.read_message().await {
            Ok(msg) => msg,
            Err(e) => {
                tracing::error!(error = %e.as_report(), "error when reading message");
                break;
            }
        };
        tracing::trace!("Received message: {:?}", msg);
        let ret = pg_proto.process(msg).await;
        if ret {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use risingwave_common::types::DataType;
    use risingwave_sqlparser::ast::Statement;
    use tokio_postgres::NoTls;

    use crate::pg_field_descriptor::PgFieldDescriptor;
    use crate::pg_message::TransactionStatus;
    use crate::pg_response::{PgResponse, RowSetResult, StatementType};
    use crate::pg_server::{
        pg_serve, BoxedError, ExecContext, ExecContextGuard, Session, SessionId, SessionManager,
        UserAuthenticator,
    };
    use crate::types;
    use crate::types::Row;

    struct MockSessionManager {}
    struct MockSession {}

    impl SessionManager for MockSessionManager {
        type Session = MockSession;

        fn connect(
            &self,
            _database: &str,
            _user_name: &str,
            _peer_addr: crate::net::AddressRef,
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

    impl Session for MockSession {
        type Portal = String;
        type PreparedStatement = String;
        type ValuesStream = BoxStream<'static, RowSetResult>;

        async fn run_one_query(
            self: Arc<Self>,
            _stmt: Statement,
            _format: types::Format,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, BoxedError> {
            Ok(PgResponse::builder(StatementType::SELECT)
                .values(
                    futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])])
                        .boxed(),
                    vec![
                        // 1043 is the oid of varchar type.
                        // -1 is the type len of varchar type.
                        PgFieldDescriptor::new("".to_string(), 1043, -1);
                        1
                    ],
                )
                .into())
        }

        fn parse(
            self: Arc<Self>,
            _sql: Option<Statement>,
            _params_types: Vec<Option<DataType>>,
        ) -> Result<String, BoxedError> {
            Ok(String::new())
        }

        fn bind(
            self: Arc<Self>,
            _prepare_statement: String,
            _params: Vec<Option<Bytes>>,
            _param_formats: Vec<types::Format>,
            _result_formats: Vec<types::Format>,
        ) -> Result<String, BoxedError> {
            Ok(String::new())
        }

        async fn execute(
            self: Arc<Self>,
            _portal: String,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, BoxedError> {
            Ok(PgResponse::builder(StatementType::SELECT)
                .values(
                    futures::stream::iter(vec![Ok(vec![Row::new(vec![Some(Bytes::new())])])])
                        .boxed(),
                    vec![
                    // 1043 is the oid of varchar type.
                    // -1 is the type len of varchar type.
                    PgFieldDescriptor::new("".to_string(), 1043, -1);
                    1
                ],
                )
                .into())
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

        fn describe_portal(
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

        fn set_config(&self, _key: &str, _value: String) -> Result<(), BoxedError> {
            Ok(())
        }

        fn take_notices(self: Arc<Self>) -> Vec<String> {
            vec![]
        }

        fn transaction_status(&self) -> TransactionStatus {
            TransactionStatus::Idle
        }

        fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard {
            let exec_context = Arc::new(ExecContext {
                running_sql: sql,
                last_instant: Instant::now(),
            });
            ExecContextGuard::new(exec_context)
        }
    }

    async fn do_test_query(bind_addr: impl Into<String>, pg_config: impl Into<String>) {
        let bind_addr = bind_addr.into();
        let pg_config = pg_config.into();

        let session_mgr = Arc::new(MockSessionManager {});
        tokio::spawn(async move { pg_serve(&bind_addr, session_mgr, None).await });
        // wait for server to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect to the database.
        let (client, connection) = tokio_postgres::connect(&pg_config, NoTls).await.unwrap();

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

    #[tokio::test]
    async fn test_query_tcp() {
        do_test_query("127.0.0.1:10000", "host=localhost port=10000").await;
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_query_unix() {
        let port: i16 = 10000;
        let dir = tempfile::TempDir::new().unwrap();
        let sock = dir.path().join(format!(".s.PGSQL.{port}"));

        do_test_query(
            format!("unix:{}", sock.to_str().unwrap()),
            format!("host={} port={}", dir.path().to_str().unwrap(), port),
        )
        .await;
    }
}
