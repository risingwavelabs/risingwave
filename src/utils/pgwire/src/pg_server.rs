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

use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use parking_lot::Mutex;
use risingwave_common::types::DataType;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_jni_core::jvm_runtime::register_jvm_builder;
use risingwave_sqlparser::ast::Statement;
use serde::Deserialize;
use thiserror_ext::AsReport;

use crate::error::{PsqlError, PsqlResult};
use crate::net::{AddressRef, Listener, TcpKeepalive};
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::TransactionStatus;
use crate::pg_protocol::{ConnectionContext, PgByteStream, PgProtocol};
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

    /// In the process of auto schema change, we need a dummy session to access
    /// catalog information in frontend and build a replace plan for the table.
    fn create_dummy_session(
        &self,
        database_id: u32,
        user_id: u32,
    ) -> Result<Arc<Self::Session>, BoxedError>;

    fn connect(
        &self,
        database: &str,
        user_name: &str,
        peer_addr: AddressRef,
    ) -> Result<Arc<Self::Session>, BoxedError>;

    fn cancel_queries_in_session(&self, session_id: SessionId);

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId);

    fn end_session(&self, session: &Self::Session);

    /// Run some cleanup tasks before the server shutdown.
    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
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
    ) -> impl Future<Output = Result<Self::PreparedStatement, BoxedError>> + Send;

    /// Receive the next notice message to send to the client.
    ///
    /// This function should be cancellation-safe.
    fn next_notice(self: &Arc<Self>) -> impl Future<Output = String> + Send;

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

    fn get_config(&self, key: &str) -> Result<String, BoxedError>;

    fn set_config(&self, key: &str, value: String) -> Result<String, BoxedError>;

    fn transaction_status(&self) -> TransactionStatus;

    fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard;

    fn check_idle_in_transaction_timeout(&self) -> PsqlResult<()>;
}

/// Each session could run different SQLs multiple times.
/// `ExecContext` represents the lifetime of a running SQL in the current session.
pub struct ExecContext {
    pub running_sql: Arc<str>,
    /// The instant of the running sql
    pub last_instant: Instant,
    /// A reference used to update when `ExecContext` is dropped
    pub last_idle_instant: Arc<Mutex<Option<Instant>>>,
}

/// `ExecContextGuard` holds a `Arc` pointer. Once `ExecContextGuard` is dropped,
/// the inner `Arc<ExecContext>` should not be referred anymore, so that its `Weak` reference (used in `SessionImpl`) will be the same lifecycle of the running sql execution context.
pub struct ExecContextGuard(#[allow(dead_code)] Arc<ExecContext>);

impl ExecContextGuard {
    pub fn new(exec_context: Arc<ExecContext>) -> Self {
        Self(exec_context)
    }
}

impl Drop for ExecContext {
    fn drop(&mut self) {
        *self.last_idle_instant.lock() = Some(Instant::now());
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
    OAuth(HashMap<String, String>),
}

/// A JWK Set is a JSON object that represents a set of JWKs.
/// The JSON object MUST have a "keys" member, with its value being an array of JWKs.
/// See <https://www.rfc-editor.org/rfc/rfc7517.html#section-5> for more details.
#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// A JSON Web Key (JWK) is a JSON object that represents a cryptographic key.
/// See <https://www.rfc-editor.org/rfc/rfc7517.html#section-4> for more details.
#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String, // Key ID
    alg: String, // Algorithm
    n: String,   // Modulus
    e: String,   // Exponent
}

async fn validate_jwt(
    jwt: &str,
    jwks_url: &str,
    issuer: &str,
    metadata: &HashMap<String, String>,
) -> Result<bool, BoxedError> {
    let header = decode_header(jwt)?;
    let jwks: Jwks = reqwest::get(jwks_url).await?.json().await?;

    // 1. Retrieve the kid from the header to find the right JWK in the JWK Set.
    let kid = header.kid.ok_or("kid not found in jwt header")?;
    let jwk = jwks
        .keys
        .into_iter()
        .find(|k| k.kid == kid)
        .ok_or("kid not found in jwks")?;

    // 2. Check if the algorithms are matched.
    if Algorithm::from_str(&jwk.alg)? != header.alg {
        return Err("alg in jwt header does not match with alg in jwk".into());
    }

    // 3. Decode the JWT and validate the claims.
    let decoding_key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e)?;
    let mut validation = Validation::new(header.alg);
    validation.set_issuer(&[issuer]);
    validation.set_required_spec_claims(&["exp", "iss"]);
    let token_data = decode::<HashMap<String, serde_json::Value>>(jwt, &decoding_key, &validation)?;

    // 4. Check if the metadata in the token matches.
    if !metadata.iter().all(
        |(k, v)| matches!(token_data.claims.get(k), Some(serde_json::Value::String(s)) if s == v),
    ) {
        return Err("metadata in jwt does not match with metadata declared with user".into());
    }
    Ok(true)
}

impl UserAuthenticator {
    pub async fn authenticate(&self, password: &[u8]) -> PsqlResult<()> {
        let success = match self {
            UserAuthenticator::None => true,
            UserAuthenticator::ClearText(text) => password == text,
            UserAuthenticator::Md5WithSalt {
                encrypted_password, ..
            } => encrypted_password == password,
            UserAuthenticator::OAuth(metadata) => {
                let mut metadata = metadata.clone();
                let jwks_url = metadata.remove("jwks_url").unwrap();
                let issuer = metadata.remove("issuer").unwrap();
                validate_jwt(
                    &String::from_utf8_lossy(password),
                    &jwks_url,
                    &issuer,
                    &metadata,
                )
                .await
                .map_err(PsqlError::StartupError)?
            }
        };
        if !success {
            return Err(PsqlError::PasswordError);
        }
        Ok(())
    }
}

/// Binds a Tcp or Unix listener at `addr`. Spawn a coroutine to serve every new connection.
///
/// Returns when the `shutdown` token is triggered.
pub async fn pg_serve(
    addr: &str,
    tcp_keepalive: TcpKeepalive,
    session_mgr: Arc<impl SessionManager>,
    context: ConnectionContext,
    shutdown: CancellationToken,
) -> Result<(), BoxedError> {
    let listener = Listener::bind(addr).await?;
    tracing::info!(addr, "server started");
    register_jvm_builder();
    let acceptor_runtime = BackgroundShutdownRuntime::from({
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(1);
        builder
            .thread_name("rw-acceptor")
            .enable_all()
            .build()
            .unwrap()
    });

    #[cfg(not(madsim))]
    let worker_runtime = tokio::runtime::Handle::current();
    #[cfg(madsim)]
    let worker_runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let session_mgr_clone = session_mgr.clone();
    let f = async move {
        loop {
            let conn_ret = listener.accept(&tcp_keepalive).await;
            match conn_ret {
                Ok((stream, peer_addr)) => {
                    tracing::info!(%peer_addr, "accept connection");
                    worker_runtime.spawn(handle_connection(
                        stream,
                        session_mgr_clone.clone(),
                        Arc::new(peer_addr),
                        context.clone(),
                    ));
                }

                Err(e) => {
                    tracing::error!(error = %e.as_report(), "failed to accept connection",);
                }
            }
        }
    };
    acceptor_runtime.spawn(f);

    // Wait for the shutdown signal.
    shutdown.cancelled().await;

    // Stop accepting new connections.
    drop(acceptor_runtime);
    // Shutdown session manager, typically close all existing sessions.
    session_mgr.shutdown().await;

    Ok(())
}

pub async fn handle_connection<S, SM>(
    stream: S,
    session_mgr: Arc<SM>,
    peer_addr: AddressRef,
    context: ConnectionContext,
) where
    S: PgByteStream,
    SM: SessionManager,
{
    PgProtocol::new(stream, session_mgr, peer_addr, context)
        .run()
        .await;
}
#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;
    use std::time::Instant;

    use bytes::Bytes;
    use futures::StreamExt;
    use futures::stream::BoxStream;
    use risingwave_common::types::DataType;
    use risingwave_common::util::tokio_util::sync::CancellationToken;
    use risingwave_sqlparser::ast::Statement;
    use tokio_postgres::NoTls;

    use crate::error::PsqlResult;
    use crate::memory_manager::MessageMemoryManager;
    use crate::pg_field_descriptor::PgFieldDescriptor;
    use crate::pg_message::TransactionStatus;
    use crate::pg_protocol::ConnectionContext;
    use crate::pg_response::{PgResponse, RowSetResult, StatementType};
    use crate::pg_server::{
        BoxedError, ExecContext, ExecContextGuard, Session, SessionId, SessionManager,
        UserAuthenticator, pg_serve,
    };
    use crate::types;
    use crate::types::Row;

    struct MockSessionManager {}
    struct MockSession {}

    impl SessionManager for MockSessionManager {
        type Session = MockSession;

        fn create_dummy_session(
            &self,
            _database_id: u32,
            _user_name: u32,
        ) -> Result<Arc<Self::Session>, BoxedError> {
            unimplemented!()
        }

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
                        PgFieldDescriptor::new("".to_owned(), 1043, -1);
                        1
                    ],
                )
                .into())
        }

        async fn parse(
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
                    PgFieldDescriptor::new("".to_owned(), 1043, -1);
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
                vec![PgFieldDescriptor::new("".to_owned(), 1043, -1)],
            ))
        }

        fn describe_portal(
            self: Arc<Self>,
            _portal: String,
        ) -> Result<Vec<PgFieldDescriptor>, BoxedError> {
            Ok(vec![PgFieldDescriptor::new("".to_owned(), 1043, -1)])
        }

        fn user_authenticator(&self) -> &UserAuthenticator {
            &UserAuthenticator::None
        }

        fn id(&self) -> SessionId {
            (0, 0)
        }

        fn get_config(&self, key: &str) -> Result<String, BoxedError> {
            match key {
                "timezone" => Ok("UTC".to_owned()),
                _ => Err(format!("Unknown config key: {key}").into()),
            }
        }

        fn set_config(&self, _key: &str, _value: String) -> Result<String, BoxedError> {
            Ok("".to_owned())
        }

        async fn next_notice(self: &Arc<Self>) -> String {
            std::future::pending().await
        }

        fn transaction_status(&self) -> TransactionStatus {
            TransactionStatus::Idle
        }

        fn init_exec_context(&self, sql: Arc<str>) -> ExecContextGuard {
            let exec_context = Arc::new(ExecContext {
                running_sql: sql,
                last_instant: Instant::now(),
                last_idle_instant: Default::default(),
            });
            ExecContextGuard::new(exec_context)
        }

        fn check_idle_in_transaction_timeout(&self) -> PsqlResult<()> {
            Ok(())
        }
    }

    async fn do_test_query(bind_addr: impl Into<String>, pg_config: impl Into<String>) {
        let bind_addr = bind_addr.into();
        let pg_config = pg_config.into();

        let session_mgr = MockSessionManager {};
        tokio::spawn(async move {
            pg_serve(
                &bind_addr,
                socket2::TcpKeepalive::new(),
                Arc::new(session_mgr),
                ConnectionContext {
                    tls_config: None,
                    redact_sql_option_keywords: None,
                    message_memory_manager: MessageMemoryManager::new(u64::MAX, u64::MAX, u64::MAX)
                        .into(),
                },
                CancellationToken::new(), // dummy
            )
            .await
        });
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
