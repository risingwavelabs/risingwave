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

use std::collections::HashMap;
use std::io::{self, Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::pin::Pin;
use std::str;
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use futures::Stream;
use itertools::Itertools;
use openssl::ssl::{SslAcceptor, SslContext, SslContextRef, SslMethod};
use risingwave_common::types::DataType;
use risingwave_sqlparser::parser::Parser;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_openssl::SslStream;
use tracing::{error, warn};

use crate::error::{PsqlError, PsqlResult};
use crate::pg_extended::ResultCache;
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeBindMessage, FeCancelMessage,
    FeCloseMessage, FeDescribeMessage, FeExecuteMessage, FeMessage, FeParseMessage,
    FePasswordMessage, FeStartupMessage,
};
use crate::pg_response::RowSetResult;
use crate::pg_server::{Session, SessionManager, UserAuthenticator};
use crate::types::Format;

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM, VS, PS, PO>
where
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
    SM: SessionManager<VS, PS, PO>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    /// Used for write/read pg messages.
    stream: Conn<S>,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,

    result_cache: HashMap<String, ResultCache<VS>>,
    unnamed_prepare_statement: Option<PS>,
    prepare_statement_store: HashMap<String, PS>,
    unnamed_portal: Option<PO>,
    portal_store: HashMap<String, PO>,
    // Used to store the dependency of portal and prepare statement.
    // When we close a prepare statement, we need to close all the portals that depend on it.
    statement_portal_dependency: HashMap<String, Vec<String>>,

    // Used for ssl connection.
    // If None, not expected to build ssl connection (panic).
    tls_context: Option<SslContext>,
}

const PGWIRE_QUERY_LOG: &str = "pgwire_query_log";

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

impl TlsConfig {
    pub fn new_default() -> Self {
        let cert = PathBuf::new().join("tests/ssl/demo.crt");
        let key = PathBuf::new().join("tests/ssl/demo.key");
        let path_to_cur_proj = PathBuf::new().join("src/utils/pgwire");

        Self {
            // Now the demo crt and key are hard code generated via simple self-signed CA.
            // In future it should change to configure by user.
            // The path is mounted from project root.
            cert: path_to_cur_proj.join(cert),
            key: path_to_cur_proj.join(key),
        }
    }
}

impl<S, SM, VS, PS, PO> Drop for PgProtocol<S, SM, VS, PS, PO>
where
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
    SM: SessionManager<VS, PS, PO>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            // Clear the session in session manager.
            self.session_mgr.end_session(session);
        }
    }
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

/// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations).
///
/// PG protocol strings are always C strings.
pub fn cstr_to_str(b: &Bytes) -> Result<&str, Utf8Error> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null)
}

impl<S, SM, VS, PS, PO> PgProtocol<S, SM, VS, PS, PO>
where
    PS: Send + Clone + 'static,
    PO: Send + Clone + std::fmt::Display + 'static,
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager<VS, PS, PO>,
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    pub fn new(stream: S, session_mgr: Arc<SM>, tls_config: Option<TlsConfig>) -> Self {
        Self {
            stream: Conn::Unencrypted(PgStream {
                stream: Some(stream),
                write_buf: BytesMut::with_capacity(10 * 1024),
            }),
            is_terminate: false,
            state: PgProtocolState::Startup,
            session_mgr,
            session: None,
            tls_context: tls_config
                .as_ref()
                .and_then(|e| build_ssl_ctx_from_config(e).ok()),
            result_cache: Default::default(),
            unnamed_prepare_statement: Default::default(),
            prepare_statement_store: Default::default(),
            unnamed_portal: Default::default(),
            portal_store: Default::default(),
            statement_portal_dependency: Default::default(),
        }
    }

    /// Processes one message. Returns true if the connection is terminated.
    pub async fn process(&mut self, msg: FeMessage) -> bool {
        self.do_process(msg).await || self.is_terminate
    }

    async fn do_process(&mut self, msg: FeMessage) -> bool {
        match self.do_process_inner(msg).await {
            Ok(v) => v,
            Err(e) => {
                match e {
                    PsqlError::IoError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return true;
                        }
                    }

                    PsqlError::SslError(e) => {
                        // For ssl error, because the stream has already been consumed, so there is
                        // no way to write more message.
                        error!("SSL connection setup error: {}", e);
                        return true;
                    }

                    PsqlError::StartupError(_) | PsqlError::PasswordError(_) => {
                        // TODO: Fix the unwrap in this stream.
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                        self.stream.flush().await.unwrap_or_else(|e| {
                            tracing::error!("flush error: {}", e);
                        });
                        return true;
                    }

                    PsqlError::QueryError(_) => {
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                        self.stream
                            .write_no_flush(&BeMessage::ReadyForQuery)
                            .unwrap();
                    }

                    PsqlError::Internal(_)
                    | PsqlError::ParseError(_)
                    | PsqlError::ExecuteError(_) => {
                        self.stream
                            .write_no_flush(&BeMessage::ErrorResponse(Box::new(e)))
                            .unwrap();
                    }
                }
                self.stream.flush().await.unwrap_or_else(|e| {
                    tracing::error!("flush error: {}", e);
                });
                false
            }
        }
    }

    async fn do_process_inner(&mut self, msg: FeMessage) -> PsqlResult<bool> {
        match msg {
            FeMessage::Ssl => self.process_ssl_msg().await?,
            FeMessage::Startup(msg) => self.process_startup_msg(msg)?,
            FeMessage::Password(msg) => self.process_password_msg(msg)?,
            FeMessage::Query(query_msg) => self.process_query_msg(query_msg.get_sql()).await?,
            FeMessage::CancelQuery(m) => self.process_cancel_msg(m)?,
            FeMessage::Terminate => self.process_terminate(),
            FeMessage::Parse(m) => self.process_parse_msg(m)?,
            FeMessage::Bind(m) => self.process_bind_msg(m)?,
            FeMessage::Execute(m) => self.process_execute_msg(m).await?,
            FeMessage::Describe(m) => self.process_describe_msg(m)?,
            FeMessage::Sync => self.stream.write_no_flush(&BeMessage::ReadyForQuery)?,
            FeMessage::Close(m) => self.process_close_msg(m)?,
            FeMessage::Flush => self.stream.flush().await?,
        }
        self.stream.flush().await?;
        Ok(false)
    }

    pub async fn read_message(&mut self) -> io::Result<FeMessage> {
        match self.state {
            PgProtocolState::Startup => self.stream.read_startup().await,
            PgProtocolState::Regular => self.stream.read().await,
        }
    }

    async fn process_ssl_msg(&mut self) -> PsqlResult<()> {
        if let Some(context) = self.tls_context.as_ref() {
            // If got and ssl context, say yes for ssl connection.
            // Construct ssl stream and replace with current one.
            self.stream.write(&BeMessage::EncryptionResponseYes).await?;
            let ssl_stream = self.stream.ssl(context).await?;
            self.stream = Conn::Ssl(ssl_stream);
        } else {
            // If no, say no for encryption.
            self.stream.write(&BeMessage::EncryptionResponseNo).await?;
        }

        Ok(())
    }

    fn process_startup_msg(&mut self, msg: FeStartupMessage) -> PsqlResult<()> {
        let db_name = msg
            .config
            .get("database")
            .cloned()
            .unwrap_or_else(|| "dev".to_string());
        let user_name = msg
            .config
            .get("user")
            .cloned()
            .unwrap_or_else(|| "root".to_string());

        let session = self
            .session_mgr
            .connect(&db_name, &user_name)
            .map_err(PsqlError::StartupError)?;
        match session.user_authenticator() {
            UserAuthenticator::None => {
                self.stream.write_no_flush(&BeMessage::AuthenticationOk)?;

                // Cancel request need this for identify and verification. According to postgres
                // doc, it should be written to buffer after receive AuthenticationOk.
                self.stream
                    .write_no_flush(&BeMessage::BackendKeyData(session.id()))?;

                self.stream.write_parameter_status_msg_no_flush()?;
                self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
            }
            UserAuthenticator::ClearText(_) => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationCleartextPassword)?;
            }
            UserAuthenticator::Md5WithSalt { salt, .. } => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationMd5Password(salt))?;
            }
        }
        self.session = Some(session);
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_password_msg(&mut self, msg: FePasswordMessage) -> PsqlResult<()> {
        let authenticator = self.session.as_ref().unwrap().user_authenticator();
        if !authenticator.authenticate(&msg.password) {
            return Err(PsqlError::PasswordError(IoError::new(
                ErrorKind::InvalidInput,
                "Invalid password",
            )));
        }
        self.stream.write_no_flush(&BeMessage::AuthenticationOk)?;
        self.stream.write_parameter_status_msg_no_flush()?;
        self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_cancel_msg(&mut self, m: FeCancelMessage) -> PsqlResult<()> {
        let session_id = (m.target_process_id, m.target_secret_key);
        tracing::trace!("cancel query in session: {:?}", session_id);
        self.session_mgr.cancel_queries_in_session(session_id);
        self.session_mgr.cancel_creating_jobs_in_session(session_id);
        self.stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
        Ok(())
    }

    async fn process_query_msg(&mut self, query_string: io::Result<&str>) -> PsqlResult<()> {
        let sql = query_string.map_err(|err| PsqlError::QueryError(Box::new(err)))?;
        let start = Instant::now();
        let session = self.session.clone().unwrap();
        let session_id = session.id().0;
        let result = self.inner_process_query_msg(sql, session).await;

        let mills = start.elapsed().as_millis();
        let truncated_sql = &sql[..std::cmp::min(sql.len(), 1024)];
        tracing::info!(
            target: PGWIRE_QUERY_LOG,
            mode = %"(simple query)",
            session = %session_id,
            status = %if result.is_ok() { "ok" } else { "err" },
            time = %format!("{}ms", mills),
            sql = %truncated_sql,
        );

        result
    }

    async fn inner_process_query_msg(
        &mut self,
        sql: &str,
        session: Arc<SM::Session>,
    ) -> PsqlResult<()> {
        // Parse sql.
        let stmts = Parser::parse_sql(sql)
            .inspect_err(|e| tracing::error!("failed to parse sql:\n{}:\n{}", sql, e))
            .map_err(|err| PsqlError::QueryError(err.into()))?;

        // Execute multiple statements in simple query. KISS later.
        for stmt in stmts {
            let session = session.clone();
            // execute query
            let res = session.clone().run_one_query(stmt, Format::Text).await;
            for notice in session.take_notices() {
                self.stream
                    .write_no_flush(&BeMessage::NoticeResponse(&notice))?;
            }
            let mut res = res.map_err(|err| PsqlError::QueryError(err))?;

            for notice in res.get_notices() {
                self.stream
                    .write_no_flush(&BeMessage::NoticeResponse(notice))?;
            }

            if res.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&res.get_row_desc()))?;

                let mut rows_cnt = 0;

                while let Some(row_set) = res.values_stream().next().await {
                    let row_set = row_set.map_err(|err| PsqlError::QueryError(err))?;
                    for row in row_set {
                        self.stream.write_no_flush(&BeMessage::DataRow(&row))?;
                        rows_cnt += 1;
                    }
                }

                // Run the callback before sending the `CommandComplete` message.
                res.run_callback().await?;

                self.stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: res.get_stmt_type(),
                        rows_cnt,
                    },
                ))?;
            } else {
                // Run the callback before sending the `CommandComplete` message.
                res.run_callback().await?;

                self.stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: res.get_stmt_type(),
                        rows_cnt: res
                            .get_effected_rows_cnt()
                            .expect("row count should be set"),
                    },
                ))?;
            }
        }
        // Put this line inside the for loop above will lead to unfinished/stuck regress test...Not
        // sure the reason.
        self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    fn process_parse_msg(&mut self, msg: FeParseMessage) -> PsqlResult<()> {
        let sql = cstr_to_str(&msg.sql_bytes).unwrap();
        let session = self.session.clone().unwrap();
        let session_id = session.id().0;
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_string();
        let start = Instant::now();

        let result = self.inner_process_parse_msg(session, sql, statement_name, msg.type_ids);

        let mills = start.elapsed().as_millis();
        let truncated_sql = &sql[..std::cmp::min(sql.len(), 1024)];
        tracing::info!(
            target: PGWIRE_QUERY_LOG,
            mode = %"(extended query parse)",
            session = %session_id,
            status = %if result.is_ok() { "ok" } else { "err" },
            time = %format!("{}ms", mills),
            sql = %truncated_sql,
        );

        result
    }

    fn inner_process_parse_msg(
        &mut self,
        session: Arc<SM::Session>,
        sql: &str,
        statement_name: String,
        type_ids: Vec<i32>,
    ) -> PsqlResult<()> {
        if self.prepare_statement_store.contains_key(&statement_name) {
            return Err(PsqlError::ParseError("Duplicated statement name".into()));
        }

        let stmt = {
            let stmts = Parser::parse_sql(sql)
                .inspect_err(|e| tracing::error!("failed to parse sql:\n{}:\n{}", sql, e))
                .map_err(|err| PsqlError::ParseError(err.into()))?;

            if stmts.len() > 1 {
                return Err(PsqlError::ParseError(
                    "Only one statement is allowed in extended query mode".into(),
                ));
            }

            // TODO: This behavior is not compatible with Postgres.
            if stmts.is_empty() {
                return Err(PsqlError::ParseError(
                    "Empty statement is parsed in extended query mode".into(),
                ));
            }

            stmts.into_iter().next().unwrap()
        };

        let param_types = type_ids
            .iter()
            .map(|&id| DataType::from_oid(id))
            .try_collect()
            .map_err(|err| PsqlError::ParseError(err.into()))?;

        let prepare_statement = session
            .parse(stmt, param_types)
            .map_err(PsqlError::ParseError)?;

        if statement_name.is_empty() {
            self.unnamed_prepare_statement.replace(prepare_statement);
        } else {
            self.prepare_statement_store
                .insert(statement_name.clone(), prepare_statement);
        }

        self.statement_portal_dependency
            .entry(statement_name)
            .or_insert_with(Vec::new)
            .clear();

        self.stream.write_no_flush(&BeMessage::ParseComplete)?;
        Ok(())
    }

    fn process_bind_msg(&mut self, msg: FeBindMessage) -> PsqlResult<()> {
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_string();
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let session = self.session.clone().unwrap();

        if self.portal_store.contains_key(&portal_name) {
            return Err(PsqlError::Internal("Duplicated portal name".into()));
        }

        let prepare_statement = self.get_statement(&statement_name)?;

        let result_formats = msg
            .result_format_codes
            .iter()
            .map(|&format_code| Format::from_i16(format_code))
            .try_collect()?;
        let param_formats = msg
            .param_format_codes
            .iter()
            .map(|&format_code| Format::from_i16(format_code))
            .try_collect()?;

        let portal = session
            .bind(prepare_statement, msg.params, param_formats, result_formats)
            .map_err(PsqlError::Internal)?;

        if portal_name.is_empty() {
            self.result_cache.remove(&portal_name);
            self.unnamed_portal.replace(portal);
        } else {
            assert!(
                self.result_cache.get(&portal_name).is_none(),
                "Named portal never can be overridden."
            );
            self.portal_store.insert(portal_name.clone(), portal);
        }

        self.statement_portal_dependency
            .get_mut(&statement_name)
            .unwrap()
            .push(portal_name);

        self.stream.write_no_flush(&BeMessage::BindComplete)?;
        Ok(())
    }

    async fn process_execute_msg(&mut self, msg: FeExecuteMessage) -> PsqlResult<()> {
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let row_max = msg.max_rows as usize;
        let session = self.session.clone().unwrap();
        let session_id = session.id().0;

        if let Some(mut result_cache) = self.result_cache.remove(&portal_name) {
            assert!(self.portal_store.contains_key(&portal_name));

            let is_cosume_completed = result_cache.consume::<S>(row_max, &mut self.stream).await?;

            if !is_cosume_completed {
                self.result_cache.insert(portal_name, result_cache);
            }
        } else {
            let start = Instant::now();
            let portal = self.get_portal(&portal_name)?;
            let sql = format!("{}", portal);
            let truncated_sql = &sql[..std::cmp::min(sql.len(), 1024)];

            let result = session.execute(portal).await;

            let mills = start.elapsed().as_millis();

            tracing::info!(
                target: PGWIRE_QUERY_LOG,
                mode = %"(extended query execute)",
                session = %session_id,
                status = %if result.is_ok() { "ok" } else { "err" },
                time = %format!("{}ms", mills),
                sql = %truncated_sql,
            );

            let pg_response = match result {
                Ok(pg_response) => pg_response,
                Err(err) => return Err(PsqlError::ExecuteError(err)),
            };
            let mut result_cache = ResultCache::new(pg_response);
            let is_consume_completed = result_cache.consume::<S>(row_max, &mut self.stream).await?;
            if !is_consume_completed {
                self.result_cache.insert(portal_name, result_cache);
            }
        }

        Ok(())
    }

    fn process_describe_msg(&mut self, msg: FeDescribeMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_string();
        let session = self.session.clone().unwrap();
        //  b'S' => Statement
        //  b'P' => Portal

        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            let prepare_statement = self.get_statement(&name)?;

            let (param_types, row_descriptions) = self
                .session
                .clone()
                .unwrap()
                .describe_statement(prepare_statement)
                .map_err(PsqlError::Internal)?;

            self.stream
                .write_no_flush(&BeMessage::ParameterDescription(
                    &param_types.iter().map(|t| t.to_oid()).collect_vec(),
                ))?;

            if row_descriptions.is_empty() {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            } else {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&row_descriptions))?;
            }
        } else if msg.kind == b'P' {
            let portal = self.get_portal(&name)?;

            let row_descriptions = session
                .describe_portral(portal)
                .map_err(PsqlError::Internal)?;

            if row_descriptions.is_empty() {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            } else {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&row_descriptions))?;
            }
        }
        Ok(())
    }

    fn process_close_msg(&mut self, msg: FeCloseMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_string();
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            if name.is_empty() {
                self.unnamed_prepare_statement = None;
            } else {
                self.prepare_statement_store.remove(&name);
            }
            for portal_name in self
                .statement_portal_dependency
                .remove(&name)
                .unwrap_or(vec![])
            {
                self.remove_portal(&portal_name);
            }
        } else if msg.kind == b'P' {
            self.remove_portal(&name);
        }
        self.stream.write_no_flush(&BeMessage::CloseComplete)?;
        Ok(())
    }

    fn remove_portal(&mut self, portal_name: &str) {
        if portal_name.is_empty() {
            self.unnamed_portal = None;
        } else {
            self.portal_store.remove(portal_name);
        }
        self.result_cache.remove(portal_name);
    }

    fn get_portal(&self, portal_name: &str) -> PsqlResult<PO> {
        if portal_name.is_empty() {
            Ok(self
                .unnamed_portal
                .as_ref()
                .ok_or_else(|| PsqlError::Internal("unnamed portal not found".into()))?
                .clone())
        } else {
            Ok(self
                .portal_store
                .get(portal_name)
                .ok_or_else(|| {
                    PsqlError::Internal(format!("Portal {} not found", portal_name).into())
                })?
                .clone())
        }
    }

    fn get_statement(&self, statement_name: &str) -> PsqlResult<PS> {
        if statement_name.is_empty() {
            Ok(self
                .unnamed_prepare_statement
                .as_ref()
                .ok_or_else(|| PsqlError::Internal("unnamed prepare statement not found".into()))?
                .clone())
        } else {
            Ok(self
                .prepare_statement_store
                .get(statement_name)
                .ok_or_else(|| {
                    PsqlError::Internal(
                        format!("Prepare statement {} not found", statement_name).into(),
                    )
                })?
                .clone())
        }
    }
}

/// Wraps a byte stream and read/write pg messages.
pub struct PgStream<S> {
    /// The underlying stream.
    stream: Option<S>,
    /// Write into buffer before flush to stream.
    write_buf: BytesMut,
}

impl<S> PgStream<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        FeStartupMessage::read(self.stream()).await
    }

    async fn read(&mut self) -> io::Result<FeMessage> {
        FeMessage::read(self.stream()).await
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> io::Result<()> {
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding("UTF8"),
        ))?;
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion("9.5.0"),
        ))?;
        Ok(())
    }

    pub fn write_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        BeMessage::write(&mut self.write_buf, message)
    }

    async fn write(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        self.write_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.stream
            .as_mut()
            .unwrap()
            .write_all(&self.write_buf)
            .await?;
        self.write_buf.clear();
        self.stream.as_mut().unwrap().flush().await?;
        Ok(())
    }

    fn stream(&mut self) -> &mut (impl AsyncRead + Unpin + AsyncWrite) {
        self.stream.as_mut().unwrap()
    }
}

/// The logic of Conn is very simple, just a static dispatcher for TcpStream: Unencrypted or Ssl:
/// Encrypted.
pub enum Conn<S> {
    Unencrypted(PgStream<S>),
    Ssl(PgStream<SslStream<S>>),
}

impl<S> PgStream<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn ssl(&mut self, ssl_ctx: &SslContextRef) -> PsqlResult<PgStream<SslStream<S>>> {
        // Note: Currently we take the ownership of previous Tcp Stream and then turn into a
        // SslStream. Later we can avoid storing stream inside PgProtocol to do this more
        // fluently.
        let stream = self.stream.take().unwrap();
        let ssl = openssl::ssl::Ssl::new(ssl_ctx).unwrap();
        let mut stream = tokio_openssl::SslStream::new(ssl, stream).unwrap();
        if let Err(e) = Pin::new(&mut stream).accept().await {
            warn!("Unable to set up a ssl connection, reason: {}", e);
            let _ = stream.shutdown().await;
            return Err(PsqlError::SslError(e.to_string()));
        }

        Ok(PgStream {
            stream: Some(stream),
            write_buf: BytesMut::with_capacity(10 * 1024),
        })
    }
}

impl<S> Conn<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        match self {
            Conn::Unencrypted(s) => s.read_startup().await,
            Conn::Ssl(s) => s.read_startup().await,
        }
    }

    async fn read(&mut self) -> io::Result<FeMessage> {
        match self {
            Conn::Unencrypted(s) => s.read().await,
            Conn::Ssl(s) => s.read().await,
        }
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write_parameter_status_msg_no_flush(),
            Conn::Ssl(s) => s.write_parameter_status_msg_no_flush(),
        }
    }

    pub fn write_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write_no_flush(message),
            Conn::Ssl(s) => s.write_no_flush(message),
        }
        .map_err(|e| {
            tracing::error!("flush error: {}", e);
            e
        })
    }

    async fn write(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.write(message).await,
            Conn::Ssl(s) => s.write(message).await,
        }
    }

    async fn flush(&mut self) -> io::Result<()> {
        match self {
            Conn::Unencrypted(s) => s.flush().await,
            Conn::Ssl(s) => s.flush().await,
        }
    }

    async fn ssl(&mut self, ssl_ctx: &SslContextRef) -> PsqlResult<PgStream<SslStream<S>>> {
        match self {
            Conn::Unencrypted(s) => s.ssl(ssl_ctx).await,
            Conn::Ssl(_s) => panic!("can not turn a ssl stream into a ssl stream"),
        }
    }
}

fn build_ssl_ctx_from_config(tls_config: &TlsConfig) -> PsqlResult<SslContext> {
    let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls()).unwrap();

    let key_path = &tls_config.key;
    let cert_path = &tls_config.cert;

    // Build ssl acceptor according to the config.
    // Now we set every verify to true.
    acceptor
        .set_private_key_file(key_path, openssl::ssl::SslFiletype::PEM)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    acceptor
        .set_ca_file(cert_path)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    acceptor
        .set_certificate_chain_file(cert_path)
        .map_err(|e| PsqlError::Internal(e.into()))?;
    let acceptor = acceptor.build();

    Ok(acceptor.into_context())
}
