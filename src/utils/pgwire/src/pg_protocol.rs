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

use std::collections::HashMap;
use std::fmt::Write;
use std::io::{self, Error as IoError, ErrorKind};
use std::str::Utf8Error;
use std::sync::Arc;
use std::{str, vec};

use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::log::trace;

use crate::error::{PsqlError, PsqlResult};
use crate::pg_extended::{PgPortal, PgStatement, PreparedStatement};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeBindMessage, FeCancelMessage,
    FeCloseMessage, FeDescribeMessage, FeExecuteMessage, FeMessage, FeParseMessage,
    FePasswordMessage, FeStartupMessage,
};
use crate::pg_server::{Session, SessionManager, UserAuthenticator};

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM>
where
    SM: SessionManager,
{
    /// Used for write/read pg messages.
    stream: PgStream<S>,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,

    unnamed_statement: Option<PgStatement>,
    unnamed_portal: Option<PgPortal>,
    named_statements: HashMap<String, PgStatement>,
    named_portals: HashMap<String, PgPortal>,
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

impl<S, SM> PgProtocol<S, SM>
where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager,
{
    pub fn new(stream: S, session_mgr: Arc<SM>) -> Self {
        Self {
            stream: PgStream {
                stream,
                write_buf: BytesMut::with_capacity(10 * 1024),
            },
            is_terminate: false,
            state: PgProtocolState::Startup,
            session_mgr,
            session: None,
            unnamed_statement: None,
            unnamed_portal: None,
            named_statements: Default::default(),
            named_portals: Default::default(),
        }
    }

    /// Processes one message. Returns true if the connection is terminated.
    pub async fn process(&mut self) -> bool {
        self.do_process().await || self.is_terminate
    }

    async fn do_process(&mut self) -> bool {
        match self.do_process_inner().await {
            Ok(v) => v,
            Err(e) => {
                let mut error_msg = String::new();
                // Execution error should not break current connection.
                // For unexpected eof, just break and not print to log.
                write!(&mut error_msg, "Error: {}", e).unwrap();
                match e {
                    PsqlError::SslError(io_err) | PsqlError::IoError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            tracing::error!("{}", error_msg);
                            return true;
                        }
                    }

                    PsqlError::StartupError(_) | PsqlError::PasswordError(_) => {
                        self.stream
                            .write_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                        tracing::error!("{}", error_msg);
                        return true;
                    }

                    PsqlError::ReadMsgError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            tracing::error!("{}", error_msg);
                            return true;
                        }
                        self.stream
                            .write_for_error(&BeMessage::ErrorResponse(Box::new(io_err)));
                        self.stream.write_for_error(&BeMessage::ReadyForQuery);
                    }

                    PsqlError::QueryError(_) => {
                        self.stream
                            .write_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                        self.stream.write_for_error(&BeMessage::ReadyForQuery);
                    }

                    PsqlError::CloseError(_)
                    | PsqlError::DescribeError(_)
                    | PsqlError::ParseError(_)
                    | PsqlError::BindError(_)
                    | PsqlError::ExecuteError(_)
                    | PsqlError::CancelNotFound => {
                        self.stream
                            .write_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                    }

                    // Never reach this.
                    PsqlError::CancelMsg(_) => {
                        todo!("Support processing cancel query")
                    }
                }
                self.stream.flush_for_error().await;
                tracing::error!("{}", error_msg);
                false
            }
        }
    }

    async fn do_process_inner(&mut self) -> PsqlResult<bool> {
        let msg = self.read_message().await?;
        match msg {
            FeMessage::Ssl => self.process_ssl_msg()?,
            FeMessage::Startup(msg) => self.process_startup_msg(msg)?,
            FeMessage::Password(msg) => self.process_password_msg(msg)?,
            FeMessage::Query(query_msg) => self.process_query_msg(query_msg.get_sql()).await?,
            FeMessage::CancelQuery(m) => self.process_cancel_msg(m)?,
            FeMessage::Terminate => self.process_terminate(),
            FeMessage::Parse(m) => self.process_parse_msg(m).await?,
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

    async fn read_message(&mut self) -> PsqlResult<FeMessage> {
        match self.state {
            PgProtocolState::Startup => self.stream.read_startup().await,
            PgProtocolState::Regular => self.stream.read().await,
        }
        .map_err(PsqlError::ReadMsgError)
    }

    fn process_ssl_msg(&mut self) -> PsqlResult<()> {
        self.stream
            .write_no_flush(&BeMessage::EncryptionResponse)
            .map_err(PsqlError::SslError)?;
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
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationOk)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;

                // Cancel request need this for identify and verification. According to postgres
                // doc, it should be written to buffer after receive AuthenticationOk.
                // let id = self.session_mgr.insert_session(session.clone());
                self.stream
                    .write_no_flush(&BeMessage::BackendKeyData(session.id()))?;

                self.stream
                    .write_parameter_status_msg_no_flush()
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
                self.stream
                    .write_no_flush(&BeMessage::ReadyForQuery)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
            }
            UserAuthenticator::ClearText(_) => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationCleartextPassword)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
            }
            UserAuthenticator::MD5WithSalt { salt, .. } => {
                self.stream
                    .write_no_flush(&BeMessage::AuthenticationMD5Password(salt))
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
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
        self.stream
            .write_no_flush(&BeMessage::AuthenticationOk)
            .map_err(PsqlError::PasswordError)?;
        self.stream
            .write_parameter_status_msg_no_flush()
            .map_err(PsqlError::PasswordError)?;
        self.stream
            .write_no_flush(&BeMessage::ReadyForQuery)
            .map_err(PsqlError::PasswordError)?;
        self.state = PgProtocolState::Regular;
        Ok(())
    }

    fn process_cancel_msg(&mut self, m: FeCancelMessage) -> PsqlResult<()> {
        let session_id = (m.target_process_id, m.target_secret_key);
        self.session_mgr.cancel_queries_in_session(session_id);
        self.stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
        Ok(())
    }

    async fn process_query_msg(&mut self, query_string: io::Result<&str>) -> PsqlResult<()> {
        let sql = query_string.map_err(|err| PsqlError::QueryError(Box::new(err)))?;
        tracing::trace!("(simple query)receive query: {}", sql);

        let session = self.session.clone().unwrap();
        // execute query
        let mut res = session
            .run_statement(sql, false)
            .await
            .map_err(|err| PsqlError::QueryError(err))?;

        if res.is_empty() {
            self.stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
        } else if res.is_query() {
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

            self.stream
                .write_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.get_stmt_type(),
                    notice: res.get_notice(),
                    rows_cnt,
                }))?;
        } else {
            self.stream
                .write_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                    stmt_type: res.get_stmt_type(),
                    notice: res.get_notice(),
                    rows_cnt: res
                        .get_effected_rows_cnt()
                        .expect("row count should be set"),
                }))?;
        }

        self.stream.write_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_parse_msg(&mut self, msg: FeParseMessage) -> PsqlResult<()> {
        let sql = cstr_to_str(&msg.sql_bytes).unwrap();
        tracing::trace!("(extended query)parse query: {}", sql);
        // Create the types description.
        let types = msg
            .type_ids
            .iter()
            .map(|x| TypeOid::as_type(*x).map_err(|e| PsqlError::ParseError(Box::new(e))))
            .collect::<PsqlResult<Vec<TypeOid>>>()?;

        // Flag indicate whether statement is a query statement.
        let is_query_sql = {
            let lower_sql = sql.to_ascii_lowercase();
            lower_sql.starts_with("select")
                || lower_sql.starts_with("values")
                || lower_sql.starts_with("show")
                || lower_sql.starts_with("with")
                || lower_sql.starts_with("describe")
        };

        let prepared_statement = PreparedStatement::parse_statement(sql.to_string(), types)?;

        // 2. Create the row description.
        let fields: Vec<PgFieldDescriptor> = if is_query_sql {
            let sql = prepared_statement.instance_default()?;

            let session = self.session.clone().unwrap();
            session
                .infer_return_type(&sql)
                .await
                .map_err(PsqlError::ParseError)?
        } else {
            vec![]
        };

        // 3. Create the statement.
        let statement = PgStatement::new(
            cstr_to_str(&msg.statement_name).unwrap().to_string(),
            prepared_statement,
            fields,
            is_query_sql,
        );

        // 4. Insert the statement.
        let name = statement.name();
        if name.is_empty() {
            self.unnamed_statement.replace(statement);
        } else {
            self.named_statements.insert(name, statement);
        }
        self.stream.write_no_flush(&BeMessage::ParseComplete)?;
        Ok(())
    }

    fn process_bind_msg(&mut self, msg: FeBindMessage) -> PsqlResult<()> {
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_string();
        // 1. Get statement.
        trace!(
            "(extended query)bind: get statement name: {}",
            &statement_name
        );
        let statement = if statement_name.is_empty() {
            self.unnamed_statement
                .as_ref()
                .ok_or_else(PsqlError::no_statement_in_bind)?
        } else {
            self.named_statements
                .get(&statement_name)
                .ok_or_else(PsqlError::no_statement_in_bind)?
        };

        // 2. Instance the statement to get the portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = statement.instance(
            portal_name.clone(),
            &msg.params,
            msg.result_format_code,
            msg.param_format_code,
        )?;

        // 3. Insert the Portal.
        if portal_name.is_empty() {
            self.unnamed_portal.replace(portal);
        } else {
            self.named_portals.insert(portal_name, portal);
        }
        self.stream.write_no_flush(&BeMessage::BindComplete)?;
        Ok(())
    }

    async fn process_execute_msg(&mut self, msg: FeExecuteMessage) -> PsqlResult<()> {
        // 1. Get portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = if msg.portal_name.is_empty() {
            self.unnamed_portal
                .as_mut()
                .ok_or_else(PsqlError::no_portal_in_execute)?
        } else {
            // NOTE Error handle need modify later.
            self.named_portals
                .get_mut(&portal_name)
                .ok_or_else(PsqlError::no_portal_in_execute)?
        };

        tracing::trace!("(extended query)execute query: {}", portal.query_string());

        // 2. Execute instance statement using portal.
        let session = self.session.clone().unwrap();
        portal
            .execute::<SM, S>(session, msg.max_rows.try_into().unwrap(), &mut self.stream)
            .await?;

        // NOTE there is no ReadyForQuery message.
        Ok(())
    }

    fn process_describe_msg(&mut self, msg: FeDescribeMessage) -> PsqlResult<()> {
        //  b'S' => Statement
        //  b'P' => Portal
        tracing::trace!(
            "(extended query)describe name: {}",
            cstr_to_str(&msg.name).unwrap()
        );

        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            let name = cstr_to_str(&msg.name).unwrap().to_string();
            let statement = if name.is_empty() {
                self.unnamed_statement
                    .as_ref()
                    .ok_or_else(PsqlError::no_statement_in_describe)?
            } else {
                // NOTE Error handle need modify later.
                self.named_statements
                    .get(&name)
                    .ok_or_else(PsqlError::no_statement_in_describe)?
            };

            // 1. Send parameter description.
            self.stream
                .write_no_flush(&BeMessage::ParameterDescription(&statement.type_desc()))?;

            // 2. Send row description.
            if statement.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&statement.row_desc()))?;
            } else {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            }
        } else if msg.kind == b'P' {
            let name = cstr_to_str(&msg.name).unwrap().to_string();
            let portal = if name.is_empty() {
                self.unnamed_portal
                    .as_ref()
                    .ok_or_else(PsqlError::no_portal_in_describe)?
            } else {
                // NOTE Error handle need modify later.
                self.named_portals
                    .get(&name)
                    .ok_or_else(PsqlError::no_portal_in_describe)?
            };

            // 3. Send row description.
            if portal.is_query() {
                self.stream
                    .write_no_flush(&BeMessage::RowDescription(&portal.row_desc()))?;
            } else {
                // According https://www.postgresql.org/docs/current/protocol-flow.html#:~:text=The%20response%20is%20a%20RowDescri[…]0a%20query%20that%20will%20return%20rows%3B,
                // return NoData message if the statement is not a query.
                self.stream.write_no_flush(&BeMessage::NoData)?;
            }
        }
        Ok(())
    }

    fn process_close_msg(&mut self, msg: FeCloseMessage) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.name).unwrap().to_string();
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            self.named_statements.remove_entry(&name);
        } else if msg.kind == b'P' {
            self.named_portals.remove_entry(&name);
        }
        self.stream.write_no_flush(&BeMessage::CloseComplete)?;
        Ok(())
    }
}

/// Wraps a byte stream and read/write pg messages.
pub struct PgStream<S> {
    /// The underlying stream.
    stream: S,
    /// Write into buffer before flush to stream.
    write_buf: BytesMut,
}

impl<S> PgStream<S>
where
    S: AsyncWrite + AsyncRead + Unpin,
{
    async fn read_startup(&mut self) -> io::Result<FeMessage> {
        FeStartupMessage::read(&mut self.stream).await
    }

    async fn read(&mut self) -> io::Result<FeMessage> {
        FeMessage::read(&mut self.stream).await
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

    // The following functions are used to response something error to client.
    // The write() interface of this kind of message must be send successfully or "unwrap" when it
    // failed. Hence we can dirtyly unwrap write_message_no_flush, it must return Ok(),
    // otherwise system will panic and it never return.
    fn write_for_error(&mut self, message: &BeMessage<'_>) {
        self.write_no_flush(message).unwrap_or_else(|e| {
            tracing::error!("Error: {}", e);
        });
    }

    // The following functions are used to response something error to client.
    // If flush fail, it logs internally and don't report to user.
    // This approach is equal to the past.
    async fn flush_for_error(&mut self) {
        self.flush().await.unwrap_or_else(|e| {
            tracing::error!("flush error: {}", e);
        });
    }

    pub fn write_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        BeMessage::write(&mut self.write_buf, message)
    }

    #[expect(dead_code)]
    async fn write(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        self.write_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();
        self.stream.flush().await?;
        Ok(())
    }
}
