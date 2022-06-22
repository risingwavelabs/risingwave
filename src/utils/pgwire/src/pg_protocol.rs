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
use std::io::{Error as IoError, ErrorKind, Result};
use std::sync::Arc;
use std::{result, str, vec};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::error::PsqlError;
use crate::pg_extended::{PgPortal, PgStatement};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeMessage, FePasswordMessage,
    FeStartupMessage,
};
use crate::pg_response::PgResponse;
use crate::pg_server::{BoxedError, Session, SessionManager, UserAuthenticator};

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM>
where
    SM: SessionManager,
{
    /// Used for write/read message in tcp connection.
    stream: S,
    /// Write into buffer before flush to stream.
    buf_out: BytesMut,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations)
// PG protocol strings are always C strings.
pub fn cstr_to_str(b: &Bytes) -> Result<&str> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null).map_err(|e| std::io::Error::new(ErrorKind::Other, e))
}

impl<S, SM> PgProtocol<S, SM>
where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager,
{
    pub fn new(stream: S, session_mgr: Arc<SM>) -> Self {
        Self {
            stream,
            is_terminate: false,
            state: PgProtocolState::Startup,
            buf_out: BytesMut::with_capacity(10 * 1024),
            session_mgr,
            session: None,
        }
    }

    pub async fn process(
        &mut self,
        unnamed_statement: &mut PgStatement,
        unnamed_portal: &mut PgPortal,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> Result<bool> {
        if self
            .do_process(
                unnamed_statement,
                unnamed_portal,
                named_statements,
                named_portals,
            )
            .await?
        {
            return Ok(true);
        }

        Ok(self.is_terminate())
    }

    async fn do_process(
        &mut self,
        unnamed_statement: &mut PgStatement,
        unnamed_portal: &mut PgPortal,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> Result<bool> {
        let msg = match self.read_message().await {
            Ok(msg) => msg,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(e);
                }
                tracing::error!("unable to read message: {}", e);
                self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(e)))?;
                self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
                return Ok(false);
            }
        };
        match msg {
            FeMessage::Ssl => {
                self.write_message_no_flush(&BeMessage::EncryptionResponse)
                    .map_err(|e| {
                        tracing::error!("failed to handle ssl request: {}", e);
                        e
                    })?;
            }
            FeMessage::Startup(msg) => {
                if let Err(e) = self.process_startup_msg(msg) {
                    tracing::error!("failed to set up pg session: {}", e);
                    self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(e)))?;
                    self.flush().await?;
                    return Ok(true);
                }
                self.state = PgProtocolState::Regular;
            }
            FeMessage::Password(msg) => {
                if let Err(e) = self.process_password_msg(msg) {
                    tracing::error!("failed to authenticate session: {}", e);
                    self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(e)))?;
                    self.flush().await?;
                    return Ok(true);
                }
                self.state = PgProtocolState::Regular;
            }
            FeMessage::Query(query_msg) => {
                self.process_query_msg_simple(query_msg.get_sql()).await?;
                self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
            }
            FeMessage::CancelQuery => {
                self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(
                    PsqlError::cancel(),
                )))?;
            }
            FeMessage::Terminate => {
                self.process_terminate();
            }
            FeMessage::Parse(m) => {
                let query = cstr_to_str(&m.query_string).unwrap();

                // 1. Create the types description.
                let type_ids = m.type_ids;
                let types: Vec<TypeOid> = type_ids
                    .into_iter()
                    .map(|x| TypeOid::as_type(x).unwrap())
                    .collect();

                // 2. Create the row description.

                let rows: Vec<PgFieldDescriptor> = if query.starts_with("SELECT")
                    || query.starts_with("select")
                {
                    if types.is_empty() {
                        let session = self.session.clone().unwrap();
                        let rows_res = session.infer_return_type(query).await;
                        match rows_res {
                            Ok(r) => r,
                            Err(e) => {
                                self.write_message_no_flush(&BeMessage::ErrorResponse(e))?;
                                // TODO: Error handle needed modified later.
                                unimplemented!();
                            }
                        }
                    } else {
                        query
                            .split(&[' ', ',', ';'])
                            .skip(1)
                            .into_iter()
                            .take_while(|x| !x.is_empty())
                            .map(|x| {
                                // NOTE: Assume all output are generic params.
                                let str = x.strip_prefix('$').unwrap();
                                // NOTE: Assume all generic are valid.
                                let v: i32 = str.parse().unwrap();
                                assert!(v.is_positive());
                                v
                            })
                            .map(|x| {
                                // NOTE Make sure the type_description include all generic parametre
                                // description we needed.
                                assert!(((x - 1) as usize) < types.len());
                                PgFieldDescriptor::new(
                                    String::new(),
                                    types[(x - 1) as usize].to_owned(),
                                )
                            })
                            .collect()
                    }
                } else {
                    vec![]
                };

                // 3. Create the statement.
                let statement = PgStatement::new(
                    cstr_to_str(&m.statement_name).unwrap().to_string(),
                    m.query_string,
                    types,
                    rows,
                );

                // 4. Insert the statement.
                let name = statement.name();
                if name.is_empty() {
                    *unnamed_statement = statement;
                } else {
                    named_statements.insert(name, statement);
                }
                self.write_message(&BeMessage::ParseComplete).await?;
            }
            FeMessage::Bind(m) => {
                let statement_name = cstr_to_str(&m.statement_name).unwrap().to_string();
                // 1. Get statement.
                let statement = if statement_name.is_empty() {
                    unnamed_statement
                } else {
                    // NOTE Error handle method may need to modified.
                    named_statements.get(&statement_name).expect("statement_name managed by client_driver, hence assume statement name always valid.")
                };

                // 2. Instance the statement to get the portal.
                let portal_name = cstr_to_str(&m.portal_name).unwrap().to_string();
                let session = self.session.clone().unwrap();
                let portal = statement
                    .instance::<SM>(session, portal_name.clone(), &m.params)
                    .await
                    .unwrap();

                // 3. Insert the Portal.
                if portal_name.is_empty() {
                    *unnamed_portal = portal;
                } else {
                    named_portals.insert(portal_name, portal);
                }
                self.write_message(&BeMessage::BindComplete).await?;
            }
            FeMessage::Execute(m) => {
                // 1. Get portal.
                let portal_name = cstr_to_str(&m.portal_name).unwrap().to_string();
                let portal = if m.portal_name.is_empty() {
                    unnamed_portal
                } else {
                    // NOTE Error handle need modify later.
                    named_portals.get_mut(&portal_name).expect("statement_name managed by client_driver, hence assume statement name always valid")
                };

                // 2. Execute instance statement using portal.
                self.process_query_msg_extended(portal, m.max_rows.try_into().unwrap())
                    .await?;

                // NOTE there is no ReadyForQuery message.
            }
            FeMessage::Describe(m) => {
                // 1. Get statement.
                if m.kind == b'S' {
                    let name = cstr_to_str(&m.query_name).unwrap().to_string();
                    let statement = if name.is_empty() {
                        unnamed_statement
                    } else {
                        // NOTE Error handle need modify later.
                        named_statements.get(&name).unwrap()
                    };

                    // 2. Send parameter description.
                    self.write_message(&BeMessage::ParameterDescription(&statement.type_desc()))
                        .await?;

                    // 3. Send row description.
                    self.write_message(&BeMessage::RowDescription(&statement.row_desc()))
                        .await?;
                } else if m.kind == b'P' {
                    let name = cstr_to_str(&m.query_name).unwrap().to_string();
                    let portal = if name.is_empty() {
                        unnamed_portal
                    } else {
                        // NOTE Error handle need modify later.
                        named_portals.get(&name).unwrap()
                    };

                    self.write_message(&BeMessage::RowDescription(&portal.row_desc()))
                        .await?;
                } else {
                    // TODO: Error Handle
                    todo!()
                }
            }
            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery).await?;
            }
            FeMessage::Close(m) => {
                let name = cstr_to_str(&m.query_name).unwrap().to_string();
                if m.kind == b'S' {
                    named_statements.remove_entry(&name);
                } else if m.kind == b'P' {
                    named_portals.remove_entry(&name);
                } else {
                    // NOTE Error handle need modify later.
                }
                self.write_message(&BeMessage::CloseComplete).await?;
            }
        }
        self.flush().await?;
        Ok(false)
    }

    async fn read_message(&mut self) -> Result<FeMessage> {
        match self.state {
            PgProtocolState::Startup => FeStartupMessage::read(&mut self.stream).await,
            PgProtocolState::Regular => FeMessage::read(&mut self.stream).await,
        }
    }

    fn process_startup_msg(&mut self, msg: FeStartupMessage) -> Result<()> {
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
            .map_err(IoError::other)?;
        match session.user_authenticator() {
            UserAuthenticator::None => {
                self.write_message_no_flush(&BeMessage::AuthenticationOk)?;
                self.write_parameter_status_msg_no_flush()?;
                self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
            }
            UserAuthenticator::ClearText(_) => {
                self.write_message_no_flush(&BeMessage::AuthenticationCleartextPassword)?;
            }
            UserAuthenticator::MD5WithSalt { salt, .. } => {
                self.write_message_no_flush(&BeMessage::AuthenticationMD5Password(salt))?;
            }
        }
        self.session = Some(session);
        Ok(())
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> Result<()> {
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding("utf8"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion("9.5.0"),
        ))?;
        Ok(())
    }

    fn process_password_msg(&mut self, msg: FePasswordMessage) -> Result<()> {
        let authenticator = self.session.as_ref().unwrap().user_authenticator();
        if !authenticator.authenticate(&msg.password) {
            return Err(IoError::new(ErrorKind::InvalidInput, "Invalid password"));
        }
        self.write_message_no_flush(&BeMessage::AuthenticationOk)?;
        self.write_parameter_status_msg_no_flush()?;
        self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_query_msg_extended(
        &mut self,
        portal: &mut PgPortal,
        row_limit: usize,
    ) -> Result<()> {
        let session = self.session.clone().unwrap();
        // execute query
        let process_res = portal.execute::<SM>(session, row_limit).await;
        self.process_query_response(process_res, true).await?;
        Ok(())
    }

    async fn process_query_msg_simple(&mut self, query_string: Result<&str>) -> Result<()> {
        match query_string {
            Ok(sql) => {
                tracing::trace!("receive query: {}", sql);
                let session = self.session.clone().unwrap();
                // execute query
                let process_res = session.run_statement(sql).await;
                self.process_query_response(process_res, false).await?;
            }
            Err(err) => {
                self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(err)))?;
            }
        };

        Ok(())
    }

    async fn process_query_response(
        &mut self,
        response: result::Result<PgResponse, BoxedError>,
        extended: bool,
    ) -> Result<()> {
        match response {
            Ok(res) => {
                if res.is_empty() {
                    self.write_message_no_flush(&BeMessage::EmptyQueryResponse)?;
                } else if res.is_query() {
                    self.process_query_with_results(res, extended).await?;
                } else {
                    self.write_message_no_flush(&BeMessage::CommandComplete(
                        BeCommandCompleteMessage {
                            stmt_type: res.get_stmt_type(),
                            notice: res.get_notice(),
                            rows_cnt: res.get_effected_rows_cnt(),
                        },
                    ))?;
                }
            }
            Err(e) => {
                self.write_message_no_flush(&BeMessage::ErrorResponse(e))?;
            }
        }
        Ok(())
    }

    async fn process_query_with_results(&mut self, res: PgResponse, extended: bool) -> Result<()> {
        // The possible responses to Execute are the same as those described above for queries
        // issued via simple query protocol, except that Execute doesn't cause ReadyForQuery or
        // RowDescription to be issued.
        // Quoted from: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
        if !extended {
            self.write_message(&BeMessage::RowDescription(&res.get_row_desc()))
                .await?;
        }

        let mut rows_cnt = 0;
        let iter = res.iter();
        for val in iter {
            self.write_message(&BeMessage::DataRow(val)).await?;
            rows_cnt += 1;
        }

        // If has rows limit, it must be extended mode.
        // If Execute terminates before completing the execution of a portal (due to reaching a
        // nonzero result-row count), it will send a PortalSuspended message; the appearance of this
        // message tells the frontend that another Execute should be issued against the same portal
        // to complete the operation. The CommandComplete message indicating completion of the
        // source SQL command is not sent until the portal's execution is completed.
        // Quote from: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY:~:text=Once%20a%20portal,ErrorResponse%2C%20or%20PortalSuspended
        if !extended || res.is_row_end() {
            self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: res.get_stmt_type(),
                notice: res.get_notice(),
                rows_cnt,
            }))?;
        } else {
            self.write_message(&BeMessage::PortalSuspended).await?;
        }
        Ok(())
    }

    fn is_terminate(&self) -> bool {
        self.is_terminate
    }

    fn write_message_no_flush(&mut self, message: &BeMessage<'_>) -> Result<()> {
        BeMessage::write(&mut self.buf_out, message)
    }

    async fn write_message(&mut self, message: &BeMessage<'_>) -> Result<()> {
        self.write_message_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.stream.write_all(&self.buf_out).await?;
        self.buf_out.clear();
        self.stream.flush().await?;
        Ok(())
    }
}
