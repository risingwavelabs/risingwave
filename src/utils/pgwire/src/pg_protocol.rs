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

use std::io::{Error as IoError, ErrorKind, Result};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::error::PsqlError;
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeMessage, FeStartupMessage,
};
use crate::pg_response::PgResponse;
use crate::pg_server::{Session, SessionManager};

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
fn cstr_to_str(b: &Bytes) -> Result<&str> {
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

    pub async fn process(&mut self, unnamed_query_string: &mut Bytes) -> Result<bool> {
        if self.do_process(unnamed_query_string).await? {
            return Ok(true);
        }

        Ok(self.is_terminate())
    }

    async fn do_process(&mut self, unnamed_query_string: &mut Bytes) -> Result<bool> {
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
            FeMessage::Query(query_msg) => {
                self.process_query_msg(query_msg.get_sql(), false).await?;
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
                *unnamed_query_string = m.query_string;
                self.write_message(&BeMessage::ParseComplete).await?;
            }
            FeMessage::Bind(_) => {
                self.write_message(&BeMessage::BindComplete).await?;
            }
            FeMessage::Execute(_) => {
                self.process_query_msg(cstr_to_str(unnamed_query_string), true)
                    .await?;
                // NOTE there is no ReadyForQuery message.
            }
            FeMessage::Describe(_) => {
                self.write_message_no_flush(&BeMessage::ParameterDescription)?;
                // FIXME: Introduce parser to analyze statements and bind data type. Here just
                // hard-code a VARCHAR.
                self.write_message(&BeMessage::RowDescription(&[PgFieldDescriptor::new(
                    "VARCHAR".to_string(),
                    TypeOid::Varchar,
                )]))
                .await?;
            }
            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery).await?;
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
        let db_name = {
            match msg.config.get("database") {
                None => "dev".to_string(),
                Some(v) => v.to_string(),
            }
        };
        self.session = Some(self.session_mgr.connect(&db_name).map_err(IoError::other)?);
        self.write_message_no_flush(&BeMessage::AuthenticationOk)?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding("utf8"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion("9.5.0"),
        ))?;
        self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_query_msg(
        &mut self,
        query_string: Result<&str>,
        extended: bool,
    ) -> Result<()> {
        match query_string {
            Ok(sql) => {
                tracing::trace!("receive query: {}", sql);
                let session = self.session.clone().unwrap();
                // execute query
                let process_res = session.run_statement(sql).await;
                match process_res {
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
            }
            Err(err) => {
                self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(err)))?;
            }
        };

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
        self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
            stmt_type: res.get_stmt_type(),
            notice: res.get_notice(),
            rows_cnt,
        }))?;
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
