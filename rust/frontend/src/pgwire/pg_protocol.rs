use std::sync::Arc;

use bytes::BytesMut;
use log::info;
use risingwave_common::error::Result;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use super::pg_server::{Session, SessionManager};
use crate::pgwire::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeMessage, FeQueryMessage,
    FeStartupMessage,
};
use crate::pgwire::pg_result::PgResult;

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol {
    /// Used for write/read message in tcp connection.
    stream: TcpStream,
    /// Write into buffer before flush to stream.
    buf_out: BytesMut,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<dyn SessionManager>,
    session: Option<Box<dyn Session>>,
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

impl PgProtocol {
    pub fn new(stream: TcpStream, session_mgr: Arc<dyn SessionManager>) -> Self {
        Self {
            stream,
            is_terminate: false,
            state: PgProtocolState::Startup,
            buf_out: BytesMut::with_capacity(10 * 1024),
            session_mgr,
            session: None,
        }
    }

    pub async fn process(&mut self) -> Result<bool> {
        if self.do_process().await? {
            return Ok(true);
        }

        Ok(self.is_terminate())
    }

    async fn do_process(&mut self) -> Result<bool> {
        let msg = self.read_message().await.unwrap();
        match msg {
            FeMessage::Ssl => {
                self.write_message_no_flush(&BeMessage::EncryptionResponse)?;
            }
            FeMessage::Startup(msg) => {
                self.process_startup_msg(msg)?;
                self.state = PgProtocolState::Regular;
            }
            FeMessage::Query(query_msg) => {
                self.process_query_msg(query_msg).await?;
            }
            FeMessage::Terminate => {
                self.process_terminate();
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

    fn process_startup_msg(&mut self, _msg: FeStartupMessage) -> Result<()> {
        self.session = Some(self.session_mgr.connect());
        self.session = Some(self.session_mgr.connect());
        self.write_message_no_flush(&BeMessage::AuthenticationOk)?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::Encoding("utf8"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_query_msg(&mut self, query: FeQueryMessage) -> Result<()> {
        info!("receive query: {}", query.get_sql());
        let session = self.session.as_ref().unwrap();

        // execute query
        let res = session.run_statement(query.get_sql()).await;
        if res.is_query() {
            self.process_query_with_results(res).await?;
        } else {
            self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: res.get_stmt_type(),
                rows_cnt: res.get_effected_rows_cnt(),
            }))?;
        }
        self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    async fn process_query_with_results(&mut self, res: PgResult) -> Result<()> {
        self.write_message(&BeMessage::RowDescription(&res.get_row_desc()))
            .await?;

        let mut rows_cnt = 0;
        let iter = res.iter();
        for val in iter {
            self.write_message(&BeMessage::DataRow(val)).await?;
            rows_cnt += 1;
        }
        self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
            stmt_type: res.get_stmt_type(),
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
