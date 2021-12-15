use risingwave_common::error::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pg_message::{PgMessage, SslMessage, StartupMessage, TerminateMessage};

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol {
    /// Used for write/read message.
    stream: TcpStream,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,
}

/// States flow happened from top to down.
enum PgProtocolState {
    SslHandshake,
    Startup,
    Regular,
}

impl PgProtocol {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            is_terminate: false,
            state: PgProtocolState::SslHandshake,
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
            PgMessage::Ssl(ssl_msg) => {
                self.process_ssl_msg(ssl_msg).await?;
            }
            PgMessage::Startup(startup_msg) => {
                self.process_startup_msg(startup_msg).await?;
            }

            PgMessage::Terminate(_temrminate_msg) => {
                self.process_terminate();
            }
        }
        self.stream.flush().await?;
        Ok(false)
    }

    async fn read_message(&mut self) -> Result<PgMessage> {
        match self.state {
            PgProtocolState::SslHandshake => {
                self.state = PgProtocolState::Startup;
                self.read_ssl_request().await
            }

            PgProtocolState::Startup => {
                self.state = PgProtocolState::Regular;
                self.read_startup_msg().await
            }

            PgProtocolState::Regular => self.read_regular_message().await,
        }
    }

    async fn read_startup_msg(&mut self) -> Result<PgMessage> {
        let len = self.stream.read_i32().await?;
        let protocol_num = self.stream.read_i32().await?;
        assert_eq!(protocol_num, 196608);
        let payload_len = len - 8;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }

        Ok(PgMessage::Startup(StartupMessage::new()))
    }

    async fn read_regular_message(&mut self) -> Result<PgMessage> {
        let val = &[self.stream.read_u8().await?];
        let tag = std::str::from_utf8(val).unwrap();
        let len = self.stream.read_i32().await?;

        let payload_len = len - 4;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }

        match tag {
            "Q" => {
                unimplemented!("Do not support query as message yet")
            }

            "X" => Ok(PgMessage::Terminate(TerminateMessage::new())),

            _ => {
                unimplemented!("Do not support other tags regular message yet")
            }
        }
    }

    async fn read_ssl_request(&mut self) -> Result<PgMessage> {
        // Do not need the length here.
        let _len_ = self.stream.read_i32().await?;
        let protocol_num = self.stream.read_i32().await?;
        assert_eq!(protocol_num, 80877103);
        Ok(PgMessage::Ssl(SslMessage::new()))
    }

    async fn process_startup_msg(&mut self, _msg: StartupMessage) -> Result<()> {
        self.write_auth_ok().await?;
        self.write_parameter_status("client_encoding", "utf8")
            .await?;
        self.write_parameter_status("standard_conforming_strings", "on")
            .await?;
        self.write_ready_for_query().await?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_ssl_msg(&mut self, _ssl_msg: SslMessage) -> Result<()> {
        self.stream.write_all(b"N").await?;
        Ok(())
    }

    async fn write_auth_ok(&mut self) -> Result<()> {
        self.stream.write_all(b"R").await?;
        self.stream.write_i32(8).await?;
        self.stream.write_i32(0).await?;
        Ok(())
    }

    async fn write_parameter_status(&mut self, name: &str, value: &str) -> Result<()> {
        self.stream.write_all(b"S").await?;
        self.stream
            .write_i32((4 + name.len() + 1 + value.len() + 1) as i32)
            .await?;
        self.stream.write_all(name.as_bytes()).await?;
        self.stream.write_all(b"\0").await?;
        self.stream.write_all(value.as_bytes()).await?;
        self.stream.write_all(b"\0").await?;
        Ok(())
    }

    async fn write_ready_for_query(&mut self) -> Result<()> {
        self.stream.write_all(b"Z").await?;
        self.stream.write_i32(5).await?;
        // TODO: add transaction status
        self.stream.write_all(b"I").await?;
        Ok(())
    }

    fn is_terminate(&self) -> bool {
        self.is_terminate
    }
}
