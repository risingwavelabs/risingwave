use log::info;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pgwire::database::Database;
use crate::pgwire::pg_field_descriptor::PgFieldDescriptor;
use crate::pgwire::pg_message::{
    PgMessage, QueryMessage, SslMessage, StartupMessage, TerminateMessage,
};
use crate::pgwire::pg_result::{PgResult, StatementType};

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol {
    /// Used for write/read message.
    stream: TcpStream,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,
    database: Database,
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

impl PgProtocol {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            is_terminate: false,
            state: PgProtocolState::Startup,
            database: Database {},
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
                self.state = PgProtocolState::Regular;
            }
            PgMessage::Query(query_msg) => {
                self.process_query_msg(query_msg).await?;
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
            PgProtocolState::Startup => self.read_startup_msg().await,

            PgProtocolState::Regular => self.read_regular_message().await,
        }
    }

    async fn read_startup_msg(&mut self) -> Result<PgMessage> {
        let len = self.stream.read_i32().await?;
        let protocol_num = self.stream.read_i32().await?;
        let payload_len = len - 8;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }
        match protocol_num {
            196608 => Ok(PgMessage::Startup(StartupMessage::new())),
            80877103 => Ok(PgMessage::Ssl(SslMessage::new())),
            _ => unimplemented!("Unsupported protocol number in start up msg"),
        }
    }

    async fn read_regular_message(&mut self) -> Result<PgMessage> {
        let val = &[self.stream.read_u8().await?];
        let tag = std::str::from_utf8(val).unwrap();
        let len = self.stream.read_i32().await?;

        let payload_len = len - 4;
        let mut payload: Vec<u8> = vec![0; payload_len as usize];
        if payload_len > 0 {
            self.stream.read_exact(&mut payload).await?;
        }

        match tag {
            "Q" => Ok(PgMessage::Query(QueryMessage::new(payload))),

            "X" => Ok(PgMessage::Terminate(TerminateMessage::new())),

            _ => {
                unimplemented!("Do not support other tags regular message yet")
            }
        }
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

    async fn process_query_msg(&mut self, query: QueryMessage) -> Result<()> {
        info!("receive query: {}", query.get_sql());
        // execute query
        let res = self.database.run_statement(query.get_sql());
        if res.is_query() {
            self.process_query_with_results(res).await?;
        } else {
            self.write_command_complete(res.get_stmt_type(), res.get_effected_rows_cnt())
                .await?;
        }
        self.write_ready_for_query().await?;
        Ok(())
    }

    async fn process_query_with_results(&mut self, res: PgResult) -> Result<()> {
        self.write_row_desc(res.get_row_desc()).await?;
        self.stream.flush().await?;

        let mut rows_cnt = 0;
        let iter = res.iter();
        for val in iter {
            self.write_data_row(val).await?;
            self.stream.flush().await?;
            rows_cnt += 1;
        }
        self.write_command_complete(res.get_stmt_type(), rows_cnt)
            .await?;
        Ok(())
    }

    // DataRow
    // +-----+-----------+--------------+--------+-----+--------+
    // | 'D' | int32 len | int16 colNum | column | ... | column |
    // +-----+-----------+--------------+----+---+-----+--------+
    //                                       |
    //                          +-----------+v------+
    //                          | int32 len | bytes |
    //                          +-----------+-------+
    async fn write_data_row(&mut self, row: &Row) -> Result<()> {
        self.stream.write_all(b"D").await?;
        let mut total_len = 4 + 2 + row.size() * 4;
        for val in &row.0 {
            total_len += val.as_ref().map_or(0, |v| v.to_string().len());
        }

        self.stream.write_i32(total_len as i32).await?;
        self.stream.write_i16(row.size() as i16).await?;

        for val in &row.0 {
            match val {
                Some(inner_val) => {
                    let val_data = inner_val.to_string();
                    self.stream.write_i32(val_data.len() as i32).await?;
                    self.stream.write_all(val_data.as_bytes()).await?;
                }
                None => {
                    self.stream.write_i32(-1).await?;
                }
            }
        }

        Ok(())
    }

    // CommandComplete
    // +-----+-----------+-----------------+
    // | 'C' | int32 len | str commandTag  |
    // +-----+-----------+-----------------+
    async fn write_command_complete(
        &mut self,
        stmt_type: StatementType,
        rows_cnt: i32,
    ) -> Result<()> {
        let mut tag = "".to_owned();
        tag.push_str(&stmt_type.to_string());
        if stmt_type == StatementType::INSERT {
            tag.push_str(" 0");
        }
        if stmt_type.is_command() {
            tag.push(' ');
            tag.push_str(&rows_cnt.to_string());
        }
        self.stream.write_all(b"C").await?;
        self.stream.write_i32((4 + tag.len() + 1) as i32).await?;
        self.stream.write_all(tag.as_bytes()).await?;
        self.stream.write_all(b"\0").await?;
        Ok(())
    }

    // RowDescription
    // +-----+-----------+--------------+-------+-----+-------+
    // | 'T' | int32 len | int16 colNum | field | ... | field |
    // +-----+-----------+--------------+----+--+-----+-------+
    //                                       |
    // +---------------+-------+-------+-----v-+-------+-------+-------+
    // | str fieldName | int32 | int16 | int32 | int16 | int32 | int16 |
    // +---------------+---+---+---+---+---+---+----+--+---+---+---+---+
    //                     |       |       |        |      |       |
    //                     v       |       v        v      |       v
    //                tableOID     |    typeOID  typeLen   |   formatCode
    //                             v                       v
    //                        colAttrNum               typeModifier
    async fn write_row_desc(&mut self, row_descs: Vec<PgFieldDescriptor>) -> Result<()> {
        self.stream.write_all(b"T").await?;
        let mut len = 4 + 2 + row_descs.len() * (4 + 2 + 4 + 2 + 4 + 2);
        for pg_field in &row_descs {
            len += pg_field.get_name().len() + 1;
        }
        self.stream.write_i32(len as i32).await?;
        self.stream.write_i16(row_descs.len() as i16).await?;

        for pg_field in &row_descs {
            self.stream
                .write_all(pg_field.get_name().as_bytes())
                .await?;
            self.stream.write_all(b"\0").await?;

            self.stream.write_i32(pg_field.get_table_oid()).await?;
            self.stream.write_i16(pg_field.get_col_attr_num()).await?;
            self.stream
                .write_i32(pg_field.get_type_oid().as_number())
                .await?;
            self.stream.write_i16(pg_field.get_type_len()).await?;
            self.stream.write_i32(pg_field.get_type_modifier()).await?;
            self.stream.write_i16(pg_field.get_format_code()).await?;
        }

        Ok(())
    }

    async fn process_ssl_msg(&mut self, _ssl_msg: SslMessage) -> Result<()> {
        self.stream.write_all(b"N").await?;
        Ok(())
    }

    // AuthenticationOk
    // +-----+----------+-----------+
    // | 'R' | int32(8) | int32(0)  |
    // +-----+----------+-----------+
    async fn write_auth_ok(&mut self) -> Result<()> {
        self.stream.write_all(b"R").await?;
        self.stream.write_i32(8).await?;
        self.stream.write_i32(0).await?;
        Ok(())
    }

    // ParameterStatus
    // +-----+-----------+----------+------+-----------+------+
    // | 'S' | int32 len | str name | '\0' | str value | '\0' |
    // +-----+-----------+----------+------+-----------+------+
    //
    // At present there is a hard-wired set of parameters for which
    // ParameterStatus will be generated: they are:
    //  server_version,
    //  server_encoding,
    //  client_encoding,
    //  application_name,
    //  is_superuser,
    //  session_authorization,
    //  DateStyle,
    //  IntervalStyle,
    //  TimeZone,
    //  integer_datetimes,
    //  standard_conforming_string
    //
    // See: https://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC.
    //
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

    // ReadyForQuery
    // +-----+----------+---------------------------+
    // | 'Z' | int32(5) | byte1(transaction status) |
    // +-----+----------+---------------------------+
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
