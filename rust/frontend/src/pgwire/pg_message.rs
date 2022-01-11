/// Part of code learned from https://github.com/zenithdb/zenith/blob/main/zenith_utils/src/pq_proto.rs.
use bytes::Bytes;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::pgwire::pg_field_descriptor::PgFieldDescriptor;
use crate::pgwire::pg_result::StatementType;

/// Messages that can be sent from pg client to server. Implement `read`.
pub enum FeMessage {
    Ssl,
    Startup(FeStartupMessage),
    Query(FeQueryMessage),
    Terminate,
}

pub struct FeStartupMessage {}

/// Query message contains the string sql.
pub struct FeQueryMessage {
    pub sql_bytes: Bytes,
}

impl FeQueryMessage {
    pub fn get_sql(&self) -> &str {
        // Why there is a \0..
        match std::str::from_utf8(&self.sql_bytes[..]) {
            Ok(v) => v.trim_end_matches('\0'),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        }
    }
}

impl FeMessage {
    /// Read one message from the stream.
    pub async fn read(stream: &mut TcpStream) -> Result<FeMessage> {
        let val = &[stream.read_u8().await?];
        let tag = std::str::from_utf8(val).unwrap();
        let len = stream.read_i32().await?;

        let payload_len = len - 4;
        let mut payload: Vec<u8> = vec![0; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        let sql_bytes = Bytes::from(payload);

        match tag {
            "Q" => Ok(FeMessage::Query(FeQueryMessage { sql_bytes })),
            "X" => Ok(FeMessage::Terminate),
            _ => {
                unimplemented!("Do not support other tags regular message yet")
            }
        }
    }
}

impl FeStartupMessage {
    /// Read startup message from the stream.
    pub async fn read(stream: &mut TcpStream) -> Result<FeMessage> {
        let len = stream.read_i32().await?;
        let protocol_num = stream.read_i32().await?;
        let payload_len = len - 8;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        match protocol_num {
            196608 => Ok(FeMessage::Startup(FeStartupMessage {})),
            80877103 => Ok(FeMessage::Ssl),
            _ => unimplemented!(
                "Unsupported protocol number in start up msg {:?}",
                protocol_num
            ),
        }
    }
}

/// Message sent from server to psql client. Implement `write`.
#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    CommandComplete(BeCommandCompleteMessage),
    // single byte - used in response to SSLRequest/GSSENCRequest
    EncryptionResponse,
    DataRow(&'a Row),
    ParameterStatus(BeParameterStatusMessage<'a>),
    ReadyForQuery,
    RowDescription(&'a [PgFieldDescriptor]),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    Encoding(&'a str),
    StandardConformingString(&'a str),
}

#[derive(Debug)]
pub struct BeCommandCompleteMessage {
    pub stmt_type: StatementType,
    pub rows_cnt: i32,
}

impl<'a> BeMessage<'a> {
    /// Write message to the given buf.
    pub async fn write(stream: &mut TcpStream, message: &BeMessage<'_>) -> Result<()> {
        match message {
            // AuthenticationOk
            // +-----+----------+-----------+
            // | 'R' | int32(8) | int32(0)  |
            // +-----+----------+-----------+
            BeMessage::AuthenticationOk => {
                stream.write_all(b"R").await?;
                stream.write_i32(8).await?;
                stream.write_i32(0).await?;
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
            BeMessage::ParameterStatus(param) => {
                use BeParameterStatusMessage::*;
                let [name, value] = match param {
                    Encoding(val) => [b"client_encoding", val.as_bytes()],
                    StandardConformingString(val) => {
                        [b"standard_conforming_strings", val.as_bytes()]
                    }
                };

                stream.write_all(b"S").await?;
                stream
                    .write_i32((4 + name.len() + 1 + value.len() + 1) as i32)
                    .await?;
                stream.write_all(name).await?;
                stream.write_all(b"\0").await?;
                stream.write_all(value).await?;
                stream.write_all(b"\0").await?;
            }

            // CommandComplete
            // +-----+-----------+-----------------+
            // | 'C' | int32 len | str commandTag  |
            // +-----+-----------+-----------------+
            BeMessage::CommandComplete(cmd) => {
                let rows_cnt = cmd.rows_cnt;
                let stmt_type = cmd.stmt_type;
                let mut tag = "".to_owned();
                tag.push_str(&stmt_type.to_string());
                if stmt_type == StatementType::INSERT {
                    tag.push_str(" 0");
                }
                if stmt_type.is_command() {
                    tag.push(' ');
                    tag.push_str(&rows_cnt.to_string());
                }
                stream.write_all(b"C").await?;
                stream.write_i32((4 + tag.len() + 1) as i32).await?;
                stream.write_all(tag.as_bytes()).await?;
                stream.write_all(b"\0").await?;
            }

            // DataRow
            // +-----+-----------+--------------+--------+-----+--------+
            // | 'D' | int32 len | int16 colNum | column | ... | column |
            // +-----+-----------+--------------+----+---+-----+--------+
            //                                       |
            //                          +-----------+v------+
            //                          | int32 len | bytes |
            //                          +-----------+-------+
            BeMessage::DataRow(row) => {
                stream.write_all(b"D").await?;
                let mut total_len = 4 + 2 + row.size() * 4;
                for val in &row.0 {
                    total_len += val.as_ref().map_or(0, |v| v.to_string().len());
                }

                stream.write_i32(total_len as i32).await?;
                stream.write_i16(row.size() as i16).await?;

                for val in &row.0 {
                    match val {
                        Some(inner_val) => {
                            let val_data = inner_val.to_string();
                            stream.write_i32(val_data.len() as i32).await?;
                            stream.write_all(val_data.as_bytes()).await?;
                        }
                        None => {
                            stream.write_i32(-1).await?;
                        }
                    }
                }
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
            BeMessage::RowDescription(row_descs) => {
                stream.write_all(b"T").await?;
                let mut len = 4 + 2 + row_descs.len() * (4 + 2 + 4 + 2 + 4 + 2);
                for pg_field in row_descs.iter() {
                    len += pg_field.get_name().len() + 1;
                }
                stream.write_i32(len as i32).await?;
                stream.write_i16(row_descs.len() as i16).await?;

                for pg_field in row_descs.iter() {
                    stream.write_all(pg_field.get_name().as_bytes()).await?;
                    stream.write_all(b"\0").await?;

                    stream.write_i32(pg_field.get_table_oid()).await?;
                    stream.write_i16(pg_field.get_col_attr_num()).await?;
                    stream
                        .write_i32(pg_field.get_type_oid().as_number())
                        .await?;
                    stream.write_i16(pg_field.get_type_len()).await?;
                    stream.write_i32(pg_field.get_type_modifier()).await?;
                    stream.write_i16(pg_field.get_format_code()).await?;
                }
            }
            // ReadyForQuery
            // +-----+----------+---------------------------+
            // | 'Z' | int32(5) | byte1(transaction status) |
            // +-----+----------+---------------------------+
            BeMessage::ReadyForQuery => {
                stream.write_all(b"Z").await?;
                stream.write_i32(5).await?;
                // TODO: add transaction status
                stream.write_all(b"I").await?;
            }

            BeMessage::EncryptionResponse => {
                stream.write_all(b"N").await?;
            }
        }

        Ok(())
    }
}
