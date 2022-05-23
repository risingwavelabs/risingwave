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

use std::ffi::CStr;
use std::io::{Error, ErrorKind, IoSlice, Result, Write};

use byteorder::{BigEndian, ByteOrder};
/// Part of code learned from https://github.com/zenithdb/zenith/blob/main/zenith_utils/src/pq_proto.rs.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_message::FeMessage::Bind;
use crate::pg_response::StatementType;
use crate::pg_server::BoxedError;
use crate::types::Row;

/// Messages that can be sent from pg client to server. Implement `read`.
pub enum FeMessage {
    Ssl,
    Startup(FeStartupMessage),
    Query(FeQueryMessage),
    Parse(FeParseMessage),
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Sync,
    CancelQuery,
    Terminate,
}

pub struct FeStartupMessage {}

/// Query message contains the string sql.
pub struct FeQueryMessage {
    pub sql_bytes: Bytes,
}

#[derive(Debug)]
pub struct FeBindMessage {}

#[derive(Debug)]
pub struct FeExecuteMessage {
    pub max_rows: i32,
}

#[derive(Debug)]
pub struct FeParseMessage {
    pub query_string: Bytes,
}

#[derive(Debug)]
pub struct FeDescribeMessage {
    // 'S' to describe a prepared statement; or 'P' to describe a portal.
    pub kind: u8,
}

impl FeDescribeMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let kind = buf.get_u8();
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if kind != b'S' {
            unimplemented!("only prepared statement Describe is implemented");
        }

        Ok(FeMessage::Describe(FeDescribeMessage { kind }))
    }
}

impl FeBindMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let _pstmt_name = read_null_terminated(&mut buf)?;

        if !portal_name.is_empty() {
            unimplemented!("named portals not implemented");
        }

        Ok(FeMessage::Bind(FeBindMessage {}))
    }
}

impl FeExecuteMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let max_rows = buf.get_i32();

        if !portal_name.is_empty() {
            unimplemented!("named portals not implemented");
        }

        if max_rows != 0 {
            unimplemented!("row limit in Execute message not supported");
        }

        Ok(FeMessage::Execute(FeExecuteMessage { max_rows }))
    }
}

impl FeParseMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let _pstmt_name = read_null_terminated(&mut buf)?;
        let query_string = read_null_terminated(&mut buf)?;
        let nparams = buf.get_i16();

        if nparams != 0 {
            unimplemented!("query params not implemented");
        }

        Ok(FeMessage::Parse(FeParseMessage { query_string }))
    }
}

impl FeQueryMessage {
    pub fn get_sql(&self) -> Result<&str> {
        match CStr::from_bytes_with_nul(&self.sql_bytes) {
            Ok(cstr) => cstr.to_str().map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("Invalid UTF-8 sequence: {}", err),
                )
            }),
            Err(err) => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Input end error: {}", err),
            )),
        }
    }
}

impl FeMessage {
    /// Read one message from the stream.
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<FeMessage> {
        let val = stream.read_u8().await?;
        let len = stream.read_i32().await?;

        let payload_len = len - 4;
        let mut payload: Vec<u8> = vec![0; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        let sql_bytes = Bytes::from(payload);

        match val {
            b'Q' => Ok(FeMessage::Query(FeQueryMessage { sql_bytes })),
            b'P' => FeParseMessage::parse(sql_bytes),
            b'D' => FeDescribeMessage::parse(sql_bytes),
            b'B' => FeBindMessage::parse(sql_bytes),
            b'E' => FeExecuteMessage::parse(sql_bytes),
            b'S' => Ok(FeMessage::Sync),
            b'X' => Ok(FeMessage::Terminate),
            _ => Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("Unsupported tag of regular message: {}", val),
            )),
        }
    }
}

impl FeStartupMessage {
    /// Read startup message from the stream.
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<FeMessage> {
        let len = stream.read_i32().await?;
        let protocol_num = stream.read_i32().await?;
        let payload_len = len - 8;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        match protocol_num {
            // code from: https://www.postgresql.org/docs/current/protocol-message-formats.html
            196608 => Ok(FeMessage::Startup(FeStartupMessage {})),
            80877103 => Ok(FeMessage::Ssl),
            // Cancel request code.
            80877102 => Ok(FeMessage::CancelQuery),
            _ => Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Unsupported protocol number in start up msg {:?}",
                    protocol_num
                ),
            )),
        }
    }
}

/// Continue read until reached a \0. Used in reading string from Bytes.
fn read_null_terminated(buf: &mut Bytes) -> Result<Bytes> {
    let mut result = BytesMut::new();

    loop {
        if !buf.has_remaining() {
            panic!("no null-terminator in string");
        }

        let byte = buf.get_u8();

        if byte == 0 {
            break;
        }
        result.put_u8(byte);
    }
    Ok(result.freeze())
}

/// Message sent from server to psql client. Implement `write` (how to serialize it into psql
/// buffer).
#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    CommandComplete(BeCommandCompleteMessage),
    // Single byte - used in response to SSLRequest/GSSENCRequest.
    EncryptionResponse,
    EmptyQueryResponse,
    ParseComplete,
    BindComplete,
    ParameterDescription,
    NoData,
    DataRow(&'a Row),
    ParameterStatus(BeParameterStatusMessage<'a>),
    ReadyForQuery,
    RowDescription(&'a [PgFieldDescriptor]),
    ErrorResponse(BoxedError),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    ClientEncoding(&'a str),
    StandardConformingString(&'a str),
    ServerVersion(&'a str),
}

#[derive(Debug)]
pub struct BeCommandCompleteMessage {
    pub stmt_type: StatementType,
    pub notice: Option<String>,
    pub rows_cnt: i32,
}

impl<'a> BeMessage<'a> {
    /// Write message to the given buf.
    pub fn write(buf: &mut BytesMut, message: &BeMessage<'_>) -> Result<()> {
        match message {
            // AuthenticationOk
            // +-----+----------+-----------+
            // | 'R' | int32(8) | int32(0)  |
            // +-----+----------+-----------+
            BeMessage::AuthenticationOk => {
                buf.put_u8(b'R');
                buf.put_i32(8);
                buf.put_i32(0);
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
                    ClientEncoding(val) => [b"client_encoding", val.as_bytes()],
                    StandardConformingString(val) => {
                        [b"standard_conforming_strings", val.as_bytes()]
                    }
                    ServerVersion(val) => [b"server_version", val.as_bytes()],
                };

                // Parameter names and values are passed as null-terminated strings
                let iov = &mut [name, b"\0", value, b"\0"].map(IoSlice::new);
                let mut buffer = [0u8; 64]; // this should be enough
                let cnt = buffer.as_mut().write_vectored(iov).unwrap();

                buf.put_u8(b'S');
                write_body(buf, |stream| {
                    stream.put_slice(&buffer[..cnt]);
                    Ok(())
                })
                .unwrap();
            }

            // CommandComplete
            // +-----+-----------+-----------------+
            // | 'C' | int32 len | str commandTag  |
            // +-----+-----------+-----------------+
            BeMessage::CommandComplete(cmd) => {
                let rows_cnt = cmd.rows_cnt;
                let stmt_type = cmd.stmt_type;
                let mut tag = "".to_owned();
                if let Some(notice) = &cmd.notice {
                    tag.push_str(notice);
                    tag.push('\n');
                }
                tag.push_str(&stmt_type.to_string());
                if stmt_type == StatementType::INSERT {
                    tag.push_str(" 0");
                }
                if stmt_type.is_command() {
                    tag.push(' ');
                    tag.push_str(&rows_cnt.to_string());
                }
                buf.put_u8(b'C');
                write_body(buf, |buf| {
                    write_cstr(buf, tag.as_bytes())?;
                    Ok(())
                })?;
            }

            // DataRow
            // +-----+-----------+--------------+--------+-----+--------+
            // | 'D' | int32 len | int16 colNum | column | ... | column |
            // +-----+-----------+--------------+----+---+-----+--------+
            //                                       |
            //                          +-----------+v------+
            //                          | int32 len | bytes |
            //                          +-----------+-------+
            BeMessage::DataRow(vals) => {
                buf.put_u8(b'D');
                write_body(buf, |buf| {
                    buf.put_u16(vals.len() as u16); // num of cols
                    for val_opt in vals.values() {
                        if let Some(val) = val_opt {
                            buf.put_u32(val.len() as u32);
                            buf.put_slice(val.as_bytes());
                        } else {
                            buf.put_i32(-1);
                        }
                    }
                    Ok(())
                })
                .unwrap();
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
                buf.put_u8(b'T');
                write_body(buf, |buf| {
                    buf.put_i16(row_descs.len() as i16); // # of fields
                    for pg_field in row_descs.iter() {
                        write_cstr(buf, pg_field.get_name().as_bytes())?;
                        buf.put_i32(pg_field.get_table_oid()); // table oid
                        buf.put_i16(pg_field.get_col_attr_num()); // attnum
                        buf.put_i32(pg_field.get_type_oid().as_number());
                        buf.put_i16(pg_field.get_type_len());
                        buf.put_i32(pg_field.get_type_modifier()); // typmod
                        buf.put_i16(pg_field.get_format_code()); // format code
                    }
                    Ok(())
                })?;
            }
            // ReadyForQuery
            // +-----+----------+---------------------------+
            // | 'Z' | int32(5) | byte1(transaction status) |
            // +-----+----------+---------------------------+
            BeMessage::ReadyForQuery => {
                buf.put_u8(b'Z');
                buf.put_i32(5);
                // TODO: add transaction status
                buf.put_u8(b'I');
            }

            BeMessage::ParseComplete => {
                buf.put_u8(b'1');
                write_body(buf, |_| Ok(()))?;
            }

            BeMessage::BindComplete => {
                buf.put_u8(b'2');
                write_body(buf, |_| Ok(()))?;
            }

            BeMessage::ParameterDescription => {
                buf.put_u8(b't');
                write_body(buf, |buf| {
                    // we don't support params, so always 0
                    buf.put_i16(0);
                    Ok(())
                })
                .unwrap();
            }

            BeMessage::NoData => {
                buf.put_u8(b'n');
                write_body(buf, |_| Ok(())).unwrap();
            }

            BeMessage::EncryptionResponse => {
                buf.put_u8(b'N');
            }

            // EmptyQueryResponse
            // +-----+----------+
            // | 'I' | int32(4) |
            // +-----+----------+
            BeMessage::EmptyQueryResponse => {
                buf.put_u8(b'I');
                buf.put_i32(4);
            }

            BeMessage::ErrorResponse(error) => {
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'E' signalizes ErrorResponse messages
                buf.put_u8(b'E');
                write_body(buf, |buf| {
                    buf.put_u8(b'S'); // severity
                    write_cstr(buf, &Bytes::from("ERROR"))?;

                    buf.put_u8(b'C'); // SQLSTATE error code
                    write_cstr(buf, &Bytes::from("XX000"))?;

                    buf.put_u8(b'M'); // the message
                    write_cstr(buf, error.to_string().as_bytes())?;

                    buf.put_u8(0); // terminator
                    Ok(())
                })
                .unwrap();
            }
        }

        Ok(())
    }
}

// Safe usize -> i32|i16 conversion, from rust-postgres
trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> Result<$t> {
                if x > <$t>::max_value() as usize {
                    Err(Error::new(ErrorKind::InvalidInput, "value too large to transmit").into())
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i32);

/// Call f() to write body of the message and prepend it with 4-byte len as
/// prescribed by the protocol. First write out body value and fill length value as i32 in front of
/// it.
fn write_body<F>(buf: &mut BytesMut, f: F) -> Result<()>
where
    F: FnOnce(&mut BytesMut) -> Result<()>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    f(buf)?;

    let size = i32::from_usize(buf.len() - base)?;
    BigEndian::write_i32(&mut buf[base..], size);
    Ok(())
}

/// Safe write of s into buf as cstring (String in the protocol).
fn write_cstr(buf: &mut BytesMut, s: &[u8]) -> Result<()> {
    if s.contains(&0) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "string contains embedded null",
        ));
    }
    buf.put_slice(s);
    buf.put_u8(0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::pg_message::FeQueryMessage;

    #[tokio::test]
    async fn test_get_sql() {
        let fe = FeQueryMessage {
            sql_bytes: Bytes::from(vec![255, 255, 255, 255, 255, 255, 0]),
        };
        assert!(fe.get_sql().is_err(), "{}", true);
        let fe = FeQueryMessage {
            sql_bytes: Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        };
        assert!(fe.get_sql().is_err(), "{}", true);
    }
}
