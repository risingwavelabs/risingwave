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
use std::ffi::CStr;
use std::io::{Error, ErrorKind, IoSlice, Result, Write};

use anyhow::anyhow;
use byteorder::{BigEndian, ByteOrder};
/// Part of code learned from <https://github.com/zenithdb/zenith/blob/main/zenith_utils/src/pq_proto.rs>.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use peekable::tokio::AsyncPeekable;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error_or_notice::ErrorOrNoticeMessage;
use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_response::StatementType;
use crate::pg_server::BoxedError;
use crate::types::Row;

/// Messages that can be sent from pg client to server. Implement `read`.
#[derive(Debug)]
pub enum FeMessage {
    Ssl,
    Gss,
    Startup(FeStartupMessage),
    Query(FeQueryMessage),
    Parse(FeParseMessage),
    Password(FePasswordMessage),
    Describe(FeDescribeMessage),
    Bind(FeBindMessage),
    Execute(FeExecuteMessage),
    Close(FeCloseMessage),
    Sync,
    CancelQuery(FeCancelMessage),
    Terminate,
    Flush,
    // special msg to detect health check, which represents the client immediately closes the connection cleanly without sending any data.
    HealthCheck,
}

#[derive(Debug)]
pub struct FeStartupMessage {
    pub config: HashMap<String, String>,
}

impl FeStartupMessage {
    pub fn build_with_payload(payload: &[u8]) -> Result<Self> {
        let config = match std::str::from_utf8(payload) {
            Ok(v) => Ok(v.trim_end_matches('\0')),
            Err(err) => Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!(err).context("Input end error"),
            )),
        }?;
        let mut map = HashMap::new();
        let config: Vec<&str> = config.split('\0').collect();
        if config.len() % 2 == 1 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Invalid input config: odd number of config pairs",
            ));
        }
        config.chunks(2).for_each(|chunk| {
            map.insert(chunk[0].to_owned(), chunk[1].to_owned());
        });
        Ok(FeStartupMessage { config: map })
    }
}

/// Query message contains the string sql.
#[derive(Debug)]
pub struct FeQueryMessage {
    pub sql_bytes: Bytes,
}

#[derive(Debug)]
pub struct FeBindMessage {
    pub param_format_codes: Vec<i16>,
    pub result_format_codes: Vec<i16>,

    pub params: Vec<Option<Bytes>>,
    pub portal_name: Bytes,
    pub statement_name: Bytes,
}

#[derive(Debug)]
pub struct FeExecuteMessage {
    pub portal_name: Bytes,
    pub max_rows: i32,
}

#[derive(Debug)]
pub struct FeParseMessage {
    pub statement_name: Bytes,
    pub sql_bytes: Bytes,
    pub type_ids: Vec<i32>,
}

#[derive(Debug)]
pub struct FePasswordMessage {
    pub password: Bytes,
}

#[derive(Debug)]
pub struct FeDescribeMessage {
    // 'S' to describe a prepared statement; or 'P' to describe a portal.
    pub kind: u8,
    pub name: Bytes,
}

#[derive(Debug)]
pub struct FeCloseMessage {
    pub kind: u8,
    pub name: Bytes,
}

#[derive(Debug)]
pub struct FeCancelMessage {
    pub target_process_id: i32,
    pub target_secret_key: i32,
}

impl FeCancelMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let target_process_id = buf.get_i32();
        let target_secret_key = buf.get_i32();
        Ok(FeMessage::CancelQuery(Self {
            target_process_id,
            target_secret_key,
        }))
    }
}

impl FeDescribeMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let kind = buf.get_u8();
        let name = read_null_terminated(&mut buf)?;

        Ok(FeMessage::Describe(FeDescribeMessage { kind, name }))
    }
}

impl FeBindMessage {
    // Bind Message Header
    // +-----+-----------+
    // | 'B' | int32 len |
    // +-----+-----------+
    // Bind Message Body
    // +----------------+---------------+
    // | str portalname | str statement |
    // +----------------+---------------+
    // +---------------------+------------------+-------+
    // | int16 numFormatCode | int16 FormatCode |  ...  |
    // +---------------------+------------------+-------+
    // +-----------------+-------------------+---------------+
    // | int16 numParams | int32 valueLength |  byte value.. |
    // +-----------------+-------------------+---------------+
    // +----------------------------------+------------------+-------+
    // | int16 numResultColumnFormatCodes | int16 FormatCode |  ...  |
    // +----------------------------------+------------------+-------+
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let statement_name = read_null_terminated(&mut buf)?;

        let len = buf.get_i16();
        let param_format_codes = (0..len).map(|_| buf.get_i16()).collect();

        // Read Params
        let len = buf.get_i16();
        let params = (0..len)
            .map(|_| {
                let val_len = buf.get_i32();
                if val_len == -1 {
                    None
                } else {
                    Some(buf.copy_to_bytes(val_len as usize))
                }
            })
            .collect();

        let len = buf.get_i16();
        let result_format_codes = (0..len).map(|_| buf.get_i16()).collect();

        Ok(FeMessage::Bind(FeBindMessage {
            param_format_codes,
            result_format_codes,
            params,
            portal_name,
            statement_name,
        }))
    }
}

impl FeExecuteMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let portal_name = read_null_terminated(&mut buf)?;
        let max_rows = buf.get_i32();

        Ok(FeMessage::Execute(FeExecuteMessage {
            portal_name,
            max_rows,
        }))
    }
}

impl FeParseMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let statement_name = read_null_terminated(&mut buf)?;
        let sql_bytes = read_null_terminated(&mut buf)?;
        let nparams = buf.get_i16();

        let type_ids: Vec<i32> = (0..nparams).map(|_| buf.get_i32()).collect();

        Ok(FeMessage::Parse(FeParseMessage {
            statement_name,
            sql_bytes,
            type_ids,
        }))
    }
}

impl FePasswordMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let password = read_null_terminated(&mut buf)?;

        Ok(FeMessage::Password(FePasswordMessage { password }))
    }
}

impl FeQueryMessage {
    pub fn get_sql(&self) -> Result<&str> {
        match CStr::from_bytes_with_nul(&self.sql_bytes) {
            Ok(cstr) => cstr.to_str().map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!(err).context("Invalid UTF-8 sequence"),
                )
            }),
            Err(err) => Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!(err).context("Input end error"),
            )),
        }
    }
}

impl FeCloseMessage {
    pub fn parse(mut buf: Bytes) -> Result<FeMessage> {
        let kind = buf.get_u8();
        let name = read_null_terminated(&mut buf)?;
        Ok(FeMessage::Close(FeCloseMessage { kind, name }))
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
            b'C' => FeCloseMessage::parse(sql_bytes),
            b'p' => FePasswordMessage::parse(sql_bytes),
            b'H' => Ok(FeMessage::Flush),
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
        let mut stream = AsyncPeekable::new(stream);

        if let Err(err) = stream.peek_exact(&mut [0; 1]).await {
            // If the stream is empty, it can be a health check. Do not return error.
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(FeMessage::HealthCheck);
            } else {
                return Err(err);
            }
        }

        let len = stream.read_i32().await?;
        let protocol_num = stream.read_i32().await?;
        let payload_len = (len - 8) as usize;
        if payload_len >= isize::MAX as usize {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("Payload length has exceed usize::MAX {:?}", payload_len),
            ));
        }
        let mut payload = vec![0; payload_len];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        match protocol_num {
            // code from: https://www.postgresql.org/docs/current/protocol-message-formats.html
            196608 => Ok(FeMessage::Startup(FeStartupMessage::build_with_payload(
                &payload,
            )?)),
            80877104 => Ok(FeMessage::Gss),
            80877103 => Ok(FeMessage::Ssl),
            // Cancel request code.
            80877102 => FeCancelMessage::parse(Bytes::from(payload)),
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
/// Ref: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[derive(Debug)]
pub enum BeMessage<'a> {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationMd5Password(&'a [u8; 4]),
    CommandComplete(BeCommandCompleteMessage),
    NoticeResponse(&'a str),
    // Single byte - used in response to SSLRequest/GSSENCRequest.
    EncryptionResponseSsl,
    EncryptionResponseGss,
    EncryptionResponseNo,
    EmptyQueryResponse,
    ParseComplete,
    BindComplete,
    PortalSuspended,
    // array of parameter oid(i32)
    ParameterDescription(&'a [i32]),
    NoData,
    DataRow(&'a Row),
    ParameterStatus(BeParameterStatusMessage<'a>),
    ReadyForQuery(TransactionStatus),
    RowDescription(&'a [PgFieldDescriptor]),
    ErrorResponse(BoxedError),
    CloseComplete,

    // 0: process ID, 1: secret key
    BackendKeyData((i32, i32)),
}

#[derive(Debug)]
pub enum BeParameterStatusMessage<'a> {
    ClientEncoding(&'a str),
    StandardConformingString(&'a str),
    ServerVersion(&'a str),
    ApplicationName(&'a str),
}

#[derive(Debug)]
pub struct BeCommandCompleteMessage {
    pub stmt_type: StatementType,
    pub rows_cnt: i32,
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    InFailedTransaction,
}

impl BeMessage<'_> {
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

            // AuthenticationCleartextPassword
            // +-----+----------+-----------+
            // | 'R' | int32(8) | int32(3)  |
            // +-----+----------+-----------+
            BeMessage::AuthenticationCleartextPassword => {
                buf.put_u8(b'R');
                buf.put_i32(8);
                buf.put_i32(3);
            }

            // AuthenticationMD5Password
            // +-----+----------+-----------+----------------+
            // | 'R' | int32(12) | int32(5)  |  Byte4(salt)  |
            // +-----+----------+-----------+----------------+
            //
            // The 4-byte random salt will be used by client to send encrypted password as
            // concat('md5', md5(concat(md5(concat(password, username)), random-salt))).
            BeMessage::AuthenticationMd5Password(salt) => {
                buf.put_u8(b'R');
                buf.put_i32(12);
                buf.put_i32(5);
                buf.put_slice(&salt[..]);
            }

            // ParameterStatus
            // +-----+-----------+----------+------+-----------+------+
            // | 'S' | int32 len | str name | '\0' | str value | '\0' |
            // +-----+-----------+----------+------+-----------+------+
            BeMessage::ParameterStatus(param) => {
                use BeParameterStatusMessage::*;
                let [name, value] = match param {
                    ClientEncoding(val) => [b"client_encoding", val.as_bytes()],
                    StandardConformingString(val) => {
                        [b"standard_conforming_strings", val.as_bytes()]
                    }
                    ServerVersion(val) => [b"server_version", val.as_bytes()],
                    ApplicationName(val) => [b"application_name", val.as_bytes()],
                };

                // Parameter names and values are passed as null-terminated strings
                let iov = &mut [name, b"\0", value, b"\0"].map(IoSlice::new);
                let mut buffer = vec![];
                let cnt = buffer.write_vectored(iov).unwrap();

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
                let mut stmt_type = cmd.stmt_type;
                let mut tag = "".to_owned();
                stmt_type = match stmt_type {
                    StatementType::INSERT_RETURNING => StatementType::INSERT,
                    StatementType::DELETE_RETURNING => StatementType::DELETE,
                    StatementType::UPDATE_RETURNING => StatementType::UPDATE,
                    s => s,
                };
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

            // NoticeResponse
            // +-----+-----------+------------------+------------------+
            // | 'N' | int32 len | byte1 field type | str field value  |
            // +-----+-----------+------------------+-+----------------+
            // description of the fields can be found here:
            // https://www.postgresql.org/docs/current/protocol-error-fields.html
            BeMessage::NoticeResponse(notice) => {
                buf.put_u8(b'N');
                write_err_or_notice(buf, &ErrorOrNoticeMessage::notice(notice))?;
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
                            buf.put_slice(val);
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
                    for pg_field in *row_descs {
                        write_cstr(buf, pg_field.get_name().as_bytes())?;
                        buf.put_i32(pg_field.get_table_oid()); // table oid
                        buf.put_i16(pg_field.get_col_attr_num()); // attnum
                        buf.put_i32(pg_field.get_type_oid());
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
            BeMessage::ReadyForQuery(txn_status) => {
                buf.put_u8(b'Z');
                buf.put_i32(5);
                // TODO: add transaction status
                buf.put_u8(match txn_status {
                    TransactionStatus::Idle => b'I',
                    TransactionStatus::InTransaction => b'T',
                    TransactionStatus::InFailedTransaction => b'E',
                });
            }

            BeMessage::ParseComplete => {
                buf.put_u8(b'1');
                write_body(buf, |_| Ok(()))?;
            }

            BeMessage::BindComplete => {
                buf.put_u8(b'2');
                write_body(buf, |_| Ok(()))?;
            }

            BeMessage::CloseComplete => {
                buf.put_u8(b'3');
                write_body(buf, |_| Ok(()))?;
            }

            BeMessage::PortalSuspended => {
                buf.put_u8(b's');
                write_body(buf, |_| Ok(()))?;
            }
            // ParameterDescription
            // +-----+-----------+--------------------+---------------+-----+---------------+
            // | 't' | int32 len | int16 ParameterNum | int32 typeOID | ... | int32 typeOID |
            // +-----+-----------+-----------------+--+---------------+-----+---------------+
            BeMessage::ParameterDescription(para_descs) => {
                buf.put_u8(b't');
                write_body(buf, |buf| {
                    buf.put_i16(para_descs.len() as i16);
                    for oid in *para_descs {
                        buf.put_i32(*oid);
                    }
                    Ok(())
                })?;
            }

            BeMessage::NoData => {
                buf.put_u8(b'n');
                write_body(buf, |_| Ok(())).unwrap();
            }

            BeMessage::EncryptionResponseSsl => {
                buf.put_u8(b'S');
            }

            BeMessage::EncryptionResponseGss => {
                buf.put_u8(b'G');
            }

            BeMessage::EncryptionResponseNo => {
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
                use thiserror_ext::AsReport;
                // For all the errors set Severity to Error and error code to
                // 'internal error'.

                // 'E' signalizes ErrorResponse messages
                buf.put_u8(b'E');
                // Format the error as a pretty report.
                let msg = error.to_report_string_pretty();
                write_err_or_notice(buf, &ErrorOrNoticeMessage::internal_error(&msg))?;
            }

            BeMessage::BackendKeyData((process_id, secret_key)) => {
                buf.put_u8(b'K');
                write_body(buf, |buf| {
                    buf.put_i32(*process_id);
                    buf.put_i32(*secret_key);
                    Ok(())
                })?;
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
                if x > <$t>::MAX as usize {
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

/// Safe write error or notice message.
fn write_err_or_notice(buf: &mut BytesMut, msg: &ErrorOrNoticeMessage<'_>) -> Result<()> {
    write_body(buf, |buf| {
        buf.put_u8(b'S'); // severity
        write_cstr(buf, msg.severity.as_str().as_bytes())?;

        buf.put_u8(b'C'); // SQLSTATE error code
        write_cstr(buf, msg.state.code().as_bytes())?;

        buf.put_u8(b'M'); // the message
        write_cstr(buf, msg.message.as_bytes())?;

        buf.put_u8(0); // terminator
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::pg_message::FeQueryMessage;

    #[test]
    fn test_get_sql() {
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
