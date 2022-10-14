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
use std::ffi::CStr;
use std::io::{Error, ErrorKind, IoSlice, Result, Write};

use byteorder::{BigEndian, ByteOrder};
/// Part of code learned from <https://github.com/zenithdb/zenith/blob/main/zenith_utils/src/pq_proto.rs>.
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_response::StatementType;
use crate::pg_server::BoxedError;
use crate::types::Row;

/// Messages that can be sent from pg client to server. Implement `read`.
pub enum FeMessage {
    Ssl,
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
}

pub struct FeStartupMessage {
    pub config: HashMap<String, String>,
}

impl FeStartupMessage {
    pub fn build_with_payload(payload: &[u8]) -> Result<Self> {
        let config = match std::str::from_utf8(payload) {
            Ok(v) => Ok(v.trim_end_matches('\0')),
            Err(err) => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("Input end error: {}", err),
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
            map.insert(chunk[0].to_string(), chunk[1].to_string());
        });
        Ok(FeStartupMessage { config: map })
    }
}

/// Query message contains the string sql.
pub struct FeQueryMessage {
    pub sql_bytes: Bytes,
}

#[derive(Debug)]
pub struct FeBindMessage {
    // param_format_code:
    //  false: text
    //  true: binary
    pub param_format_code: bool,

    // result_format_code:
    //  false: text
    //  true: binary
    pub result_format_code: bool,

    pub params: Vec<Bytes>,
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
        // Read FormatCode
        let len = buf.get_i16();

        let param_format_code = if len == 0 || len == 1 {
            if len == 0 {
                false
            } else {
                buf.get_i16() == 1
            }
        } else {
            let first_value = buf.get_i16();
            for _ in 1..len {
                assert!(buf.get_i16() == first_value,"Only support uniform param format (TEXT or BINARY), can't support mix format now.");
            }
            first_value == 1
        };
        // Read Params
        let len = buf.get_i16();
        let params = (0..len)
            .map(|_| {
                let val_len = buf.get_i32();
                buf.copy_to_bytes(val_len as usize)
            })
            .collect();
        // Read ResultFormatCode
        let len = buf.get_i16();

        assert!(len==0||len==1,"Only support default result format(len==0) or uniform result format(len==1), can't support mix format now.");

        let result_format_code = if len == 0 {
            // default format:text
            false
        } else {
            buf.get_i16() == 1
        };

        Ok(FeMessage::Bind(FeBindMessage {
            param_format_code,
            result_format_code,
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
        let len = stream.read_i32().await?;
        let protocol_num = stream.read_i32().await?;
        let payload_len = len - 8;
        let mut payload = vec![0; payload_len as usize];
        if payload_len > 0 {
            stream.read_exact(&mut payload).await?;
        }
        match protocol_num {
            // code from: https://www.postgresql.org/docs/current/protocol-message-formats.html
            196608 => Ok(FeMessage::Startup(FeStartupMessage::build_with_payload(
                &payload,
            )?)),
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
    EncryptionResponse,
    EmptyQueryResponse,
    ParseComplete,
    BindComplete,
    PortalSuspended,
    ParameterDescription(&'a [TypeOid]),
    NoData,
    DataRow(&'a Row),
    ParameterStatus(BeParameterStatusMessage<'a>),
    ReadyForQuery,
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
}

#[derive(Debug)]
pub struct BeCommandCompleteMessage {
    pub stmt_type: StatementType,
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
                let mut field = BytesMut::new();
                field.put_u8(b'M');
                field.put_slice(b"NOTICE: ");
                field.put_slice(notice.as_bytes());
                write_body(buf, |stream| {
                    write_cstr(stream, field.chunk())?;
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
                    for oid in para_descs.iter() {
                        buf.put_i32(oid.as_number());
                    }
                    Ok(())
                })?;
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
