// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use anyhow::anyhow;
use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op};
use risingwave_connector::parser::plain_parser::PlainParser;
use risingwave_connector::parser::{
    BigintUnsignedHandlingMode, ByteStreamSourceParser, EncodingProperties, JsonProperties,
    ParseResult, ProtocolProperties, SourceStreamChunkBuilder, SpecificParserConfig, TimeHandling,
    TimestampHandling, TimestamptzHandling,
};
use risingwave_connector::source::{SourceColumnDesc, SourceContext, SourceCtrlOpts};

use super::utils::{Result, err};

const WEBHOOK_FORMAT_HEADER: &str = "x-rw-webhook-format";
const WEBHOOK_ENCODE_HEADER: &str = "x-rw-webhook-encode";
const WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER: &str = "x-rw-webhook-json-timestamp-handling-mode";
const WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER: &str =
    "x-rw-webhook-json-timestamptz-handling-mode";
const WEBHOOK_JSON_TIME_HANDLING_HEADER: &str = "x-rw-webhook-json-time-handling-mode";
const WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER: &str =
    "x-rw-webhook-json-bigint-unsigned-handling-mode";
const WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER: &str = "x-rw-webhook-json-handle-toast-columns";

#[derive(Debug)]
pub(super) struct WebhookJsonDecoder {
    columns: Vec<SourceColumnDesc>,
    parser: PlainParser,
}

impl WebhookJsonDecoder {
    pub(super) async fn new(headers: &HeaderMap, columns: Vec<SourceColumnDesc>) -> Result<Self> {
        let parser = PlainParser::new(
            SpecificParserConfig {
                encoding_config: EncodingProperties::Json(json_properties_from_headers(headers)?),
                protocol_config: ProtocolProperties::Plain,
            },
            columns.clone(),
            Arc::new(SourceContext::dummy()),
        )
        .await
        .map_err(|e| {
            err(
                anyhow!(e).context("failed to build webhook JSON decoder"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;

        Ok(Self { columns, parser })
    }

    pub(super) fn decode(&mut self, is_batched: bool, body: &Bytes) -> Result<DataChunk> {
        let source_ctrl_opts = SourceCtrlOpts {
            chunk_size: body.len().max(1),
            split_txn: false,
        };
        let mut chunk_builder =
            SourceStreamChunkBuilder::new(self.columns.clone(), source_ctrl_opts);

        for row in payload_rows(is_batched, body) {
            match self
                .parser
                .parse_one_with_txn(None, Some(row.to_vec()), chunk_builder.row_writer())
                .now_or_never()
                .ok_or_else(|| {
                    err(
                        anyhow!("webhook JSON decoder unexpectedly yielded"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                })?
                .map_err(|e| {
                    err(
                        anyhow!(e).context("failed to decode webhook JSON payload"),
                        StatusCode::UNPROCESSABLE_ENTITY,
                    )
                })? {
                ParseResult::Rows => {}
                ParseResult::TransactionControl(_) | ParseResult::SchemaChange(_) => {
                    return Err(err(
                        anyhow!("unexpected non-row webhook JSON parse result"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ));
                }
            }
        }

        chunk_builder.finish_current_chunk();
        let chunks = chunk_builder.consume_ready_chunks().collect_vec();
        let [chunk] = chunks.try_into().map_err(|chunks: Vec<_>| {
            err(
                anyhow!("expected one decoded webhook chunk, got {}", chunks.len()),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        let (data_chunk, ops) = chunk.into_parts();
        if !ops.iter().all(|op| matches!(op, Op::Insert)) {
            return Err(err(
                anyhow!("webhook JSON decoder emitted non-insert rows"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
        Ok(data_chunk)
    }
}

fn json_properties_from_headers(headers: &HeaderMap) -> Result<JsonProperties> {
    let format = header_value(headers, WEBHOOK_FORMAT_HEADER)?.unwrap_or_else(|| "plain".into());
    if !format.eq_ignore_ascii_case("plain") {
        return Err(err(
            anyhow!("unsupported webhook payload format `{format}`"),
            StatusCode::BAD_REQUEST,
        ));
    }

    let encode = header_value(headers, WEBHOOK_ENCODE_HEADER)?.unwrap_or_else(|| "json".into());
    if !encode.eq_ignore_ascii_case("json") {
        return Err(err(
            anyhow!("unsupported webhook payload encode `{encode}`"),
            StatusCode::BAD_REQUEST,
        ));
    }

    let timestamp_handling =
        parse_timestamp_handling(headers, WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER)?;
    let timestamptz_handling =
        parse_timestamptz_handling(headers, WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER)?;
    let time_handling = parse_time_handling(headers, WEBHOOK_JSON_TIME_HANDLING_HEADER)?;
    let bigint_unsigned_handling =
        parse_bigint_unsigned_handling(headers, WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER)?;
    let handle_toast_columns =
        parse_bool_header(headers, WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER)?.unwrap_or(false);

    Ok(JsonProperties {
        use_schema_registry: false,
        timestamp_handling,
        timestamptz_handling,
        time_handling,
        bigint_unsigned_handling,
        handle_toast_columns,
    })
}

fn parse_timestamp_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<TimestampHandling>> {
    match header_value(headers, key)?.as_deref() {
        Some("milli") => Ok(Some(TimestampHandling::Milli)),
        Some("guess_number_unit") => Ok(Some(TimestampHandling::GuessNumberUnit)),
        Some(value) => Err(err(
            anyhow!("unrecognized `{key}` value `{value}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_timestamptz_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<TimestamptzHandling>> {
    header_value(headers, key)?
        .as_deref()
        .map(TimestamptzHandling::from_options)
        .transpose()
        .map_err(|e| {
            err(
                anyhow!(e).context("invalid webhook JSON decoder option"),
                StatusCode::BAD_REQUEST,
            )
        })
}

fn parse_time_handling(headers: &HeaderMap, key: &'static str) -> Result<Option<TimeHandling>> {
    match header_value(headers, key)?.as_deref() {
        Some("milli") => Ok(Some(TimeHandling::Milli)),
        Some("micro") => Ok(Some(TimeHandling::Micro)),
        Some(value) => Err(err(
            anyhow!("unrecognized `{key}` value `{value}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_bigint_unsigned_handling(
    headers: &HeaderMap,
    key: &'static str,
) -> Result<Option<BigintUnsignedHandlingMode>> {
    match header_value(headers, key)?.as_deref() {
        Some("long") => Ok(Some(BigintUnsignedHandlingMode::Long)),
        Some("precise") => Ok(Some(BigintUnsignedHandlingMode::Precise)),
        Some(value) => Err(err(
            anyhow!("unrecognized `{key}` value `{value}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn parse_bool_header(headers: &HeaderMap, key: &'static str) -> Result<Option<bool>> {
    match header_value(headers, key)?.as_deref() {
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(value) => Err(err(
            anyhow!("unrecognized `{key}` value `{value}`"),
            StatusCode::BAD_REQUEST,
        )),
        None => Ok(None),
    }
}

fn header_value(headers: &HeaderMap, key: &'static str) -> Result<Option<String>> {
    headers
        .get(key)
        .map(|value| {
            value.to_str().map(|value| value.to_owned()).map_err(|e| {
                err(
                    anyhow!(e).context(format!("invalid UTF-8 in `{key}` header")),
                    StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()
}

fn payload_rows(is_batched: bool, body: &Bytes) -> impl Iterator<Item = &[u8]> {
    let split_rows = is_batched.then(|| body.split(|&byte| byte == b'\n'));
    split_rows
        .into_iter()
        .flatten()
        .chain((!is_batched).then_some(body.as_ref()))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};

    use super::*;

    async fn decoder(columns: Vec<(&str, DataType, bool)>) -> WebhookJsonDecoder {
        WebhookJsonDecoder::new(
            &HeaderMap::new(),
            columns
                .into_iter()
                .enumerate()
                .map(|(idx, (name, data_type, is_pk))| {
                    SourceColumnDesc::from_column_desc(
                        &ColumnDesc::named(name, ColumnId::new(idx as i32), data_type),
                        is_pk,
                    )
                })
                .collect(),
        )
        .await
        .unwrap()
    }

    #[test]
    fn test_parse_plain_json_config_defaults() {
        let config = json_properties_from_headers(&HeaderMap::new()).unwrap();
        assert!(config.timestamptz_handling.is_none());
    }

    #[test]
    fn test_parse_plain_json_config_rejects_unsupported_encode() {
        let mut headers = HeaderMap::new();
        headers.insert(WEBHOOK_ENCODE_HEADER, HeaderValue::from_static("csv"));
        let err = json_properties_from_headers(&headers).unwrap_err();
        assert_eq!(err.code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_parse_plain_json_config_accepts_timestamptz_mode() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        let config = json_properties_from_headers(&headers).unwrap();
        assert!(matches!(
            config.timestamptz_handling,
            Some(TimestamptzHandling::Milli)
        ));
    }

    #[test]
    fn test_parse_plain_json_config_accepts_all_supported_modes() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            HeaderValue::from_static("micro"),
        );
        headers.insert(
            WEBHOOK_JSON_TIME_HANDLING_HEADER,
            HeaderValue::from_static("milli"),
        );
        headers.insert(
            WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER,
            HeaderValue::from_static("precise"),
        );
        headers.insert(
            WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER,
            HeaderValue::from_static("true"),
        );

        let config = json_properties_from_headers(&headers).unwrap();
        assert!(matches!(
            config.timestamp_handling,
            Some(TimestampHandling::Milli)
        ));
        assert!(matches!(
            config.timestamptz_handling,
            Some(TimestamptzHandling::Micro)
        ));
        assert!(matches!(config.time_handling, Some(TimeHandling::Milli)));
        assert!(matches!(
            config.bigint_unsigned_handling,
            Some(BigintUnsignedHandlingMode::Precise)
        ));
        assert!(config.handle_toast_columns);
    }

    #[test]
    fn test_parse_plain_json_config_rejects_invalid_supported_modes() {
        for header in [
            WEBHOOK_JSON_TIMESTAMP_HANDLING_HEADER,
            WEBHOOK_JSON_TIMESTAMPTZ_HANDLING_HEADER,
            WEBHOOK_JSON_TIME_HANDLING_HEADER,
            WEBHOOK_JSON_BIGINT_UNSIGNED_HANDLING_HEADER,
            WEBHOOK_JSON_HANDLE_TOAST_COLUMNS_HEADER,
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header, HeaderValue::from_static("invalid"));
            let err = json_properties_from_headers(&headers).unwrap_err();
            assert_eq!(err.code(), StatusCode::BAD_REQUEST, "{header}");
        }
    }

    #[tokio::test]
    async fn test_decode_plain_json_payload() {
        let mut decoder = decoder(vec![
            ("id", DataType::Int32, true),
            ("name", DataType::Varchar, false),
        ])
        .await;
        let chunk = decoder
            .decode(false, &Bytes::from_static(br#"{"id":1,"name":"alice"}"#))
            .unwrap();

        let row = chunk.rows().next().unwrap();
        assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
        assert_eq!(
            row.datum_at(1).to_owned_datum(),
            Some(ScalarImpl::Utf8("alice".into()))
        );
    }

    #[tokio::test]
    async fn test_decode_plain_json_payload_rejects_missing_pk() {
        let mut decoder = decoder(vec![
            ("id", DataType::Int32, true),
            ("name", DataType::Varchar, false),
        ])
        .await;
        let err = decoder
            .decode(false, &Bytes::from_static(br#"{"name":"alice"}"#))
            .unwrap_err();
        assert_eq!(err.code(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
