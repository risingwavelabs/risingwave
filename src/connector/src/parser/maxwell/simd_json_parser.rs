// Copyright 2023 Singularity Data
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

use std::fmt::Debug;

use futures::future::ready;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use simd_json::{BorrowedValue, ValueAccess};

use super::operators::*;
use crate::parser::common::simd_json_parse_value;
use crate::parser::{ParseFuture, SourceParser, SourceStreamChunkRowWriter, WriteGuard};

const AFTER: &str = "data";
const BEFORE: &str = "old";
const OP: &str = "type";

#[derive(Debug)]
pub struct MaxwellParser;

impl MaxwellParser {
    fn parse_inner(
        &self,
        payload: &[u8],
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        let mut payload_mut = payload.to_vec();
        let event: BorrowedValue<'_> = simd_json::to_borrowed_value(&mut payload_mut)
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        let op = event.get(OP).and_then(|v| v.as_str()).ok_or_else(|| {
            RwError::from(ProtocolError(
                "op field not found in maxwell json".to_owned(),
            ))
        })?;

        match op {
            MAXWELL_INSERT_OP => {
                let after = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for creating event".to_string(),
                    ))
                })?;
                writer.insert(|column| {
                    simd_json_parse_value(
                        &column.data_type,
                        after.get(column.name.to_ascii_lowercase().as_str()),
                    )
                    .map_err(Into::into)
                })
            }
            MAXWELL_UPDATE_OP => {
                let after = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "data is missing for updating event".to_string(),
                    ))
                })?;
                let before = event.get(BEFORE).ok_or_else(|| {
                    RwError::from(ProtocolError(
                        "old is missing for updating event".to_string(),
                    ))
                })?;

                writer.update(|column| {
                    // old only contains the changed columns but data contains all columns.
                    let col_name_lc = column.name.to_ascii_lowercase();
                    let before_value = before
                        .get(col_name_lc.as_str())
                        .or_else(|| after.get(col_name_lc.as_str()));
                    let before = simd_json_parse_value(&column.data_type, before_value)?;
                    let after =
                        simd_json_parse_value(&column.data_type, after.get(col_name_lc.as_str()))?;
                    Ok((before, after))
                })
            }
            MAXWELL_DELETE_OP => {
                let before = event.get(AFTER).ok_or_else(|| {
                    RwError::from(ProtocolError("old is missing for delete event".to_string()))
                })?;
                writer.delete(|column| {
                    simd_json_parse_value(
                        &column.data_type,
                        before.get(column.name.to_ascii_lowercase().as_str()),
                    )
                    .map_err(Into::into)
                })
            }
            other => Err(RwError::from(ProtocolError(format!(
                "unknown Maxwell op: {}",
                other
            )))),
        }
    }
}

impl SourceParser for MaxwellParser {
    type ParseResult<'a> = impl ParseFuture<'a, Result<WriteGuard>>;

    fn parse<'a, 'b, 'c>(
        &'a self,
        payload: &'b [u8],
        writer: SourceStreamChunkRowWriter<'c>,
    ) -> Self::ParseResult<'a>
    where
        'b: 'a,
        'c: 'a,
    {
        ready(self.parse_inner(payload, writer))
    }
}
