// Copyright 2023 RisingWave Labs
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

use std::vec::IntoIter;

use futures::stream::FusedStream;
use futures::{StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::{PsqlError, PsqlResult};
use crate::pg_message::{BeCommandCompleteMessage, BeMessage};
use crate::pg_protocol::Conn;
use crate::pg_response::{PgResponse, ValuesStream};
use crate::types::Row;

pub struct ResultCache<VS>
where
    VS: ValuesStream,
{
    result: PgResponse<VS>,
    row_cache: IntoIter<Row>,
}

impl<VS> ResultCache<VS>
where
    VS: ValuesStream,
{
    pub fn new(result: PgResponse<VS>) -> Self {
        ResultCache {
            result,
            row_cache: vec![].into_iter(),
        }
    }

    /// Return indicate whether the result is consumed completely.
    pub async fn consume<S: AsyncWrite + AsyncRead + Unpin>(
        &mut self,
        row_limit: usize,
        msg_stream: &mut Conn<S>,
    ) -> PsqlResult<bool> {
        for notice in self.result.get_notices() {
            msg_stream.write_no_flush(&BeMessage::NoticeResponse(notice))?;
        }
        if self.result.is_empty() {
            // Run the callback before sending the response.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(&BeMessage::EmptyQueryResponse)?;
            return Ok(true);
        }

        let mut query_end = false;
        if self.result.is_query() {
            let mut query_row_count = 0;

            // fetch row data
            // if row_limit is 0, fetch all rows
            // if row_limit > 0, fetch row_limit rows
            while row_limit == 0 || query_row_count < row_limit {
                if self.row_cache.len() > 0 {
                    for row in self.row_cache.by_ref() {
                        msg_stream.write_no_flush(&BeMessage::DataRow(&row))?;
                        query_row_count += 1;
                        if row_limit > 0 && query_row_count >= row_limit {
                            break;
                        }
                    }
                } else {
                    self.row_cache = if let Some(rows) = self
                        .result
                        .values_stream()
                        .try_next()
                        .await
                        .map_err(|err| PsqlError::ExecuteError(err))?
                    {
                        rows.into_iter()
                    } else {
                        query_end = true;
                        break;
                    };
                }
            }

            // Check if the result is consumed completely.
            // If not, cache the result.
            if self.row_cache.len() == 0 && self.result.values_stream().peekable().is_terminated() {
                query_end = true;
            }
            if query_end {
                // Run the callback before sending the `CommandComplete` message.
                self.result.run_callback().await?;

                msg_stream.write_no_flush(&BeMessage::CommandComplete(
                    BeCommandCompleteMessage {
                        stmt_type: self.result.get_stmt_type(),
                        rows_cnt: query_row_count as i32,
                    },
                ))?;
            } else {
                msg_stream.write_no_flush(&BeMessage::PortalSuspended)?;
            }
        } else {
            // Run the callback before sending the `CommandComplete` message.
            self.result.run_callback().await?;

            msg_stream.write_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: self.result.get_stmt_type(),
                rows_cnt: self
                    .result
                    .get_effected_rows_cnt()
                    .expect("row count should be set"),
            }))?;

            query_end = true;
        }

        Ok(query_end)
    }
}
