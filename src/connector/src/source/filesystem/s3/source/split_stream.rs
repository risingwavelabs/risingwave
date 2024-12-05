// Copyright 2024 RisingWave Labs
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

use anyhow::Context as _;
use bytes::BytesMut;
use futures::io::Cursor;
use futures::AsyncBufReadExt;
use futures_async_stream::try_stream;

use crate::source::{BoxSourceStream, SourceMessage};

#[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
/// This function splits a byte stream by the newline separator "(\r)\n" into a message stream.
/// It can be difficult to split and compute offsets correctly when the bytes are received in
/// chunks.  There are two cases to consider:
/// - When a bytes chunk does not end with "(\r)\n", we should not treat the last segment as a new line
///   message, but keep it for the next chunk, and prepend it to the first line of the next chunk.
/// - When a bytes chunk ends with "(\r)\n", there is no additional action required.
pub(super) async fn split_stream(data_stream: BoxSourceStream) {
    let mut last_message = None;

    #[for_await]
    for batch in data_stream {
        let batch = batch?;

        let Some((offset, split_id, meta)) = batch
            .first()
            .map(|msg| (msg.offset.clone(), msg.split_id.clone(), msg.meta.clone()))
        else {
            continue;
        };

        let mut offset: usize = offset.parse().context("failed to parse the offset")?;
        let mut buf = BytesMut::new();
        for msg in batch {
            let payload = msg.payload.unwrap_or_default();
            buf.extend(payload);
        }
        let mut msgs = Vec::new();

        let mut cursor = Cursor::new(buf.freeze());
        let mut line_cnt: usize = 0;
        loop {
            let mut line = String::new();
            match cursor.read_line(&mut line).await {
                Ok(0) => {
                    if !msgs.is_empty() {
                        yield msgs;
                    }
                    break;
                }
                Ok(_n) => {
                    if line_cnt == 0 && last_message.is_some() {
                        let msg: SourceMessage = std::mem::take(&mut last_message).unwrap();
                        let last_payload = msg.payload.unwrap();
                        offset -= last_payload.len();
                        line.insert_str(0, &String::from_utf8(last_payload).unwrap());
                    }

                    let mut separator = String::with_capacity(2);
                    for delim in ['\n', '\r'] {
                        if line.ends_with(delim) {
                            separator.insert(0, line.pop().unwrap());
                        } else {
                            // If the data is batched as "XXXX\r" and "\nXXXX",
                            // the line will be "XXXX\r" here because the cursor reaches EOF.
                            // Hence we should break the delim loop here,
                            // otherwise the \r would be treated as separator even without \n.
                            break;
                        }
                    }

                    let len = line.len();

                    offset += len + separator.len();
                    let msg = SourceMessage {
                        key: None,
                        payload: Some(line.into()),
                        offset: offset.to_string(),
                        split_id: split_id.clone(),
                        meta: meta.clone(),
                    };

                    msgs.push(msg);

                    if separator.is_empty() {
                        // Not ending with \n, prepend to the first line of the next batch
                        last_message = msgs.pop();
                    }
                }
                Err(e) => return Err(e.into()),
            }

            line_cnt += 1;
        }
    }

    if let Some(msg) = last_message {
        yield vec![msg];
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::{StreamExt, TryStreamExt};

    use super::*;

    #[tokio::test]
    async fn test_split_stream() {
        // Test with tail separators.
        for tail_separator in ["", "\n", "\r\n"] {
            const N1: usize = 1000;
            const N2: usize = 500;
            const N3: usize = 50;
            let lines = (0..N1)
                .map(|x| (0..x % N2).map(|_| 'A').collect::<String>())
                .collect::<Vec<_>>();
            let total_chars = lines.iter().map(|e| e.len()).sum::<usize>();
            // Join lines with \n & \r\n alternately
            let delims = ["\n", "\r\n"];
            let text = lines
                .iter()
                .enumerate()
                .skip(1)
                .fold(lines[0].clone(), |acc, (idx, now)| {
                    format!("{}{}{}", acc, delims[idx % 2], now)
                })
                + tail_separator;
            let text = text.into_bytes();
            let split_id: Arc<str> = "1".to_string().into_boxed_str().into();
            let s = text
                .chunks(N2)
                .enumerate()
                .map(move |(i, e)| {
                    Ok(e.chunks(N3)
                        .enumerate()
                        .map(|(j, buf)| SourceMessage {
                            key: None,
                            payload: Some(buf.to_owned()),
                            offset: (i * N2 + j * N3).to_string(),
                            split_id: split_id.clone(),
                            meta: crate::source::SourceMeta::Empty,
                        })
                        .collect::<Vec<_>>())
                })
                .collect::<Vec<_>>();
            let stream = futures::stream::iter(s).boxed();
            let msg_stream = split_stream(stream).try_collect::<Vec<_>>().await.unwrap();
            // Check the correctness of each line's offset
            let mut expected_offset: usize = 0;
            msg_stream
                .iter()
                .flatten()
                .enumerate()
                .for_each(|(idx, msg)| {
                    expected_offset += lines[idx].len()
                        + if idx < lines.len() - 1 {
                            delims[1 - idx % 2].len()
                        } else {
                            tail_separator.len()
                        };
                    assert_eq!(
                        msg.offset.parse::<usize>().unwrap(),
                        expected_offset,
                        "idx = {}, tail_separator = {:?}",
                        idx,
                        tail_separator
                    );
                });
            let items = msg_stream
                .into_iter()
                .flatten()
                .map(|e| String::from_utf8(e.payload.unwrap()).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(items.len(), N1);
            let text = items.join("");
            assert_eq!(text.len(), total_chars);
        }
    }
}
