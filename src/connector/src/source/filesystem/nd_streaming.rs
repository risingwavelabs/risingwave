use std::io::BufRead;

use anyhow::anyhow;
use bytes::BytesMut;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::error::RwError;

use crate::parser::ByteStreamSourceParser;
use crate::source::{BoxSourceStream, SourceMessage, StreamChunkWithState};
#[derive(Debug)]

/// A newline-delimited bytes stream wrapper that can any converts arbitrary file(bytes) streams
/// into message streams segmented by the newline character '\n'.
pub struct NdByteStreamWrapper<T> {
    inner: T,
}
impl<T> NdByteStreamWrapper<T> {
    pub fn new(inner: T) -> Self
    where
        T: ByteStreamSourceParser,
    {
        Self { inner }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    /// This function splits a byte stream by the newline character '\n' into a message stream.
    /// It can be difficult to split and compute offsets correctly when the bytes are received in
    /// chunks.  There are two cases to consider:
    /// - When a bytes chunk does not end with '\n', we should not treat the last segment as a new
    ///   line message, but keep it for the next chunk, and insert it before next chunk's first line
    ///   beginning.
    /// - When a bytes chunk ends with '\n', there is no additional action required.
    async fn split_stream(data_stream: BoxSourceStream) {
        let mut buf = BytesMut::new();

        let mut last_message = None;
        #[for_await]
        for batch in data_stream {
            let batch = batch?;

            if batch.is_empty() {
                continue;
            }
            let (offset, split_id, meta) = batch
                .first()
                .map(|msg| (msg.offset.clone(), msg.split_id.clone(), msg.meta.clone()))
                .unwrap(); // Never panic because we check batch is not empty

            let mut offset: usize = offset.parse()?;

            // Never panic because we check batch is not empty
            let last_offset: usize = batch.last().map(|m| m.offset.clone()).unwrap().parse()?;
            for (i, msg) in batch.into_iter().enumerate() {
                let payload = msg.payload.unwrap_or_default();
                if i == 0 {
                    // The 'offset' field in 'SourceMessage' indicates the end position of a chunk.
                    // But indicates the beginning here.
                    offset -= payload.len();
                }
                buf.extend(payload);
            }
            let mut msgs = Vec::new();
            for (i, line) in buf.lines().enumerate() {
                let mut line = line?;
                offset += line.len();
                // Insert the trailing of the last chunk in front of the first line, do not count
                // the length here.
                if i == 0 && last_message.is_some() {
                    let msg: SourceMessage = std::mem::take(&mut last_message).unwrap();
                    line = String::from_utf8(msg.payload.unwrap().into()).unwrap() + &line;
                }

                msgs.push(SourceMessage {
                    payload: Some(line.into()),
                    offset: offset.to_string(),
                    split_id: split_id.clone(),
                    meta: meta.clone(),
                });
                offset += 1;
            }

            if offset > last_offset {
                last_message = msgs.pop();
            }

            if !msgs.is_empty() {
                yield msgs;
            }

            buf.clear();
        }
        if let Some(msg) = last_message {
            yield vec![msg];
        }
    }
}
impl<T: ByteStreamSourceParser> ByteStreamSourceParser for NdByteStreamWrapper<T> {
    fn into_stream(
        self,
        data_stream: crate::source::BoxSourceStream,
    ) -> crate::source::BoxSourceWithStateStream {
        self.inner.into_stream(Self::split_stream(data_stream))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_split_stream() {
        const N1: usize = 10000;
        const N2: usize = 500;
        const N3: usize = 50;
        let lines = (0..N1)
            .map(|x| (0..x % N2).map(|y| 'A').collect::<String>())
            .collect::<Vec<_>>();
        let total_chars = lines.iter().map(|e| e.len()).sum::<usize>();
        let text = lines.join("\n").into_bytes();
        let split_id: Arc<str> = "1".to_string().into_boxed_str().into();
        let s = text
            .chunks(N2)
            .enumerate()
            .map(move |(i, e)| {
                Ok(e.chunks(N3)
                    .enumerate()
                    .map(|(j, buf)| SourceMessage {
                        payload: Some(buf.to_owned().into()),
                        offset: (i * N2 + (j + 1) * N3).to_string(),
                        split_id: split_id.clone(),
                        meta: crate::source::SourceMeta::Empty,
                    })
                    .collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(s).boxed();
        let msg_stream = NdByteStreamWrapper::<()>::split_stream(stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let items = msg_stream
            .into_iter()
            .flatten()
            .map(|e| String::from_utf8(e.payload.unwrap().into()).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(items.len(), N1);
        let text = items.join("");
        assert_eq!(text.len(), total_chars);
    }
}
