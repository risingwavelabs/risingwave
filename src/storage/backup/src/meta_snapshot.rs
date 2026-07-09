// Copyright 2022 RisingWave Labs
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

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::Hasher;
use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectDataStreamReader, ObjectStreamingUploader,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use twox_hash::XxHash64;

use crate::MetaSnapshotId;
use crate::error::{BackupError, BackupResult};

pub trait Metadata: Display + Send + Sync {
    fn encode_to_writer<'a>(
        &'a self,
        writer: &'a mut SnapshotPayloadWriter,
    ) -> impl Future<Output = BackupResult<()>> + Send + 'a;

    fn decode_from_reader<R>(
        reader: SnapshotPayloadReader<R>,
    ) -> impl Future<Output = BackupResult<Self>> + Send
    where
        Self: Sized,
        R: AsyncRead + Send;

    fn hummock_version_ref(&self) -> &HummockVersion;

    fn hummock_version(self) -> HummockVersion;

    fn storage_url(&self) -> BackupResult<String>;

    fn storage_directory(&self) -> BackupResult<String>;

    fn table_change_log_object_ids(&self) -> HashSet<HummockRawObjectId>;
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct MetaSnapshot<T: Metadata> {
    pub format_version: u32,
    pub id: MetaSnapshotId,
    /// Snapshot of meta store.
    pub metadata: T,
}

impl<T: Metadata> MetaSnapshot<T> {
    pub async fn encode_to_uploader(&self, uploader: ObjectStreamingUploader) -> BackupResult<()> {
        let mut writer = SnapshotPayloadWriter::new(uploader);

        let mut header = BytesMut::with_capacity(size_of::<u32>() + size_of::<u64>());
        header.put_u32_le(self.format_version);
        header.put_u64_le(self.id);
        writer.write_snapshot_bytes(header.freeze()).await?;

        self.metadata.encode_to_writer(&mut writer).await?;
        writer.finish().await
    }

    pub async fn decode_from_stream(reader: MonitoredStreamingReader) -> BackupResult<Self> {
        let mut reader =
            SnapshotPayloadReader::new(ObjectDataStreamReader::new(reader.into_stream()));
        let (format_version, id) = reader.read_snapshot_header().await?;
        let metadata = T::decode_from_reader(reader).await?;
        Ok(Self {
            format_version,
            id,
            metadata,
        })
    }
}

pub struct SnapshotPayloadWriter {
    uploader: ObjectStreamingUploader,
    hasher: XxHash64,
    size: usize,
}

impl SnapshotPayloadWriter {
    pub fn new(uploader: ObjectStreamingUploader) -> Self {
        Self {
            uploader,
            hasher: XxHash64::with_seed(0),
            size: 0,
        }
    }

    pub async fn write_snapshot_bytes(&mut self, data: Bytes) -> BackupResult<()> {
        self.hasher.write(data.as_ref());
        self.size += data.len();
        self.uploader.write_bytes(data).await?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub async fn finish(mut self) -> BackupResult<()> {
        let mut checksum = BytesMut::with_capacity(size_of::<u64>());
        checksum.put_u64_le(self.hasher.finish());
        let checksum = checksum.freeze();
        self.size += checksum.len();
        self.uploader.write_bytes(checksum).await?;
        self.uploader.finish().await?;
        Ok(())
    }
}

pub struct SnapshotPayloadReader<R> {
    reader: std::pin::Pin<Box<R>>,
    hasher: XxHash64,
}

impl<R: AsyncRead> SnapshotPayloadReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Box::pin(reader),
            hasher: XxHash64::with_seed(0),
        }
    }

    pub async fn read_snapshot_header(&mut self) -> BackupResult<(u32, MetaSnapshotId)> {
        let format_version = self.read_u32_le().await?;
        let id = self.read_u64_le().await?;
        Ok((format_version, id))
    }

    pub async fn read_u32_le(&mut self) -> BackupResult<u32> {
        let bytes = self.read_exact(size_of::<u32>()).await?;
        Ok(u32::from_le_bytes(
            bytes[..].try_into().expect("u32 length"),
        ))
    }

    pub async fn read_u64_le(&mut self) -> BackupResult<u64> {
        let bytes = self.read_exact(size_of::<u64>()).await?;
        Ok(u64::from_le_bytes(
            bytes[..].try_into().expect("u64 length"),
        ))
    }

    pub async fn read_exact(&mut self, len: usize) -> BackupResult<BytesMut> {
        let mut bytes = BytesMut::with_capacity(len);
        bytes.resize(len, 0);
        self.reader.as_mut().read_exact(&mut bytes).await?;
        self.hasher.write(&bytes);
        Ok(bytes)
    }

    pub async fn skip_exact(&mut self, mut len: usize) -> BackupResult<()> {
        const BUFFER_SIZE: usize = 8 * 1024;
        let mut buf = [0; BUFFER_SIZE];
        while len > 0 {
            let read_len = len.min(BUFFER_SIZE);
            self.reader
                .as_mut()
                .read_exact(&mut buf[..read_len])
                .await?;
            self.hasher.write(&buf[..read_len]);
            len -= read_len;
        }
        Ok(())
    }

    pub async fn read_to_end(mut self) -> BackupResult<Vec<u8>> {
        let mut data = vec![];
        self.reader.as_mut().read_to_end(&mut data).await?;
        if data.len() < size_of::<u64>() {
            return Err(BackupError::Decoding(
                anyhow::anyhow!("meta snapshot is missing checksum").into(),
            ));
        }

        let checksum = data.split_off(data.len() - size_of::<u64>());
        self.hasher.write(&data);
        self.verify_checksum(&checksum)?;
        Ok(data)
    }

    pub async fn finish_after_skipping_to_end(mut self) -> BackupResult<()> {
        const BUFFER_SIZE: usize = 8 * 1024;
        let mut tail = Vec::with_capacity(size_of::<u64>());
        let mut buf = [0; BUFFER_SIZE];

        loop {
            let n = self.reader.as_mut().read(&mut buf).await?;
            if n == 0 {
                break;
            }

            tail.extend_from_slice(&buf[..n]);
            if tail.len() > size_of::<u64>() {
                let payload_len = tail.len() - size_of::<u64>();
                self.hasher.write(&tail[..payload_len]);
                tail.drain(..payload_len);
            }
        }

        if tail.len() != size_of::<u64>() {
            return Err(BackupError::Decoding(
                anyhow::anyhow!("meta snapshot is missing checksum").into(),
            ));
        }
        self.verify_checksum(&tail)
    }

    pub async fn finish(mut self) -> BackupResult<()> {
        let mut checksum = [0; size_of::<u64>()];
        self.reader.as_mut().read_exact(&mut checksum).await?;
        self.verify_checksum(&checksum)?;

        let mut trailing = [0; 1];
        let n = self.reader.as_mut().read(&mut trailing).await?;
        if n != 0 {
            return Err(BackupError::Decoding(
                anyhow::anyhow!("unexpected bytes after meta snapshot checksum").into(),
            ));
        }
        Ok(())
    }

    fn verify_checksum(&self, checksum: &[u8]) -> BackupResult<()> {
        let expected = u64::from_le_bytes(checksum.try_into().expect("u64 length"));
        let found = self.hasher.finish();
        if expected != found {
            return Err(BackupError::ChecksumMismatch { expected, found });
        }
        Ok(())
    }
}

impl<T: Metadata> Display for MetaSnapshot<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "format_version: {}", self.format_version)?;
        writeln!(f, "id: {}", self.id)?;
        writeln!(f, "{}", self.metadata)?;
        Ok(())
    }
}
