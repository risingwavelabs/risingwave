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

//! Spill files of batch executors.
//!
//! A [`SpillDir`] is a private directory of one executor. A [`SpillFile`] holds a sequence of
//! length-prefixed protobuf data chunks. It is written once through a [`SpillFileWriter`], can be
//! read any number of times, and is deleted when its last handle is dropped. The directory is
//! deleted when the [`SpillDir`] and all of its files are dropped.
//!
//! Writes are buffered and never fsynced: spill files are useless after a restart, and the spill
//! root is cleaned at startup anyway. The write and read buffers are charged to the memory context
//! given by the caller.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, anyhow};
use futures_async_stream::try_stream;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::memory::MemoryContext;
use risingwave_common::row::Row;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::Message;
use risingwave_pb::data::DataChunk as PbDataChunk;
use thiserror_ext::AsReport;

use super::spill_op::{SpillBackend, SpillOp};
use crate::error::{BatchError, Result};
use crate::executor::BoxedDataChunkStream;
use crate::monitor::BatchSpillMetrics;

/// Capacity of each writer's buffer. A chunk larger than this gets a buffer of its own size, which
/// is freed once the chunk is written.
pub const WRITE_BUFFER_SIZE: usize = 256 * 1024;
/// Buffer size of each reader.
pub const READ_BUFFER_SIZE: usize = 256 * 1024;

/// A private spill directory of one executor. Cloning shares the directory.
#[derive(Clone)]
pub struct SpillDir(Arc<DirInner>);

struct DirInner {
    store: Store,
    next_file_id: AtomicU64,
    metrics: Arc<BatchSpillMetrics>,
}

enum Store {
    Disk(PathBuf),
    /// Only for tests. File contents are not charged to any memory context.
    Memory(Mutex<HashMap<u64, Arc<Vec<u8>>>>),
}

impl SpillDir {
    /// Directories and files are created synchronously, so that a cancelled caller cannot leave one
    /// behind that nothing deletes.
    pub fn create(backend: SpillBackend, metrics: Arc<BatchSpillMetrics>) -> Result<Self> {
        let store = match backend {
            SpillBackend::Disk => {
                let path =
                    SpillOp::batch_spill_root().join(format!("{:032x}", rand::random::<u128>()));
                io(std::fs::create_dir_all(&path))?;
                Store::Disk(path)
            }
            SpillBackend::Memory => Store::Memory(Mutex::default()),
        };
        Ok(Self(Arc::new(DirInner {
            store,
            next_file_id: AtomicU64::new(0),
            metrics,
        })))
    }

    /// Creates a new file whose write buffer is charged to `mem_ctx`.
    pub fn create_file(&self, mem_ctx: &MemoryContext) -> Result<SpillFileWriter> {
        let id = self.0.next_file_id.fetch_add(1, Ordering::Relaxed);
        let sink = match &self.0.store {
            Store::Disk(dir) => Sink::Disk(Some(io(File::create(dir.join(id.to_string())))?)),
            Store::Memory(_) => Sink::Memory(Vec::new()),
        };
        Ok(SpillFileWriter {
            dir: self.clone(),
            id,
            sink,
            pending: Vec::new(),
            bytes: 0,
            chunks: 0,
            rows: 0,
            max_chunk_heap_size: 0,
            charge: Charge::new(mem_ctx.clone()),
            finished: false,
        })
    }

    fn remove_file(&self, id: u64) {
        match &self.0.store {
            Store::Disk(dir) => {
                let path = dir.join(id.to_string());
                if let Err(error) = std::fs::remove_file(&path)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(path = %path.display(), error = %error.as_report(), "failed to remove spill file");
                }
            }
            Store::Memory(files) => {
                files.lock().unwrap().remove(&id);
            }
        }
    }
}

impl Drop for DirInner {
    fn drop(&mut self) {
        if let Store::Disk(dir) = &self.store
            && let Err(error) = std::fs::remove_dir_all(dir)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %dir.display(), error = %error.as_report(), "failed to remove spill directory");
        }
    }
}

enum Sink {
    /// `None` only while a flush owns the file.
    Disk(Option<File>),
    Memory(Vec<u8>),
}

/// Appends data chunks to a new spill file. Dropping it without [`Self::finish`] deletes the file.
pub struct SpillFileWriter {
    dir: SpillDir,
    id: u64,
    sink: Sink,
    pending: Vec<u8>,
    bytes: u64,
    chunks: u64,
    rows: u64,
    max_chunk_heap_size: u64,
    charge: Charge,
    finished: bool,
}

impl SpillFileWriter {
    /// Appends the visible rows of `chunk`.
    pub async fn write_chunk(&mut self, chunk: &DataChunk) -> Result<()> {
        if chunk.cardinality() == 0 {
            return Ok(());
        }
        let compacted;
        let chunk = if chunk.is_vis_compacted() {
            chunk
        } else {
            compacted = chunk.clone().compact_vis();
            &compacted
        };
        let proto = chunk.to_protobuf();
        let heap_size = chunk.estimated_heap_size() as u64;
        let len = proto.encoded_len();
        let len_prefix =
            u32::try_from(len).map_err(|_| anyhow!("spilled chunk of {len} bytes is too large"))?;
        let record = len + 4;
        if self.pending.len() + record > WRITE_BUFFER_SIZE {
            self.flush().await?;
        }
        let needed = self.pending.len() + record;
        if needed > self.pending.capacity() {
            // Doubling as `Vec` does would leave the buffer at twice `WRITE_BUFFER_SIZE`.
            let capacity =
                (self.pending.capacity() * 2).clamp(needed, WRITE_BUFFER_SIZE.max(needed));
            self.pending.reserve_exact(capacity - self.pending.len());
            self.charge.set(self.pending.capacity());
        }
        self.pending.extend_from_slice(&len_prefix.to_le_bytes());
        proto
            .encode(&mut self.pending)
            .context("failed to encode spilled chunk")?;
        self.bytes += record as u64;
        self.chunks += 1;
        self.rows += chunk.cardinality() as u64;
        self.max_chunk_heap_size = self.max_chunk_heap_size.max(heap_size);
        self.dir
            .0
            .metrics
            .batch_spill_write_bytes
            .inc_by(record as u64);
        if self.pending.len() >= WRITE_BUFFER_SIZE {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        match &mut self.sink {
            Sink::Disk(file) => {
                let mut f = file.take().expect("spill file is open");
                let mut pending = std::mem::take(&mut self.pending);
                let (f, pending) = blocking(move || {
                    f.write_all(&pending)?;
                    pending.clear();
                    Ok((f, pending))
                })
                .await?;
                *file = Some(f);
                self.pending = pending;
            }
            Sink::Memory(data) => {
                data.extend_from_slice(&self.pending);
                self.pending.clear();
            }
        }
        if self.pending.capacity() > WRITE_BUFFER_SIZE {
            self.pending = Vec::new();
            self.charge.set(0);
        }
        Ok(())
    }

    /// Flushes the pending bytes and returns the finished file.
    pub async fn finish(mut self) -> Result<SpillFile> {
        self.flush().await?;
        self.pending = Vec::new();
        self.charge.set(0);
        match &mut self.sink {
            Sink::Disk(file) => drop(file.take()),
            Sink::Memory(data) => {
                let Store::Memory(files) = &self.dir.0.store else {
                    unreachable!("memory sink belongs to a memory store");
                };
                files
                    .lock()
                    .unwrap()
                    .insert(self.id, Arc::new(std::mem::take(data)));
            }
        }
        self.finished = true;
        Ok(SpillFile(Arc::new(FileInner {
            dir: self.dir.clone(),
            id: self.id,
            bytes: self.bytes,
            chunks: self.chunks,
            rows: self.rows,
            max_chunk_heap_size: self.max_chunk_heap_size,
        })))
    }
}

impl Drop for SpillFileWriter {
    fn drop(&mut self) {
        if !self.finished {
            if let Sink::Disk(file) = &mut self.sink {
                drop(file.take());
            }
            self.dir.remove_file(self.id);
        }
    }
}

/// A finished spill file. Clones share the file, which is deleted when the last clone and the last
/// reader are dropped.
#[derive(Clone)]
pub struct SpillFile(Arc<FileInner>);

struct FileInner {
    dir: SpillDir,
    id: u64,
    bytes: u64,
    chunks: u64,
    rows: u64,
    max_chunk_heap_size: u64,
}

impl Drop for FileInner {
    fn drop(&mut self) {
        self.dir.remove_file(self.id);
    }
}

impl SpillFile {
    pub fn bytes(&self) -> u64 {
        self.0.bytes
    }

    pub fn chunks(&self) -> u64 {
        self.0.chunks
    }

    pub fn rows(&self) -> u64 {
        self.0.rows
    }

    /// Estimated heap size of its largest chunk once it is read back.
    pub fn max_chunk_heap_size(&self) -> u64 {
        self.0.max_chunk_heap_size
    }

    /// Reads the chunks back in the order they were written. The read buffer is charged to
    /// `mem_ctx`.
    pub fn read(&self, mem_ctx: &MemoryContext) -> BoxedDataChunkStream {
        read_file(self.0.clone(), mem_ctx.clone())
    }
}

enum Source {
    /// `None` only while a read owns the reader.
    Disk(Option<BufReader<File>>),
    Memory {
        data: Arc<Vec<u8>>,
        offset: usize,
    },
}

impl Source {
    /// Reads the next message, of which at most `remaining` bytes are left in the file.
    async fn next_message(&mut self, remaining: u64) -> Result<Vec<u8>> {
        const CORRUPTED: &str = "spill file ends in the middle of a chunk";
        let corrupted = || anyhow!(CORRUPTED);
        match self {
            Source::Disk(reader) => {
                let mut r = reader.take().expect("spill file is open");
                let (r, message) = blocking(move || {
                    let mut len = [0u8; 4];
                    r.read_exact(&mut len)?;
                    let len = u32::from_le_bytes(len) as u64;
                    if len + 4 > remaining {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            CORRUPTED,
                        ));
                    }
                    let mut message = vec![0u8; len as usize];
                    r.read_exact(&mut message)?;
                    Ok((r, message))
                })
                .await?;
                *reader = Some(r);
                Ok(message)
            }
            Source::Memory { data, offset } => {
                let len_bytes = data.get(*offset..*offset + 4).ok_or_else(corrupted)?;
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                let message = data
                    .get(*offset + 4..*offset + 4 + len)
                    .ok_or_else(corrupted)?
                    .to_vec();
                *offset += 4 + len;
                Ok(message)
            }
        }
    }
}

#[try_stream(boxed, ok = DataChunk, error = BatchError)]
async fn read_file(file: Arc<FileInner>, mem_ctx: MemoryContext) {
    let mut charge = Charge::new(mem_ctx);
    let mut source = match &file.dir.0.store {
        Store::Disk(dir) => {
            let path = dir.join(file.id.to_string());
            let f = blocking(move || File::open(path)).await?;
            charge.set(READ_BUFFER_SIZE);
            Source::Disk(Some(BufReader::with_capacity(READ_BUFFER_SIZE, f)))
        }
        Store::Memory(files) => {
            let data = files
                .lock()
                .unwrap()
                .get(&file.id)
                .cloned()
                .ok_or_else(|| anyhow!("spill file {} not found", file.id))?;
            Source::Memory { data, offset: 0 }
        }
    };
    let mut remaining = file.bytes;
    for _ in 0..file.chunks {
        // The encoded copies are dropped before the chunk is yielded.
        let chunk = {
            let message = source.next_message(remaining).await?;
            remaining -= message.len() as u64 + 4;
            file.dir
                .0
                .metrics
                .batch_spill_read_bytes
                .inc_by(message.len() as u64 + 4);
            let proto = PbDataChunk::decode(message.as_slice())
                .context("failed to decode spilled chunk")?;
            DataChunk::from_protobuf(&proto)?
        };
        yield chunk;
    }
}

/// Runs blocking file I/O off the async workers.
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T> {
    io(tokio::task::spawn_blocking(f)
        .await
        .context("spill I/O task failed")?)
}

fn io<T>(result: std::io::Result<T>) -> Result<T> {
    Ok(result.context("spill I/O failed")?)
}

/// Memory charged to a context, released on drop.
struct Charge {
    ctx: MemoryContext,
    bytes: usize,
}

impl Charge {
    fn new(ctx: MemoryContext) -> Self {
        Self { ctx, bytes: 0 }
    }

    fn set(&mut self, bytes: usize) {
        self.ctx.add_unchecked(bytes as i64 - self.bytes as i64);
        self.bytes = bytes;
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.set(0);
    }
}

/// Builds data chunks row by row. A chunk ends at `max_rows` rows or once its arrays hold
/// `max_bytes`, so it takes at most `max_bytes` plus its last row, however wide the rows are.
///
/// The arrays start empty, so that what they hold follows the rows appended. That includes the slots
/// of NULLs, which take memory but no space in a spill file.
pub struct BoundedChunkBuilder {
    data_types: Vec<DataType>,
    max_rows: usize,
    max_bytes: usize,
    array_builders: Vec<ArrayBuilderImpl>,
    rows: usize,
}

impl BoundedChunkBuilder {
    pub fn new(data_types: Vec<DataType>, max_rows: usize, max_bytes: usize) -> Self {
        let array_builders = Self::empty_array_builders(&data_types);
        Self {
            data_types,
            max_rows,
            max_bytes,
            array_builders,
            rows: 0,
        }
    }

    fn empty_array_builders(data_types: &[DataType]) -> Vec<ArrayBuilderImpl> {
        data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(0))
            .collect()
    }

    /// Appends `row`, and returns the chunk if the row ends it.
    #[must_use]
    pub fn append_row(&mut self, row: impl Row) -> Option<DataChunk> {
        for (builder, datum) in self.array_builders.iter_mut().zip_eq_debug(row.iter()) {
            builder.append(datum);
        }
        self.rows += 1;
        let bytes = self
            .array_builders
            .iter()
            .map(|builder| builder.estimated_heap_size())
            .sum::<usize>();
        (self.rows >= self.max_rows || bytes >= self.max_bytes).then(|| self.take_chunk())
    }

    /// Returns the rows of the unfinished chunk, if there are any.
    #[must_use]
    pub fn consume_all(&mut self) -> Option<DataChunk> {
        (self.rows > 0).then(|| self.take_chunk())
    }

    fn take_chunk(&mut self) -> DataChunk {
        let empty = Self::empty_array_builders(&self.data_types);
        let columns = std::mem::replace(&mut self.array_builders, empty)
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();
        DataChunk::new(columns, std::mem::take(&mut self.rows))
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use prometheus::core::Atomic;
    use risingwave_common::array::{Array, DataChunk, DataChunkTestExt, I64Array, Utf8Array};
    use risingwave_common::metrics::TrAdderAtomic;

    use super::*;

    async fn round_trip(backend: SpillBackend) {
        let dir = SpillDir::create(backend, BatchSpillMetrics::for_test()).unwrap();
        let ctx = MemoryContext::root(TrAdderAtomic::new(0), u64::MAX);
        let chunks = [
            DataChunk::from_pretty(
                "i T
                 1 a
                 . b
                 3 .",
            ),
            // Invisible rows are not written.
            DataChunk::from_pretty(
                "i T
                 4 d D
                 5 e",
            ),
            DataChunk::from_pretty("i T"),
        ];
        let mut writer = dir.create_file(&ctx).unwrap();
        for chunk in &chunks {
            writer.write_chunk(chunk).await.unwrap();
        }
        let file = writer.finish().await.unwrap();
        assert_eq!((file.chunks(), file.rows()), (2, 4));
        // Only the reader's buffer is charged once the writer is done.
        assert_eq!(ctx.get_bytes_used(), 0);

        for _ in 0..2 {
            let read: Vec<DataChunk> = file.read(&ctx).try_collect().await.unwrap();
            assert_eq!(read.len(), 2);
            assert_eq!(read[0], chunks[0]);
            assert_eq!(
                read[1],
                DataChunk::from_pretty(
                    "i T
                     5 e"
                )
            );
        }
        assert_eq!(ctx.get_bytes_used(), 0);

        if let Store::Disk(path) = &dir.0.store {
            let path = path.clone();
            assert!(path.join("0").exists());
            drop(file);
            assert!(!path.join("0").exists());
            drop(dir);
            assert!(!path.exists());
        }
    }

    #[tokio::test]
    async fn test_round_trip_memory() {
        round_trip(SpillBackend::Memory).await;
    }

    #[tokio::test]
    async fn test_round_trip_disk() {
        round_trip(SpillBackend::Disk).await;
    }

    #[tokio::test]
    async fn test_write_buffer_is_bounded() {
        let dir = SpillDir::create(SpillBackend::Memory, BatchSpillMetrics::for_test()).unwrap();
        let ctx = MemoryContext::root(TrAdderAtomic::new(0), u64::MAX);
        let chunk = |rows: usize, width: usize| {
            let value = "x".repeat(width);
            DataChunk::new(
                vec![Utf8Array::from_iter((0..rows).map(|_| Some(value.as_str()))).into_ref()],
                rows,
            )
        };
        let mut writer = dir.create_file(&ctx).unwrap();
        for (rows, width) in [(10, 1000); 200]
            .into_iter()
            .chain([(1, 2 * WRITE_BUFFER_SIZE), (10, 1000)])
        {
            writer.write_chunk(&chunk(rows, width)).await.unwrap();
            assert!(ctx.get_bytes_used() <= WRITE_BUFFER_SIZE as i64);
        }
        let file = writer.finish().await.unwrap();
        assert_eq!((file.chunks(), file.rows()), (202, 2011));
        assert_eq!(ctx.get_bytes_used(), 0);
    }

    #[test]
    fn test_bounded_chunk_builder() {
        fn chunk_rows(types: Vec<DataType>, input: DataChunk, max_rows: usize) -> Vec<usize> {
            let mut builder = BoundedChunkBuilder::new(types, max_rows, 16 << 10);
            let mut chunks = vec![];
            for row in input.rows() {
                chunks.extend(builder.append_row(row));
            }
            chunks.extend(builder.consume_all());
            let widest_row = input.estimated_heap_size() / input.cardinality();
            for chunk in &chunks {
                assert!(chunk.estimated_heap_size() <= (16 << 10) + widest_row);
            }
            chunks.iter().map(|chunk| chunk.cardinality()).collect()
        }
        let text = |rows: usize, width: usize| {
            let value = "x".repeat(width);
            DataChunk::new(
                vec![Utf8Array::from_iter((0..rows).map(|_| Some(value.as_str()))).into_ref()],
                rows,
            )
        };

        // Narrow rows end at `max_rows`, with the rest in the last chunk.
        assert_eq!(
            chunk_rows(vec![DataType::Varchar], text(5, 1), 2),
            [2, 2, 1]
        );
        // Wide rows end by their bytes, and a row larger than `max_bytes` makes a chunk on its own.
        assert!(chunk_rows(vec![DataType::Varchar], text(100, 1 << 10), 1024).len() > 1);
        assert_eq!(
            chunk_rows(vec![DataType::Varchar], text(2, 32 << 10), 1024),
            [1, 1]
        );
        // The slots of NULLs count, though they take no space in a spill file.
        let nulls = DataChunk::new(
            (0..100)
                .map(|_| I64Array::from_iter((0..1024).map(|_| None::<i64>)).into_ref())
                .collect(),
            1024,
        );
        let rows = chunk_rows(vec![DataType::Int64; 100], nulls, 1024);
        assert!(rows.len() > 1 && rows[0] > 1, "{rows:?}");
    }

    #[tokio::test]
    async fn test_unfinished_writer_removes_its_file() {
        let dir = SpillDir::create(SpillBackend::Disk, BatchSpillMetrics::for_test()).unwrap();
        let Store::Disk(path) = &dir.0.store else {
            unreachable!()
        };
        let path = path.clone();
        let ctx = MemoryContext::none();
        let mut writer = dir.create_file(&ctx).unwrap();
        writer
            .write_chunk(&DataChunk::from_pretty(
                "i
                 1",
            ))
            .await
            .unwrap();
        assert!(path.join("0").exists());
        drop(writer);
        assert!(!path.join("0").exists());
    }
}
