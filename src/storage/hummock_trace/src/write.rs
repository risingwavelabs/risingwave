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

use std::io::Write;
#[cfg(test)]
use std::io::{Read, Result as IOResult};
use std::mem::size_of;
use std::sync::Arc;

use bincode::{config, encode_into_std_write, encode_to_vec, Encode};
#[cfg(test)]
use mockall::{automock, mock};
use parking_lot::Mutex;

use super::record::Record;
use crate::error::Result;

pub(crate) static MAGIC_BYTES: u32 = 0x484D5452; // HMTR

#[cfg_attr(test, automock)]
pub(crate) trait TraceWriter {
    fn write(&mut self, record: Record) -> Result<usize>;
    fn sync(&mut self) -> Result<()>;
    fn write_all(&mut self, records: Vec<Record>) -> Result<usize> {
        let mut total_size = 0;
        for r in records {
            total_size += self.write(r)?
        }
        Ok(total_size)
    }
}
#[cfg_attr(test, automock)]
pub(crate) trait Serializer {
    fn serialize(&mut self, record: Record) -> Result<Vec<u8>>;
}

pub(crate) struct BincodeSerializer;

impl BincodeSerializer {
    fn new() -> Self {
        Self {}
    }
}

impl Serializer for BincodeSerializer {
    fn serialize(&mut self, record: Record) -> Result<Vec<u8>> {
        let bytes = encode_to_vec(record, config::standard())?;
        Ok(bytes)
    }
}

pub(crate) struct TraceWriterImpl<W: Write, S: Serializer> {
    writer: W,
    serializer: S,
}

impl<W: Write, S: Serializer> TraceWriterImpl<W, S> {
    pub(crate) fn new(mut writer: W, mut serializer: S) -> Result<Self> {
        assert_eq!(writer.write(&MAGIC_BYTES.to_le_bytes())?, size_of::<u32>());
        Ok(Self { writer, serializer })
    }
}

impl<W: Write> TraceWriterImpl<W, BincodeSerializer> {
    pub(crate) fn new_bincode(mut writer: W) -> Result<Self> {
        let s = BincodeSerializer::new();
        Self::new(writer, s)
    }
}

impl<W: Write, S: Serializer> TraceWriter for TraceWriterImpl<W, S> {
    fn write(&mut self, record: Record) -> Result<usize> {
        let bytes = self.serializer.serialize(record)?;
        let size = self.writer.write(&bytes)?;
        Ok(size)
    }

    fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) struct MemTraceStore {
    buf: Vec<u8>,
    read_index: usize,
}

#[cfg(test)]
impl MemTraceStore {
    pub(crate) fn new() -> Self {
        Self {
            buf: Vec::new(),
            read_index: 0,
        }
    }
}

#[cfg(test)]
impl Write for MemTraceStore {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        for b in buf {
            self.buf.push(*b);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> IOResult<()> {
        Ok(())
    }
}

#[cfg(test)]
impl Read for MemTraceStore {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let start_index = self.read_index;

        for i in 0..buf.len() {
            if self.read_index >= self.buf.len() {
                break;
            }
            buf[i] = self.buf[self.read_index];
            self.read_index += 1;
        }

        Ok(self.read_index - start_index)
    }
}
// In-memory writer that is generally used for tests
#[cfg(test)]
pub(crate) struct TraceMemWriter {
    mem: Arc<Mutex<Vec<Record>>>,
}

#[cfg(test)]
impl TraceMemWriter {
    pub(crate) fn new(mem: Arc<Mutex<Vec<Record>>>) -> Self {
        Self { mem }
    }
}

#[cfg(test)]
impl TraceWriter for TraceMemWriter {
    fn write(&mut self, record: Record) -> Result<usize> {
        self.mem.lock().push(record);
        Ok(0)
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mock! {
    Write{}
    impl Write for Write{
        fn write(&mut self, bytes: &[u8]) -> std::result::Result<usize, std::io::Error>;
        fn flush(&mut self) -> std::result::Result<(), std::io::Error>;
    }
}

#[cfg(test)]
mod test {
    use bincode::{config, encode_to_vec};
    use mockall::predicate;

    use super::*;
    #[test]
    fn test_writer_impl_write() {
        let mut mock_write = MockWrite::new();
        let op = crate::Operation::Ingest(vec![(vec![0], vec![0])], 0, 0);
        let record = Record::new(0, op);
        let r_bytes = encode_to_vec(record.clone(), config::standard()).unwrap();
        let r_len = r_bytes.len();

        let mut mock_serializer = MockSerializer::new();

        mock_serializer
            .expect_serialize()
            .times(1)
            .returning(move |_| Ok(r_bytes.clone()));
        mock_write.expect_write().times(1).returning(|_| Ok(4));
        mock_write
            .expect_write()
            .times(1)
            .returning(move |_| Ok(r_len));
        mock_write.expect_flush().times(1).returning(|| Ok(()));

        let mut writer = TraceWriterImpl::new(mock_write, mock_serializer).unwrap();
        writer.write(record).unwrap();
        writer.sync().unwrap();
    }
}
