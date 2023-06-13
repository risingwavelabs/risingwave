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

use std::io::Write;
use std::mem::size_of;

use bincode::{config, encode_into_std_write};
#[cfg(test)]
use mockall::{automock, mock};

use super::record::Record;
use crate::error::Result;
use crate::TraceError;

pub(crate) type MagicBytes = u32;
pub(crate) static MAGIC_BYTES: MagicBytes = 0x484D5452; // HMTR

#[cfg_attr(test, automock)]
pub(crate) trait TraceWriter {
    fn write(&mut self, record: Record) -> Result<usize>;
    fn flush(&mut self) -> Result<()>;
    fn write_all(&mut self, records: Vec<Record>) -> Result<usize> {
        let mut total_size = 0;
        for r in records {
            total_size += self.write(r)?
        }
        Ok(total_size)
    }
}

/// Serializer serializes a record to std write.
#[cfg_attr(test, automock)]
pub(crate) trait Serializer<W: Write> {
    fn serialize(&self, record: Record, buf: &mut W) -> Result<usize>;
}

pub(crate) struct BincodeSerializer;

impl BincodeSerializer {
    fn new() -> Self {
        Self
    }
}

impl<W: Write> Serializer<W> for BincodeSerializer {
    fn serialize(&self, record: Record, writer: &mut W) -> Result<usize> {
        let size = encode_into_std_write(record, writer, config::standard())?;
        Ok(size)
    }
}

pub(crate) struct TraceWriterImpl<W: Write, S: Serializer<W>> {
    writer: W,
    serializer: S,
}

impl<W: Write, S: Serializer<W>> TraceWriterImpl<W, S> {
    pub(crate) fn try_new(writer: W, serializer: S) -> Result<Self> {
        let mut writer = Self { writer, serializer };
        writer.write_magic_bytes()?;
        Ok(writer)
    }

    fn write_magic_bytes(&mut self) -> Result<()> {
        let size = self.writer.write(&MAGIC_BYTES.to_be_bytes())?;
        if size != size_of::<MagicBytes>() {
            Err(TraceError::Other("failed to write magic bytes"))
        } else {
            Ok(())
        }
    }
}

impl<W: Write> TraceWriterImpl<W, BincodeSerializer> {
    pub(crate) fn try_new_bincode(writer: W) -> Result<Self> {
        Self::try_new(writer, BincodeSerializer::new())
    }
}

impl<W: Write, S: Serializer<W>> TraceWriter for TraceWriterImpl<W, S> {
    fn write(&mut self, record: Record) -> Result<usize> {
        let size = self.serializer.serialize(record, &mut self.writer)?;
        Ok(size)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

impl<W: Write, S: Serializer<W>> Drop for TraceWriterImpl<W, S> {
    fn drop(&mut self) {
        self.flush().expect("failed to flush TraceWriterImpl");
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use bincode::{config, decode_from_slice, encode_to_vec};
    use byteorder::{BigEndian, ReadBytesExt};
    use bytes::Bytes;

    use super::*;
    use crate::{Operation, TracedReadOptions};

    mock! {
        Write{}
        impl Write for Write{
            fn write(&mut self, bytes: &[u8]) -> std::result::Result<usize, std::io::Error>;
            fn flush(&mut self) -> std::result::Result<(), std::io::Error>;
        }
    }

    #[test]
    fn test_bincode_serialize() {
        let op = Operation::get(
            Bytes::from(vec![0, 1, 2, 3]),
            Some(123),
            TracedReadOptions::for_test(123),
        );
        let expected = Record::new_local_none(0, op);
        let serializer = BincodeSerializer::new();
        let mut buf = Vec::new();
        let write_size = serializer.serialize(expected.clone(), &mut buf).unwrap();
        assert_eq!(write_size, buf.len());

        let (actual, read_size) = decode_from_slice(&buf, config::standard()).unwrap();

        assert_eq!(write_size, read_size);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_new_writer_impl() {
        // Create a Cursor that can be used as a mock writer.
        let mut buf = Cursor::new(Vec::new());

        {
            // Create a TraceWriterImpl instance using the mock writer and a BincodeSerializer.
            let mut writer = TraceWriterImpl::try_new_bincode(&mut buf).unwrap();

            writer.flush().unwrap();
        }
        buf.set_position(0);
        let magic_bytes = buf.read_u32::<BigEndian>().unwrap();
        assert_eq!(magic_bytes, MAGIC_BYTES);
    }

    #[test]
    fn test_writer_impl_write() {
        let mut mock_writer = MockWrite::new();
        let key = Bytes::from(vec![123]);
        let value = Bytes::from(vec![234]);
        let op = Operation::insert(key, value, None);
        let record = Record::new_local_none(0, op);
        let r_bytes = encode_to_vec(record.clone(), config::standard()).unwrap();
        let r_len = r_bytes.len();

        mock_writer
            .expect_write()
            .times(1)
            .returning(|_| Ok(size_of::<u32>()));
        mock_writer.expect_write().returning(|b| Ok(b.len()));

        mock_writer.expect_flush().times(1).returning(|| Ok(()));

        let mut mock_serializer = MockSerializer::new();

        mock_serializer
            .expect_serialize()
            .times(1)
            .returning(move |_, _| Ok(r_len));

        let mut writer = TraceWriterImpl::try_new(mock_writer, mock_serializer).unwrap();

        writer.write(record).unwrap();
    }
}
