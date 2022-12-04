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

use std::io::Read;

use bincode::{config, decode_from_std_read};
use byteorder::{BigEndian, ReadBytesExt};
#[cfg(test)]
use mockall::automock;

use crate::error::{Result, TraceError};
use crate::{Record, MAGIC_BYTES};
#[cfg_attr(test, automock)]
pub trait TraceReader {
    fn read(&mut self) -> Result<Record>;
    fn read_n(&mut self, n: usize) -> Result<Vec<Record>> {
        let mut ops = Vec::with_capacity(n);
        for _ in 0..n {
            let op = self.read()?;
            ops.push(op);
        }
        Ok(ops)
    }
}
/// Deserializer decodes a record from memory
#[cfg_attr(test, automock)]
pub trait Deserializer<R: Read> {
    /// consumes the reader and deserialize a record
    fn deserialize(&self, reader: &mut R) -> Result<Record>;
}

/// Decodes bincode format serialized data
pub struct BincodeDeserializer;

impl BincodeDeserializer {
    fn new() -> Self {
        Self
    }
}

impl<R: Read> Deserializer<R> for BincodeDeserializer {
    fn deserialize(&self, reader: &mut R) -> Result<Record> {
        let record = decode_from_std_read(reader, config::standard())?;
        Ok(record)
    }
}

pub struct TraceReaderImpl<R: Read, D: Deserializer<R>> {
    reader: R,
    deserializer: D,
}

impl<R: Read, D: Deserializer<R>> TraceReaderImpl<R, D> {
    pub fn new(mut reader: R, deserializer: D) -> Result<Self> {
        // Read the 32-bit unsigned integer from the reader using the BigEndian byte order.
        let magic_bytes = reader.read_u32::<BigEndian>()?;

        // Check if the magic bytes match the expected value.
        if magic_bytes != MAGIC_BYTES {
            Err(TraceError::MagicBytes {
                expected: MAGIC_BYTES,
                found: magic_bytes,
            })
        } else {
            // Return the TraceReaderImpl instance containing the reader and deserializer.
            Ok(Self {
                reader,
                deserializer,
            })
        }
    }
}

impl<R: Read> TraceReaderImpl<R, BincodeDeserializer> {
    pub fn new_bincode(reader: R) -> Result<Self> {
        let deserializer = BincodeDeserializer::new();
        Self::new(reader, deserializer)
    }
}

impl<R: Read, D: Deserializer<R>> TraceReader for TraceReaderImpl<R, D> {
    fn read(&mut self) -> Result<Record> {
        self.deserializer.deserialize(&mut self.reader)
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Result, Write};
    use std::mem::size_of;

    use bincode::config::{self};
    use bincode::encode_to_vec;
    use mockall::mock;
    use risingwave_pb::common::Status;
    use risingwave_pb::meta::SubscribeResponse;

    use super::{TraceReader, TraceReaderImpl};
    use crate::{
        traced_bytes, BincodeDeserializer, Deserializer, MockDeserializer, Operation, Record,
        TraceSubResp, TracedBytes, MAGIC_BYTES,
    };

    mock! {
        Reader{}
        impl Read for Reader{
            fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
        }
    }

    #[derive(Default)]
    pub(crate) struct MemTraceStore(Vec<u8>);

    impl Write for MemTraceStore {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            let size = self.0.write(buf)?;
            Ok(size)
        }

        fn flush(&mut self) -> Result<()> {
            self.0.flush()?;
            Ok(())
        }
    }

    impl Read for MemTraceStore {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            if self.0.is_empty() {
                return Ok(0);
            }
            let end = std::cmp::min(buf.len(), self.0.len());
            let v = self.0.drain(0..end);
            buf.copy_from_slice(v.as_slice());
            Ok(v.len())
        }
    }

    #[test]
    fn test_bincode_deserialize() {
        let deserializer = BincodeDeserializer::new();
        let op = Operation::get(
            traced_bytes![5, 5, 15, 6],
            7564,
            None,
            true,
            Some(5433),
            123,
            false,
        );
        let expected = Record::new_local_none(54433, op);

        let mut buf = MemTraceStore::default();

        let record_bytes = encode_to_vec(expected.clone(), config::standard()).unwrap();
        let _ = buf.write(&record_bytes).unwrap();

        let actual = deserializer.deserialize(&mut buf).unwrap();

        assert_eq!(expected, actual);
    }
    #[test]
    fn test_bincode_serialize_resp() {
        let deserializer = BincodeDeserializer::new();
        let resp = TraceSubResp(SubscribeResponse {
            status: Some(Status {
                code: 0,
                message: "abc".to_string(),
            }),
            info: None,
            operation: 1,
            version: 100,
        });
        let op = Operation::MetaMessage(Box::new(resp));
        let expected = Record::new_local_none(123, op);

        let mut buf = MemTraceStore::default();

        let record_bytes = encode_to_vec(expected.clone(), config::standard()).unwrap();
        let _ = buf.write(&record_bytes).unwrap();

        let actual = deserializer.deserialize(&mut buf).unwrap();

        assert_eq!(expected, actual);
    }
    #[test]
    fn test_bincode_deserialize_many() {
        let count = 5000;
        let mut buf = MemTraceStore::default();
        let mut records = Vec::new();

        for i in 0..count {
            let key = TracedBytes::from(format!("key{}", i).as_bytes().to_vec());
            let value = TracedBytes::from(format!("value{}", i).as_bytes().to_vec());
            let op = Operation::ingest(vec![(key, Some(value))], vec![], 0, 0);
            let record = Record::new_local_none(i, op);
            records.push(record.clone());
            let record_bytes = encode_to_vec(record.clone(), config::standard()).unwrap();
            let _ = buf.write(&record_bytes).unwrap();
        }

        let deserializer = BincodeDeserializer::new();

        for expected in records {
            let actual = deserializer.deserialize(&mut buf).unwrap();
            assert_eq!(expected, actual);
        }

        assert!(deserializer.deserialize(&mut buf).is_err());
        assert_eq!(buf.0.len(), 0);
    }

    #[test]
    fn test_read_records() {
        let count = 5000;
        let mut mock_reader = MockReader::new();
        let mut mock_deserializer = MockDeserializer::new();

        mock_reader.expect_read().times(1).returning(|b| {
            b.clone_from_slice(&MAGIC_BYTES.to_be_bytes());
            Ok(size_of::<u32>())
        });

        mock_reader.expect_read().returning(|b| Ok(b.len()));

        let expected = Record::new_local_none(0, Operation::Finish);
        let return_expected = expected.clone();

        mock_deserializer
            .expect_deserialize()
            .times(count)
            .returning(move |_| Ok(return_expected.clone()));

        let mut trace_reader = TraceReaderImpl::new(mock_reader, mock_deserializer).unwrap();

        for _ in 0..count {
            let actual = trace_reader.read().unwrap();
            assert_eq!(expected, actual);
        }
    }
}
