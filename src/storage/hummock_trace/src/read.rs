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

use bincode::{config, decode_from_std_read};
use byteorder::{LittleEndian, ReadBytesExt};
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

pub struct TraceReaderImpl<R: ReadBytesExt> {
    reader: R,
}

impl<R: ReadBytesExt> TraceReaderImpl<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let flag = reader.read_u32::<LittleEndian>()?;
        if flag != MAGIC_BYTES {
            Err(TraceError::MagicBytesError {
                expected: MAGIC_BYTES,
                found: flag,
            })
        } else {
            Ok(Self { reader })
        }
    }
}

impl<R: ReadBytesExt> TraceReader for TraceReaderImpl<R> {
    fn read(&mut self) -> Result<Record> {
        let op = decode_from_std_read(&mut self.reader, config::standard())?;
        Ok(op)
    }
}
#[cfg(test)]
mod test {
    use std::io::Write;

    use bincode::config::{self};
    use bincode::encode_to_vec;
    use byteorder::{LittleEndian, WriteBytesExt};

    use super::{TraceReader, TraceReaderImpl};
    use crate::{MemTraceStore, Operation, Record, MAGIC_BYTES};

    #[test]
    fn read_ops() {
        let count = 5000;
        let mut records = Vec::new();
        let mut store = MemTraceStore::new();
        store.write_u32::<LittleEndian>(MAGIC_BYTES).unwrap();
        for i in 0..count {
            let key = format!("key{}", i).as_bytes().to_vec();
            let value = format!("value{}", i).as_bytes().to_vec();
            let op = Operation::Ingest(vec![(key, value)], 0, 0);
            let record = Record::new(0, op);
            let buf = encode_to_vec(record.clone(), config::standard()).unwrap();
            store.write(&buf).unwrap();
            records.push(record);
        }
        let mut reader = TraceReaderImpl::new(store).unwrap();
        for i in 0..count {
            let record = reader.read().unwrap();
            assert_eq!(record, records[i]);
        }
        // throw err if reader is empty
        assert_eq!(reader.read().is_err(), true);
    }
}
