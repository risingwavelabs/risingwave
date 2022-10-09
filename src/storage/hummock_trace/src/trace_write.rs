use std::fs::File;
use std::io::{Result, Write};
use std::sync::Arc;

use bincode::{config, encode_into_slice};
use parking_lot::Mutex;

use super::trace_record::{Operation, Record};

static MAGIC_BYTES: u32 = 0xA8596D4F;

pub(crate) trait TraceWriter {
    fn write(&mut self, record: Record) -> Result<()>;
    fn write_all(&mut self, records: Vec<Record>) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub(crate) struct TraceFileWriter {
    file: File,
    buf: Vec<u8>,
}

impl TraceFileWriter {
    pub(crate) fn new(file_name: String) -> Result<Self> {
        let mut file = File::create(file_name)?;
        let buf = Vec::new();
        file.write(&MAGIC_BYTES.to_le_bytes())?;
        Ok(Self { file, buf })
    }
}

impl TraceWriter for TraceFileWriter {
    fn write(&mut self, record: Record) -> Result<()> {
        let size = encode_into_slice(record, &mut self.buf, config::standard()).unwrap();
        self.file.write(&size.to_le_bytes())?; // write as little-endian
        self.file.write_all(&self.buf[..size])
    }

    fn write_all(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.write(record)?;
        }
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.file.sync_all()
    }
}

// In-memory writer that is generally used for tests
pub(crate) struct TraceMemWriter {
    mem: Arc<Mutex<Vec<Record>>>,
}

impl TraceMemWriter {
    pub(crate) fn new(mem: Arc<Mutex<Vec<Record>>>) -> Self {
        Self { mem }
    }
}

impl TraceWriter for TraceMemWriter {
    fn write(&mut self, record: Record) -> Result<()> {
        self.mem.lock().push(record);
        Ok(())
    }

    fn write_all(&mut self, records: Vec<Record>) -> Result<()> {
        self.mem.lock().extend(records);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(crate) trait TraceReader {
    fn read(&self) -> Result<Operation>;
}

pub(crate) struct TraceFileReader {}

impl TraceReader for TraceFileReader {
    fn read(&self) -> Result<Operation> {
        todo!()
    }
}
