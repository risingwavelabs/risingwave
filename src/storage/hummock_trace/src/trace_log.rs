use std::fs::File;
use std::io::{Result, Write};
use std::sync::Arc;

use parking_lot::Mutex;

use super::trace_record::{Operation, Record, TraceRecord};

pub(crate) trait TraceWriter {
    fn write(&mut self, record: Record) -> Result<()>;
    fn write_all(&mut self, records: Vec<Record>) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
}

pub(crate) struct TraceFileWriter {
    file: File,
}

impl TraceFileWriter {
    pub(crate) fn new(file_name: String) -> Result<Self> {
        let file = File::create(file_name)?;
        Ok(Self { file })
    }
}

impl TraceWriter for TraceFileWriter {
    fn write(&mut self, record: Record) -> Result<()> {
        let buf = format!("{},{}\n", record.id(), record.op().serialize());
        self.file.write_all(buf.as_bytes())
    }

    fn write_all(&mut self, records: Vec<Record>) -> Result<()> {
        let buf: String = records
            .iter()
            .map(|r| format!("{},{}\n", r.id(), r.op().serialize()))
            .fold(String::new(), |a, b| a + &b);

        self.file.write_all(buf.as_bytes())
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
