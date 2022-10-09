use std::io::{Read, Result, Write};
use std::sync::Arc;

use bincode::{config, encode_into_slice};
use parking_lot::Mutex;

use super::trace_record::{Operation, Record};

static MAGIC_BYTES: u32 = 0xA8596D4F;

pub(crate) trait TraceWriter {
    fn write(&mut self, record: Record) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn write_all(&mut self, records: Vec<Record>) -> Result<()> {
        for record in records {
            self.write(record)?
        }
        Ok(())
    }
}

pub(crate) struct TraceWriterImpl<W: Write> {
    buf: Vec<u8>,
    writer: W,
}

impl<W: Write> TraceWriterImpl<W> {
    pub(crate) fn new(mut writer: W) -> Result<Self> {
        let buf = Vec::new();
        writer.write(&MAGIC_BYTES.to_le_bytes())?;
        Ok(Self { buf, writer })
    }
}

impl<W: Write> TraceWriter for TraceWriterImpl<W> {
    fn write(&mut self, record: Record) -> Result<()> {
        let size = encode_into_slice(record, &mut self.buf, config::standard()).unwrap();
        self.writer.write(&size.to_le_bytes())?; // write as little-endian
        self.writer.write_all(&self.buf[..size])
    }

    fn sync(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

pub(crate) struct MemTraceStore {
    buf: Vec<u8>,
}

impl Write for MemTraceStore {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        for b in buf {
            self.buf.push(*b);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Read for MemTraceStore {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        todo!()
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
