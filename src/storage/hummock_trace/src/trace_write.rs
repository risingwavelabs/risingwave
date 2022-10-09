use std::io::{Read, Result as IOResult, Write};
use std::sync::Arc;

use bincode::{config, encode_into_std_write};
use parking_lot::Mutex;

use super::trace_record::Record;
use crate::error::Result;

pub(crate) static MAGIC_BYTES: u32 = 0x484D5452; // HMTR

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
    fn write(&mut self, record: Record) -> Result<usize> {
        let size = encode_into_std_write(record, &mut self.writer, config::standard())?;
        Ok(size)
    }

    fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

pub(crate) struct MemTraceStore {
    buf: Vec<u8>,
}

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

impl Read for MemTraceStore {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
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
    fn write(&mut self, record: Record) -> Result<usize> {
        self.mem.lock().push(record);
        Ok(0)
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}
