use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use crossbeam::channel::{unbounded, Receiver, Sender};

use super::hummock_trace_log::{TraceFileWriter, TraceWriter};
use crate::storage_value::StorageValue;

// HummockTrace traces operations from Hummock
pub struct HummockTrace {
    records_tx: Sender<RecordRequest>,
}

impl HummockTrace {
    pub(crate) fn new() -> Self {
        let writer = TraceFileWriter::new("hummock.trace".to_string()).unwrap();

        Self::new_with_writer(Box::new(writer))
    }

    pub(crate) fn new_with_writer(writer: Box<dyn TraceWriter + Send>) -> Self {
        let (records_tx, records_rx) = unbounded::<RecordRequest>();
        let (writer_tx, writer_rx) = unbounded::<WriteRequest>();

        // start a worker to receive hummock records
        tokio::spawn(Self::start_collector_worker(records_rx, writer_tx, 100));

        // start a worker to receive write requests
        tokio::spawn(Self::start_writer_worker(writer, writer_rx));

        Self { records_tx }
    }

    pub fn new_trace_span(&self, op: Operation) -> TraceSpan {
        let id = next_record_id();
        let span = TraceSpan::new(self.records_tx.clone(), id);
        span.send(op);
        span
    }

    async fn start_collector_worker(
        records_rx: Receiver<RecordRequest>,
        writer_tx: Sender<WriteRequest>,
        records_capacity: usize,
    ) {
        let mut records = Vec::with_capacity(records_capacity); // sorted records?

        loop {
            if let Ok(message) = records_rx.recv() {
                match message {
                    RecordRequest::Record(record) => {
                        records.push(record);
                        if records.len() == records_capacity {
                            writer_tx.send(WriteRequest::Write(records)).unwrap();
                            records = Vec::with_capacity(records_capacity);
                        }
                    }
                    RecordRequest::Fin() => {
                        writer_tx.send(WriteRequest::Write(records)).unwrap();
                        writer_tx.send(WriteRequest::Fin()).unwrap();
                        return;
                    }
                }
            }
        }
    }

    async fn start_writer_worker(
        mut writer: Box<dyn TraceWriter + Send>,
        writer_rx: Receiver<WriteRequest>,
    ) {
        loop {
            if let Ok(request) = writer_rx.recv() {
                match request {
                    WriteRequest::Write(records) => {
                        writer.write_all(records).unwrap();
                    }
                    WriteRequest::Fin() => {
                        writer.sync().unwrap();
                        return;
                    }
                }
            }
        }
    }
}

impl Drop for HummockTrace {
    fn drop(&mut self) {
        // close the workers
        self.records_tx.send(RecordRequest::Fin()).unwrap();
    }
}

pub type RecordID = u64;

static NEXT_RECORD_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_record_id() -> RecordID {
    NEXT_RECORD_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub enum RecordRequest {
    Record(Record),
    Fin(),
}

pub enum WriteRequest {
    Write(Vec<Record>),
    Fin(),
}

pub(crate) type Record = (RecordID, Operation);

pub trait TraceRecord {
    fn serialize(&self) -> String;
}

#[derive(Debug)]
pub enum Operation {
    Get(Vec<u8>),
    Ingest(Vec<(Bytes, StorageValue)>),
    Iter(Vec<u8>),
    Sync(u64),
    Seal(u64, bool),
    Finish(),
}

impl TraceRecord for Operation {
    fn serialize(&self) -> String {
        match self {
            Operation::Get(key) => {
                format!("GET {:?}", key)
            }
            Operation::Ingest(kvs) => {
                format!("INGEST {:?}", kvs)
            }
            Operation::Iter(value) => {
                format!("ITER {:?}", value)
            }
            Operation::Sync(epoch) => {
                format!("SYNC {}", epoch)
            }
            Operation::Seal(epoch, is_checkpoint) => {
                format!("SEAL {} {}", epoch, is_checkpoint)
            }
            Operation::Finish() => "FINISH".to_string(),
        }
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.serialize() == other.serialize()
    }
}

#[derive(Clone)]
pub struct TraceSpan {
    tx: Sender<RecordRequest>,
    id: RecordID,
}

impl TraceSpan {
    pub fn new(tx: Sender<RecordRequest>, id: RecordID) -> Self {
        Self { tx, id }
    }

    pub fn send(&self, op: Operation) {
        self.tx.send(RecordRequest::Record((self.id, op))).unwrap();
    }

    pub fn finish(&self) {
        self.tx
            .send(RecordRequest::Record((self.id, Operation::Finish())))
            .unwrap();
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::{next_record_id, HummockTrace, Operation};
    use crate::monitor::hummock_trace_log::TraceMemWriter;

    // test atomic id
    #[tokio::test()]
    async fn atomic_span_id() {
        let mut handles = Vec::new();
        let ids_lock = Arc::new(Mutex::new(HashSet::new()));
        let count: u64 = 100;

        for _ in 0..count {
            let ids = ids_lock.clone();
            handles.push(tokio::spawn(async move {
                let id = next_record_id();
                ids.lock().insert(id);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let ids = ids_lock.lock();

        for i in 0..count {
            assert_eq!(ids.contains(&i), true);
        }
    }

    #[tokio::test()]
    async fn span_sequential() {
        let log_lock = Arc::new(Mutex::new(Vec::new()));
        let writer_log = log_lock.clone();
        tokio::spawn(async move {
            let writer = TraceMemWriter::new(writer_log);
            let tracer = Arc::new(HummockTrace::new_with_writer(Box::new(writer)));
            {
                tracer.new_trace_span(Operation::Get(vec![0]));
            }
            {
                tracer.new_trace_span(Operation::Sync(0));
            }
        })
        .await
        .unwrap();

        let log = log_lock.lock();

        assert_eq!(log.len(), 4);
        assert_eq!(log.get(0).unwrap(), &(0, Operation::Get(vec![0])));
        assert_eq!(log.get(1).unwrap(), &(0, Operation::Finish()));
        assert_eq!(log.get(2).unwrap(), &(1, Operation::Sync(0)));
        assert_eq!(log.get(3).unwrap(), &(1, Operation::Finish()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn span_concurrent_in_memory() {
        let log_lock = Arc::new(Mutex::new(Vec::new()));
        let count = 100;

        {
            let writer = TraceMemWriter::new(log_lock.clone());
            let tracer = Arc::new(HummockTrace::new_with_writer(Box::new(writer)));

            let mut handles = Vec::new();

            for i in 0..count {
                let t = tracer.clone();
                handles.push(tokio::spawn(async move {
                    t.new_trace_span(Operation::Get(vec![i]));
                    t.new_trace_span(Operation::Sync(i as u64));
                }));
            }

            for handle in handles {
                handle.await.unwrap();
            }
        }

        let log = log_lock.lock();
        assert_eq!(log.len(), (count as usize) * 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 50)]
    async fn span_concurrent_in_file() {
        let count = 100;
        let tracer = Arc::new(HummockTrace::new());

        let mut handles = Vec::new();

        for i in 0..count {
            let t = tracer.clone();
            handles.push(tokio::spawn(async move {
                t.new_trace_span(Operation::Get(vec![i]));
                t.new_trace_span(Operation::Sync(i as u64));
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
