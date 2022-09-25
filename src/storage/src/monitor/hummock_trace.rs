use std::{sync::atomic::AtomicU64};

use bytes::Bytes;
use crossbeam::channel::{unbounded, Receiver, Sender};
use tokio::{fs::File, io::AsyncWriteExt};
use crate::storage_value::StorageValue;

// HummockTrace traces operations from Hummock
pub(crate) struct HummockTrace {
  records_tx: Sender<RecordRequest>,
  writer_tx: Sender<WriteRequest>,
}

impl HummockTrace{

  pub fn new()->Self{
    let (records_tx, records_rx) = unbounded::<RecordRequest>();
    let (writer_tx, writer_rx) = unbounded::<WriteRequest>();

    // start a worker to receive hummock records
    tokio::spawn(Self::start_collector_worker(records_rx, writer_tx.clone() ,100));

    // start a worker to receive write requests
    tokio::spawn(Self::start_writer_worker(writer_rx));

    Self { records_tx, writer_tx}
  }

  pub fn new_trace_span(&self, op: Operation)->TraceSpan{
    let id = next_record_id();
    let span = TraceSpan::new(self.records_tx.clone(), id);
    span.send(op);
    span
  }

  async fn start_collector_worker(records_rx: Receiver<RecordRequest>, writer_tx:Sender<WriteRequest> ,records_capacity: usize){

    let mut records = Vec::with_capacity(records_capacity); // sorted records?

    loop{
      if let Ok(message) = records_rx.recv(){
        match message{
          RecordRequest::Record(record)=>{
            records.push(record);
            if records.len() == records_capacity{
              writer_tx.send(WriteRequest::Write(records)).unwrap();
              records = Vec::with_capacity(records_capacity);
            }
          }
          RecordRequest::Fin() => {
            break;
          }
        }
      }
    }

  }

  async fn start_writer_worker(writer_rx: Receiver<WriteRequest>){
    let mut log_file = File::create("hummock_trace.log").await.unwrap();

    loop{
      if let Ok(request) = writer_rx.recv(){
        match request{
          WriteRequest::Write(records)=>{
            let mut buf = Vec::with_capacity(records.len());
            for (id, op) in records{
              let content = format!("{},{}" ,id, op.serialize());
              buf.push(content);
            }
            log_file.write_all(buf.join("\n").as_bytes()).await.unwrap();
          }
          WriteRequest::Fin() => {
            break;
          }
        }
      }
    }
  }
}


impl Drop for HummockTrace{
  fn drop(&mut self){
    // close the workers
    self.records_tx.send(RecordRequest::Fin()).unwrap();
    self.writer_tx.send(WriteRequest::Fin()).unwrap();
  }
}

pub type RecordID = u64;

static NEXT_RECORD_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_record_id() -> RecordID {
  NEXT_RECORD_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub(crate) enum RecordRequest {
  Record(Record),
  Fin()
}

pub(crate) enum WriteRequest {
  Write(Vec<Record>),
  Fin()
}

pub(crate) type Record = (RecordID, Operation);

pub trait TraceRecord {
  fn serialize(self)->String;
}

pub enum Operation{
  Get(Vec<u8>),
  Ingest(Vec<(Bytes, StorageValue)>),
  Iter(Vec<u8>),
  Sync(u64),
  Seal(u64, bool),
  Finish(),
}

impl TraceRecord for Operation{
    fn serialize(self)->String {
      match self {
        Operation::Get(key) => {
          format!("GET {:?}", key)
        }
        Operation::Ingest(kvs) => {
          format!("INGEST {:?}", kvs)
        }
        Operation::Iter(prefix) => {
         format!("ITER {}", String::from_utf8(prefix).unwrap())
        }
        Operation::Sync(epoch) => {
          format!("SYNC {}", epoch)
        }
        Operation::Seal(epoch, is_checkpoint) => {
          format!("SEAL {} {}", epoch, is_checkpoint)
        }
        Operation::Finish() => {
          "FINISH".to_string()
        }
      }
    }
}

#[derive(Clone)]
pub(crate) struct TraceSpan {
  tx: Sender<RecordRequest>,
  id: RecordID
}

impl TraceSpan{
  pub fn new(tx: Sender<RecordRequest>, id: RecordID)->Self{
    Self {tx, id}
  }

  pub fn send(&self, op: Operation){
    self.tx.send(RecordRequest::Record((self.id, op))).unwrap();
  }

  pub fn finish(&self){
    self.tx.send(RecordRequest::Record((self.id, Operation::Finish()))).unwrap();
  }
}

impl Drop for TraceSpan{
  fn drop(&mut self) {
    self.finish();
  }
}