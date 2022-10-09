use std::sync::atomic::{AtomicU64, Ordering};

use bincode::{Decode, Encode};

pub type RecordID = u64;

static NEXT_RECORD_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_record_id() -> RecordID {
    NEXT_RECORD_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Encode, Decode, Debug, PartialEq)]
pub(crate) struct Record(RecordID, Operation);

impl Record {
    pub(crate) fn new(id: RecordID, op: Operation) -> Self {
        Self(id, op)
    }

    pub(crate) fn id(&self) -> RecordID {
        self.0
    }

    pub(crate) fn op(&self) -> &Operation {
        &self.1
    }
}

pub trait TraceRecord {
    fn serialize(&self) -> String;
}

#[derive(Debug, Encode, Decode, PartialEq)]
pub enum Operation {
    Get(Vec<u8>),
    Ingest(Vec<(Vec<u8>, Vec<u8>)>),
    Iter(Vec<u8>),
    Sync(u64),
    Seal(u64, bool),
    Finish(),
}
