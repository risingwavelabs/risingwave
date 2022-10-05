use std::mem::size_of;
use std::sync::atomic::AtomicU64;

use bincode::{Decode, Encode};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub type RecordID = u64;

static NEXT_RECORD_ID: AtomicU64 = AtomicU64::new(0);

pub fn next_record_id() -> RecordID {
    NEXT_RECORD_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Encode, Decode, Debug)]
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

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

pub trait TraceRecord {
    fn serialize(&self) -> String;
}

#[derive(Debug, Encode, Decode)]
pub enum Operation {
    Get(Vec<u8>),
    Ingest(Vec<(Vec<u8>, Vec<u8>)>),
    Iter(Vec<u8>),
    Sync(u64),
    Seal(u64, bool),
    Finish(),
}

impl Operation {
    pub(crate) fn serialize(&self) -> String {
        match self {
            Operation::Get(key) => {
                let mut buf = BytesMut::with_capacity(key.len() + size_of::<u64>());
            }
            Operation::Ingest(kvs) => {}
            Operation::Iter(value) => {}
            Operation::Sync(epoch) => {}
            Operation::Seal(epoch, is_checkpoint) => {}
            Operation::Finish() => {}
        };
        String::from("")
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self.serialize() == other.serialize()
    }
}

impl TraceRecord for Record {
    fn serialize(&self) -> String {
        let (id, op) = (self.0, self.op());
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_u64(id);
        match op {
            Operation::Get(_) => todo!(),
            Operation::Ingest(_) => todo!(),
            Operation::Iter(_) => todo!(),
            Operation::Sync(epoch_id) => {
                buf.put_u8(3);
            }
            Operation::Seal(_, _) => todo!(),
            Operation::Finish() => todo!(),
        };
        return String::from("123");
    }
}
