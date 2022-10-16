use crossbeam::channel::{unbounded, Sender};

use crate::RecordMsg;

thread_local! {
  static RECORD_SENDER: Sender<RecordMsg> = {
    let (tx, rx) = unbounded();
    tx
  }
}

struct Collector;

impl Collector {}
