use crate::task::channel::{BoxChanReceiver, BoxChanSender, ChanReceiver, ChanSender};
use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result};
use std::option::Option;
use std::sync::mpsc;

pub struct FifoSender {
    sender: mpsc::Sender<DataChunk>,
}

pub struct FifoReceiver {
    receiver: mpsc::Receiver<DataChunk>,
}

#[async_trait::async_trait]
impl ChanSender for FifoSender {
    async fn send(&mut self, chunk: DataChunk) -> Result<()> {
        self.sender.send(chunk).map_err(|e| {
            ErrorCode::InternalError(format!("chunk was sent to a closed channel {}", e)).into()
        })
    }
}

#[async_trait::async_trait]
impl ChanReceiver for FifoReceiver {
    async fn recv(&mut self) -> Option<DataChunk> {
        match self.receiver.recv() {
            Err(_) => None, // Sender is dropped.
            Ok(chunk) => Some(chunk),
        }
    }
}

pub fn new_fifo_channel() -> (BoxChanSender, Vec<BoxChanReceiver>) {
    let (s, r) = mpsc::channel();
    (
        Box::new(FifoSender { sender: s }),
        vec![Box::new(FifoReceiver { receiver: r })],
    )
}
