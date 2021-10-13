use crate::array::DataChunkRef;
use crate::error::{ErrorCode, Result};
use crate::task::channel::{BoxChanReceiver, BoxChanSender, ChanReceiver, ChanSender};
use std::option::Option;
use std::sync::mpsc;

pub struct FifoSender {
    sender: mpsc::Sender<DataChunkRef>,
}

pub struct FifoReceiver {
    receiver: mpsc::Receiver<DataChunkRef>,
}

#[async_trait::async_trait]
impl ChanSender for FifoSender {
    async fn send(&mut self, chunk: DataChunkRef) -> Result<()> {
        self.sender.send(chunk).map_err(|e| {
            ErrorCode::InternalError(format!("chunk was sent to a closed channel {}", e)).into()
        })
    }
}

#[async_trait::async_trait]
impl ChanReceiver for FifoReceiver {
    async fn recv(&mut self) -> Option<DataChunkRef> {
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
