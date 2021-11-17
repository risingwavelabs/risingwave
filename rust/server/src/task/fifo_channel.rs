use crate::task::channel::{BoxChanReceiver, BoxChanSender, ChanReceiver, ChanSender};
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use tokio::sync::mpsc;

pub struct FifoSender {
    sender: mpsc::UnboundedSender<Option<DataChunk>>,
}

pub struct FifoReceiver {
    receiver: mpsc::UnboundedReceiver<Option<DataChunk>>,
}

#[async_trait::async_trait]
impl ChanSender for FifoSender {
    async fn send(&mut self, chunk: Option<DataChunk>) -> Result<()> {
        self.sender
            .send(chunk)
            .to_rw_result_with("FifoSender::send")
    }
}

#[async_trait::async_trait]
impl ChanReceiver for FifoReceiver {
    async fn recv(&mut self) -> Result<Option<DataChunk>> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| InternalError("broken fifo_channel".to_string()).into())
        // We never call on a closed channel.
    }
}

pub fn new_fifo_channel() -> (BoxChanSender, Vec<BoxChanReceiver>) {
    let (s, r) = mpsc::unbounded_channel();
    (
        Box::new(FifoSender { sender: s }),
        vec![Box::new(FifoReceiver { receiver: r })],
    )
}

mod tests {
    #[tokio::test]
    async fn test_recv_fail_on_closed_channel() {
        use crate::task::fifo_channel::new_fifo_channel;

        let (sender, mut receivers) = new_fifo_channel();
        assert_eq!(receivers.len(), 1);
        drop(sender);

        let receiver = receivers.get_mut(0).unwrap();
        assert_eq!(
            receiver.recv().await.unwrap_err().to_string(),
            "internal error: broken fifo_channel"
        );
    }
}
