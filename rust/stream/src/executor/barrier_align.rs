use risingwave_common::error::Result;
use tokio::sync::mpsc;
use tokio::{self, select};

use super::{Barrier, Executor, Message, StreamChunk};

#[derive(Debug, PartialEq)]
enum BarrierWaitState {
    Left,
    Right,
    Either,
}

pub enum AlignedMessage {
    Left(Result<StreamChunk>),
    Right(Result<StreamChunk>),
    Barrier(Barrier),
}

pub struct BarrierAligner {
    // /// The input from the left executor
    // input_l: Box<dyn Executor>,
    // /// The input from the right executor
    // input_r: Box<dyn Executor>,
    /// The input from the left executor
    input_l: mpsc::Receiver<Result<Message>>,
    /// The input from the right executor
    input_r: mpsc::Receiver<Result<Message>>,
    /// The barrier state
    state: BarrierWaitState,
}

impl BarrierAligner {
    pub fn new(mut input_l: Box<dyn Executor>, mut input_r: Box<dyn Executor>) -> Self {
        let (tx_l, rx_l) = mpsc::channel(32);
        let (tx_r, rx_r) = mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                let message = input_l.next().await;
                tx_l.send(message).await.unwrap();
            }
        });

        tokio::spawn(async move {
            loop {
                let message = input_r.next().await;
                tx_r.send(message).await.unwrap();
            }
        });
        Self {
            input_l: rx_l,
            input_r: rx_r,
            state: BarrierWaitState::Either,
        }
    }

    pub async fn next(&mut self) -> AlignedMessage {
        loop {
            select! {
              Some(message) = self.input_l.recv(), if self.state != BarrierWaitState::Right => {
                match message {
                  Ok(message) => match message {
                    Message::Chunk(chunk) => break AlignedMessage::Left(Ok(chunk)),
                    Message::Barrier(barrier) => {
                      match self.state {
                        BarrierWaitState::Left => {
                          self.state = BarrierWaitState::Either;
                          break AlignedMessage::Barrier(barrier);
                        }
                        BarrierWaitState::Either => {
                          self.state = BarrierWaitState::Right;
                        }
                        _ => unreachable!("Should not reach this barrier state: {:?}", self.state),
                      };
                    },
                  },
                  Err(e) => break AlignedMessage::Left(Err(e)),
                }
              },
              Some(message) = self.input_r.recv(), if self.state != BarrierWaitState::Left => {
                match message {
                  Ok(message) => match message {
                    Message::Chunk(chunk) => break AlignedMessage::Right(Ok(chunk)),
                    Message::Barrier(barrier) => match self.state {
                      BarrierWaitState::Right => {
                        self.state = BarrierWaitState::Either;
                        break AlignedMessage::Barrier(barrier);
                      }
                      BarrierWaitState::Either => {
                        self.state = BarrierWaitState::Left;
                      }
                      _ => unreachable!("Should not reach this barrier state: {:?}", self.state),
                    },
                  },
                  Err(e) => break AlignedMessage::Right(Err(e)),
                }
              }
              else => {
                unreachable!("Both channels closed");
              }
            }
        }
    }
}
