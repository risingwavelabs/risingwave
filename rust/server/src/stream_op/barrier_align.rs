use super::{Executor, Message, StreamChunk};
use risingwave_common::error::Result;
use tokio::select;

#[derive(Debug, PartialEq)]
enum BarrierWaitState {
    Left,
    Right,
    Either,
}

pub enum AlignedMessage {
    Left(Result<StreamChunk>),
    Right(Result<StreamChunk>),
    // TODO: change this to `Result<Barrier>` when we have a `Barrier`
    Barrier(Result<Message>),
}

pub struct BarrierAligner {
    /// The input from the left executor
    input_l: Box<dyn Executor>,
    /// The input from the right executor
    input_r: Box<dyn Executor>,
    /// The barrier state
    state: BarrierWaitState,
}

impl BarrierAligner {
    pub fn new(input_l: Box<dyn Executor>, input_r: Box<dyn Executor>) -> Self {
        Self {
            input_l,
            input_r,
            state: BarrierWaitState::Either,
        }
    }

    pub async fn next(&mut self) -> AlignedMessage {
        loop {
            select! {
              message = self.input_l.next(), if self.state != BarrierWaitState::Right => {
                match message {
                  Ok(message) => match message {
                    Message::Chunk(chunk) => break AlignedMessage::Left(Ok(chunk)),
                    Message::Barrier(_) => {
                      match self.state {
                        BarrierWaitState::Left => {
                          self.state = BarrierWaitState::Either;
                          break AlignedMessage::Barrier(Ok(message));
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
              message = self.input_r.next(), if self.state != BarrierWaitState::Left => {
                match message {
                  Ok(message) => match message {
                    Message::Chunk(chunk) => break AlignedMessage::Right(Ok(chunk)),
                    Message::Barrier(_) => match self.state {
                      BarrierWaitState::Right => {
                        self.state = BarrierWaitState::Either;
                        break AlignedMessage::Barrier(Ok(message));
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
            }
        }
    }
}
