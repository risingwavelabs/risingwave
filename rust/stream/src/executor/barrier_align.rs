// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use async_stream::try_stream;
use futures::StreamExt;
use risingwave_common::error::Result;
use tokio::select;

use super::{Barrier, Executor, Message, StreamChunk};
use crate::executor::BoxedExecutorStream;

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

impl<'a> TryFrom<&'a AlignedMessage> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a AlignedMessage) -> std::result::Result<Self, Self::Error> {
        match m {
            AlignedMessage::Barrier(b) => Ok(b),
            _ => Err(()),
        }
    }
}

pub struct BarrierAligner {
    /// The input from the left executor
    input_l: BoxedExecutorStream,
    /// The input from the right executor
    input_r: BoxedExecutorStream,
    /// The barrier state
    state: BarrierWaitState,
}

impl BarrierAligner {
    pub fn new(mut input_l: Box<dyn Executor>, mut input_r: Box<dyn Executor>) -> Self {
        // Wrap the input executors into streams to ensure cancellation-safety
        let input_l = try_stream! {
            loop {
                let message = input_l.next().await?;
                yield message;
            }
        };
        let input_r = try_stream! {
            loop {
                let message = input_r.next().await?;
                yield message;
            }
        };
        Self {
            input_l: Box::pin(input_l),
            input_r: Box::pin(input_r),
            state: BarrierWaitState::Either,
        }
    }

    pub async fn next(&mut self) -> AlignedMessage {
        loop {
            select! {
                message = self.input_l.next(), if self.state != BarrierWaitState::Right => {
                match message.unwrap() {
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
                message = self.input_r.next(), if self.state != BarrierWaitState::Left => {
                    match message.unwrap() {
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
            }
        }
    }
}
