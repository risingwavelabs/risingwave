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
use std::fmt::Debug;

use async_trait::async_trait;
use risingwave_common::error::Result;

use crate::executor::{Executor, Message};

#[derive(Debug)]
pub struct EpochCheckExecutor {
    /// The input of the current executor.
    input: Box<dyn Executor>,

    /// Epoch number recorded from last barrier message.
    last_epoch: Option<u64>,
}

impl EpochCheckExecutor {
    pub fn new(input: Box<dyn Executor>) -> Self {
        Self {
            input,
            last_epoch: None,
        }
    }
}

#[async_trait]
impl super::DebugExecutor for EpochCheckExecutor {
    async fn next(&mut self) -> Result<Message> {
        let message = self.input.next().await?;

        if let Message::Barrier(b) = &message {
            let new_epoch = b.epoch.curr;
            let stale = self
                .last_epoch
                .map(|last_epoch| last_epoch > new_epoch)
                .unwrap_or(false);

            if stale {
                panic!(
                    "epoch check failed on {}: last epoch is {:?}, while the epoch of incoming barrier is {}.\nstale barrier: {:?}",
                    self.input.identity(),
                    self.last_epoch,
                    new_epoch,
                    b
                );
            }
            self.last_epoch = Some(new_epoch);
        }

        Ok(message)
    }

    fn input(&self) -> &dyn Executor {
        self.input.as_ref()
    }

    fn input_mut(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::StreamChunk;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_epoch_ok() {
        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks([StreamChunk::default()].into_iter());
        source.push_barrier(114, false);
        source.push_barrier(114, false);
        source.push_barrier(514, false);

        let mut checked = EpochCheckExecutor::new(Box::new(source));
        assert_matches!(checked.next().await.unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(b) if b.epoch.curr == 114);
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(b) if b.epoch.curr == 114);
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(b) if b.epoch.curr == 514);
    }

    #[should_panic]
    #[tokio::test]
    async fn test_epoch_bad() {
        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks([StreamChunk::default()].into_iter());
        source.push_barrier(514, false);
        source.push_barrier(514, false);
        source.push_barrier(114, false);

        let mut checked = EpochCheckExecutor::new(Box::new(source));
        assert_matches!(checked.next().await.unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(b) if b.epoch.curr == 514);
        assert_matches!(checked.next().await.unwrap(), Message::Barrier(b) if b.epoch.curr == 514);

        checked.next().await.unwrap(); // should panic
    }
}
