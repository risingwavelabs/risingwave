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

use std::fmt::Debug;
use std::sync::Once;

use async_trait::async_trait;
use risingwave_common::error::Result;

use crate::executor::{Executor, Message};

pub const CACHE_CLEAR_ENABLED_ENV_VAR_KEY: &str = "RW_NO_CACHE";

/// [`CacheClearExecutor`] clears the memory cache of `input` executor after a barrier passes.
#[derive(Debug)]
pub struct CacheClearExecutor {
    /// The input of the current executor.
    input: Box<dyn Executor>,
}

impl CacheClearExecutor {
    pub fn new(input: Box<dyn Executor>) -> Self {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| info!("CacheClearExecutor enabled."));

        Self { input }
    }
}

#[async_trait]
impl super::DebugExecutor for CacheClearExecutor {
    async fn next(&mut self) -> Result<Message> {
        let message = self.input.next().await?;

        if let Message::Barrier(_) = &message {
            self.input.clear_cache()?;
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
