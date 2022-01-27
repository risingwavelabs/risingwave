use std::fmt::Debug;

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
