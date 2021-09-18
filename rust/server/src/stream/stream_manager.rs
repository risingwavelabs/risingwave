use crate::error::Result;
use risingwave_proto::stream_plan::StreamFragment;

// Place holder for stream manager.

pub struct StreamManager {}

impl StreamManager {
    pub fn new() -> Self {
        StreamManager {}
    }
    pub fn create_fragment(&self, _fragment: StreamFragment) -> Result<()> {
        Ok(())
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}
