use parse_display::Display;

mod catalog;
mod config;
mod desc;

pub use catalog::*;
pub use config::*;
pub use desc::*;

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq)]
pub struct SinkId {
    pub sink_id: u32,
}

impl SinkId {
    pub const fn new(sink_id: u32) -> Self {
        SinkId { sink_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        SinkId {
            sink_id: u32::MAX - 1,
        }
    }

    pub fn sink_id(&self) -> u32 {
        self.sink_id
    }
}

impl From<u32> for SinkId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<SinkId> for u32 {
    fn from(id: SinkId) -> Self {
        id.sink_id
    }
}
