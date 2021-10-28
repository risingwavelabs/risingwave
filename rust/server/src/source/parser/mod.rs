pub use json_parser::*;
pub use protobuf_parser::*;

use crate::error::Result;
use crate::source::SourceColumnDesc;
use crate::types::Datum;

mod json_parser;
mod protobuf_parser;

/// `SourceParser` is the message parser, `ChunkReader` will parse the messages in `SourceReader` one by one through `SourceParser` and assemble them into `DataChunk`
pub trait SourceParser: Send + Sync + 'static {
    /// parse needs to be a member method because some format like Protobuf needs to be pre-compiled
    fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Vec<Datum>>;
}
