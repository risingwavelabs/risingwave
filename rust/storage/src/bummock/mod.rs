use risingwave_common::array::DataChunkRef;

pub enum BummockResult {
    Data(Vec<DataChunkRef>),
    DataEof,
}
