use std::sync::Arc;

use prost::DecodeError;
use risingwave_pb::data::{Column as ProstColumn, Op as ProstOp, StreamChunk as ProstStreamChunk};

use crate::array::column::Column;
use crate::array::DataChunk;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result, RwError};
use crate::util::prost::unpack_from_any;

/// `Op` represents three operations in `StreamChunk`.
/// `UpdateDelete` and `UpdateInsert` always appear in pairs.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum Op {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

impl Op {
    pub fn to_protobuf(self) -> ProstOp {
        match self {
            Op::Insert => ProstOp::Insert,
            Op::Delete => ProstOp::Delete,
            Op::UpdateInsert => ProstOp::UpdateInsert,
            Op::UpdateDelete => ProstOp::UpdateDelete,
        }
    }

    pub fn from_protobuf(prost: &i32) -> Result<Op> {
        let op = match ProstOp::from_i32(*prost) {
            Some(ProstOp::Insert) => Op::Insert,
            Some(ProstOp::Delete) => Op::Delete,
            Some(ProstOp::UpdateInsert) => Op::UpdateInsert,
            Some(ProstOp::UpdateDelete) => Op::UpdateDelete,
            None => {
                return Err(RwError::from(ErrorCode::ProstError(DecodeError::new(
                    "No such op type",
                ))))
            }
        };
        Ok(op)
    }
}

pub type Ops<'a> = &'a [Op];

/// `StreamChunk` is used to pass data over the streaming pathway.
#[derive(Default, Debug, Clone)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    pub ops: Vec<Op>,
    pub columns: Vec<Column>,
    pub visibility: Option<Bitmap>,
}

impl StreamChunk {
    pub fn new(ops: Vec<Op>, columns: Vec<Column>, visibility: Option<Bitmap>) -> Self {
        for col in &columns {
            assert_eq!(col.array_ref().len(), ops.len());
        }
        StreamChunk {
            ops,
            columns,
            visibility,
        }
    }

    /// return the number of visible tuples
    pub fn cardinality(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap.iter().map(|visible| visible as usize).sum()
        } else {
            self.capacity()
        }
    }

    /// return physical length of any chunk column
    pub fn capacity(&self) -> usize {
        self.columns
            .first()
            .map(|col| col.array_ref().len())
            .unwrap_or(0)
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// compact the `StreamChunk` with its visibility map
    pub fn compact(self) -> Result<Self> {
        match &self.visibility {
            None => Ok(self),
            Some(visibility) => {
                let cardinality = visibility
                    .iter()
                    .fold(0, |vis_cnt, vis| vis_cnt + vis as usize);
                let columns = self
                    .columns
                    .into_iter()
                    .map(|col| {
                        let array = col.array();
                        let data_type = col.data_type();
                        array
                            .compact(visibility, cardinality)
                            .map(|array| Column::new(Arc::new(array), data_type))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut ops = Vec::with_capacity(cardinality);
                for (op, visible) in self.ops.into_iter().zip(visibility.iter()) {
                    if visible {
                        ops.push(op);
                    }
                }
                Ok(StreamChunk {
                    ops,
                    columns,
                    visibility: None,
                })
            }
        }
    }

    pub fn into_parts(self) -> (DataChunk, Vec<Op>) {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = self;
        let builder = DataChunk::builder().columns(columns);
        let data_chunk = if let Some(vis) = visibility {
            builder.visibility(vis).build()
        } else {
            builder.build()
        };
        (data_chunk, ops)
    }

    pub fn into_inner(self) -> (Vec<Op>, Vec<Column>, Option<Bitmap>) {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = self;

        (ops, columns, visibility)
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamChunk> {
        Ok(ProstStreamChunk {
            cardinality: self.cardinality() as u32,
            ops: self.ops.iter().map(|op| op.to_protobuf() as i32).collect(),
            columns: self
                .columns
                .iter()
                .map(|col| Ok(unpack_from_any::<ProstColumn>(&col.to_protobuf()?).unwrap()))
                .collect::<Result<Vec<_>>>()?,
        })
    }

    pub fn from_protobuf(prost: &ProstStreamChunk) -> Result<Self> {
        let cardinality = prost.get_cardinality() as usize;
        let mut stream_chunk = StreamChunk {
            ops: vec![],
            columns: vec![],
            visibility: None,
        };
        for op in prost.get_ops() {
            stream_chunk.ops.push(Op::from_protobuf(op)?);
        }

        for column in prost.get_columns() {
            stream_chunk
                .columns
                .push(Column::from_protobuf(column, cardinality)?);
        }

        Ok(stream_chunk)
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn visibility(&self) -> &Option<Bitmap> {
        &self.visibility
    }
}
