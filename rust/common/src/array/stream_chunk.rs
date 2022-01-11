use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use prost::DecodeError;
use risingwave_pb::data::{Column as ProstColumn, Op as ProstOp, StreamChunk as ProstStreamChunk};

use crate::array::column::Column;
use crate::array::stream_chunk_iter::{RowRef, StreamChunkRefIter};
use crate::array::DataChunk;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result, RwError};
use crate::util::hash_util::finalize_hashers;
use crate::util::prost::unpack_from_any;

/// `Op` represents three operations in `StreamChunk`.
///
/// `UpdateDelete` and `UpdateInsert` are semantically equivalent to `Delete` and `Insert`
/// but always appear in pairs to represent an update operation.
/// For example, table source, aggregation and outer join can generate updates by themselves,
/// while most of the other operators only pass through updates with best effort.
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
#[derive(Default, Clone)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    ops: Vec<Op>,
    columns: Vec<Column>,
    visibility: Option<Bitmap>,
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

    /// `cardinality` return the number of visible tuples
    pub fn cardinality(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap.iter().map(|visible| visible as usize).sum()
        } else {
            self.capacity()
        }
    }

    /// `capacity` return physical length of internals ops & columns
    pub fn capacity(&self) -> usize {
        self.ops.len()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column(&self, index: usize) -> &Column {
        &self.columns[index]
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
                        array
                            .compact(visibility, cardinality)
                            .map(|array| Column::new(Arc::new(array)))
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

    pub fn get_hash_values<H: BuildHasher>(
        &self,
        keys: &[usize],
        hasher_builder: H,
    ) -> Result<Vec<u64>> {
        let mut states = Vec::with_capacity(self.cardinality());
        states.resize_with(self.capacity(), || hasher_builder.build_hasher());
        for key in keys {
            let array = self.columns[*key].array();
            array.hash_vec(&mut states);
        }
        Ok(finalize_hashers(&mut states))
    }

    /// Random access a tuple in a stream chunk. Return in a row format.
    ///
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> Result<(RowRef<'_>, bool)> {
        let row = self.row_at_unchecked_vis(pos);
        let vis = match self.visibility.as_ref() {
            Some(bitmap) => bitmap.is_set(pos)?,
            None => true,
        };
        Ok((row, vis))
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// Note that this function do not return whether the row is visible.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    pub fn row_at_unchecked_vis(&self, pos: usize) -> RowRef<'_> {
        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            row.push(column.array_ref().value_at(pos));
        }
        RowRef::new(self.ops[pos], row)
    }

    /// Get an iterator for visible rows.
    pub fn rows(&self) -> StreamChunkRefIter<'_> {
        StreamChunkRefIter::new(self)
    }

    /// `to_pretty_string` returns a table-like text representation of the `StreamChunk`.
    pub fn to_pretty_string(&self) -> String {
        use prettytable::format::Alignment;
        use prettytable::{format, Cell, Row, Table};

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for row in self.rows() {
            let mut cells = Vec::with_capacity(row.size() + 1);
            cells.push(Cell::new_align(
                match row.op() {
                    Op::Insert => "+",
                    Op::Delete => "-",
                    Op::UpdateDelete => "U-",
                    Op::UpdateInsert => "U+",
                },
                Alignment::RIGHT,
            ));
            for datum in &row.values {
                let str = match datum {
                    None => "".to_owned(), // NULL
                    Some(scalar) => scalar.to_string(),
                };
                cells.push(Cell::new(&str));
            }
            table.add_row(Row::new(cells));
        }
        table.to_string()
    }
}

impl fmt::Debug for StreamChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamChunk {{ cardinality = {}, capacity = {}, data = \n{} }}",
            self.cardinality(),
            self.capacity(),
            self.to_pretty_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::I64Array;
    use crate::{column, column_nonnull};

    #[test]
    fn test_to_pretty_string() {
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Delete, Op::UpdateDelete, Op::UpdateInsert],
            vec![
                column_nonnull!(I64Array, [1, 2, 3, 4]),
                column!(I64Array, [Some(6), None, Some(7), None]),
            ],
            None,
        );
        assert_eq!(
            chunk.to_pretty_string(),
            "\
+----+---+---+
|  + | 1 | 6 |
|  - | 2 |   |
| U- | 3 | 7 |
| U+ | 4 |   |
+----+---+---+
"
        );
    }
}
