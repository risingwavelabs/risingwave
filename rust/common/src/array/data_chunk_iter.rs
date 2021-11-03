use std::cmp::Ordering;

use itertools::Itertools;

use crate::array::DataChunk;
use crate::types::{Datum, DatumRef, ToOwnedDatum};

pub struct DataChunkRefIter<'a> {
    chunk: &'a DataChunk,
    idx: usize,
}

/// Data Chunk iter only iterate visible tuples.
impl<'a> Iterator for DataChunkRefIter<'a> {
    type Item = RowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.idx >= self.chunk.capacity() {
                return None;
            }
            let (cur_val, vis) = self.chunk.row_at(self.idx).ok()?;
            self.idx += 1;
            if vis {
                return Some(cur_val);
            }
        }
    }
}

impl<'a> DataChunkRefIter<'a> {
    pub fn new(chunk: &'a DataChunk) -> Self {
        Self { chunk, idx: 0 }
    }
}

/// TODO: Consider merge with Row in storage. It is end with Ref because it do not own data
/// and avoid conflict with [`Row`].
#[derive(Debug, PartialEq)]
pub struct RowRef<'a>(Vec<DatumRef<'a>>);

impl<'a> RowRef<'a> {
    pub fn new(values: Vec<DatumRef<'a>>) -> Self {
        Self(values)
    }

    pub fn value_at(&self, pos: usize) -> DatumRef<'a> {
        self.0[pos]
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl From<RowRef<'_>> for Row {
    fn from(row_ref: RowRef<'_>) -> Self {
        Row(row_ref
            .0
            .into_iter()
            .map(ToOwnedDatum::to_owned_datum)
            .collect::<Vec<_>>())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Row(pub Vec<Datum>);

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0.iter().zip(other.0.iter()).all_equal()
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        for (x, y) in self.0.iter().zip(other.0.iter()) {
            match x.partial_cmp(y) {
                Some(Ordering::Equal) => continue,
                order => return order,
            }
        }
        Some(Ordering::Equal)
    }
}

impl Eq for Row {}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}
