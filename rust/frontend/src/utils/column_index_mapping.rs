#![allow(dead_code)]

use std::vec;

use fixedbitset::FixedBitSet;
pub trait ColIndexMapping {
    /// panic if the input index is out of source
    fn map(&self, index: usize) -> usize;
}
pub type BoxedColIndexMapping = Box<dyn ColIndexMapping>;

struct CompositeMapping {
    mapping1: BoxedColIndexMapping,
    mapping2: BoxedColIndexMapping,
}
impl CompositeMapping {
    fn new(mapping1: BoxedColIndexMapping, mapping2: BoxedColIndexMapping) -> Self {
        Self { mapping1, mapping2 }
    }
}
impl ColIndexMapping for CompositeMapping {
    fn map(&self, index: usize) -> usize {
        self.mapping2.map(self.mapping1.map(index))
    }
}

struct ColPruneMapping {
    map: Vec<Option<usize>>,
}
impl ColIndexMapping for ColPruneMapping {
    fn map(&self, index: usize) -> usize {
        self.map[index].unwrap()
    }
}
impl ColPruneMapping {
    fn with_remaining_columns(cols: &FixedBitSet) -> Self {
        let mut map = vec![None; cols.len()];
        for (tar, src) in cols.ones().enumerate() {
            map[src] = Some(tar);
        }
        ColPruneMapping { map }
    }
    fn with_removed_columns(cols: &FixedBitSet) -> Self {
        let mut cols = cols.clone();
        cols.toggle_range(..);
        Self::with_remaining_columns(&cols)
    }
}

struct AddMapping {
    delta: usize,
}

impl AddMapping {
    pub fn new(delta: usize) -> Self {
        Self { delta }
    }
}
impl ColIndexMapping for AddMapping {
    fn map(&self, index: usize) -> usize {
        index + self.delta
    }
}

struct MinusMapping {
    delta: usize,
}

impl ColIndexMapping for MinusMapping {
    fn map(&self, index: usize) -> usize {
        assert!(index >= self.delta);
        index - self.delta
    }
}

impl MinusMapping {
    pub fn new(delta: usize) -> Self {
        Self { delta }
    }
}
