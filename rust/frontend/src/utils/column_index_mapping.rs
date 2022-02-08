#![allow(dead_code)]
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
