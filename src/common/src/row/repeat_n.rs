use super::Row2;
use crate::types::{DatumRef, ToDatumRef};

/// Row for the [`repeat_n`] function.
#[derive(Debug)]
pub struct RepeatN<D> {
    datum: D,
    n: usize,
}

impl<D: PartialEq> PartialEq for RepeatN<D> {
    fn eq(&self, other: &Self) -> bool {
        if self.n == 0 && other.n == 0 {
            true
        } else {
            self.datum == other.datum && self.n == other.n
        }
    }
}
impl<D: Eq> Eq for RepeatN<D> {}

impl<D: ToDatumRef> Row2 for RepeatN<D> {
    type Iter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    #[inline]
    fn datum_at(&self, index: usize) -> crate::types::DatumRef<'_> {
        if index < self.n {
            self.datum.to_datum_ref()
        } else {
            panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.n, index
            )
        }
    }

    #[inline]
    unsafe fn datum_at_unchecked(&self, _index: usize) -> crate::types::DatumRef<'_> {
        // Always ignore the index and return the datum.
        self.datum.to_datum_ref()
    }

    #[inline]
    fn len(&self) -> usize {
        self.n
    }

    #[inline]
    fn iter(&self) -> Self::Iter<'_> {
        itertools::repeat_n(self.datum.to_datum_ref(), self.n)
    }
}

/// Create a row which contains `n` repetitions of `datum`.
pub fn repeat_n<D: ToDatumRef>(datum: D, n: usize) -> RepeatN<D> {
    RepeatN { datum, n }
}
