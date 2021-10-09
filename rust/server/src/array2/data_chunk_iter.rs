use crate::types::ScalarRefImpl;

/// TODO: Consider merge with Row in storage. It is end with Ref because it do not own data
/// and avoid conflict with [`Row`].
pub struct RowRef<'a>(Vec<Option<ScalarRefImpl<'a>>>);

impl<'a> RowRef<'a> {
    pub fn new(values: Vec<Option<ScalarRefImpl<'a>>>) -> Self {
        Self(values)
    }

    pub fn value_at(&self, pos: usize) -> Option<&ScalarRefImpl<'a>> {
        self.0[pos].as_ref()
    }
}
