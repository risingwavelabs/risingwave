pub trait AssertResult: AsRef<str> {
    #[track_caller]
    fn assert_result_eq(&self, other: impl AsRef<str>) -> &Self {
        assert_eq!(self.as_ref().trim(), other.as_ref().trim());
        self
    }

    #[track_caller]
    fn assert_result_ne(&self, other: impl AsRef<str>) -> &Self {
        assert_ne!(self.as_ref().trim(), other.as_ref().trim());
        self
    }
}

impl<S: AsRef<str>> AssertResult for S {}
