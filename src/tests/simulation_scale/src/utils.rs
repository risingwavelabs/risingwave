pub trait AssertResult: AsRef<str> {
    /// Asserts that the result is equal to the expected result after trimming whitespace.
    #[track_caller]
    fn assert_result_eq(&self, other: impl AsRef<str>) -> &Self {
        assert_eq!(self.as_ref().trim(), other.as_ref().trim());
        self
    }

    /// Asserts that the result is not equal to the expected result after trimming whitespace.
    #[track_caller]
    fn assert_result_ne(&self, other: impl AsRef<str>) -> &Self {
        assert_ne!(self.as_ref().trim(), other.as_ref().trim());
        self
    }
}

impl<S: AsRef<str>> AssertResult for S {}
