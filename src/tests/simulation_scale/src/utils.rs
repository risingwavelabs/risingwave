pub trait AssertResultExt: AsRef<str> {
    #[track_caller]
    fn assert_result_eq(&self, other: impl AsRef<str>) {
        assert_eq!(self.as_ref().trim(), other.as_ref().trim());
    }

    #[track_caller]
    fn assert_result_ne(&self, other: impl AsRef<str>) {
        assert_ne!(self.as_ref().trim(), other.as_ref().trim());
    }
}

impl<S: AsRef<str>> AssertResultExt for S {}
