use super::reader::SystemParamsRead;
use crate::for_all_params;

macro_rules! define_diff {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        /// The diff of the system params.
        ///
        /// Fields that are changed are set to `Some`, otherwise `None`.
        #[derive(Default, Debug, Clone)]
        pub struct SystemParamsDiff {
            $(
                #[doc = $doc]
                pub $field: Option<$type>,
            )*
        }
    }
}
for_all_params!(define_diff);

impl SystemParamsDiff {
    /// Create a diff between the given two system params.
    pub fn diff(prev: impl SystemParamsRead, curr: impl SystemParamsRead) -> Self {
        let mut diff = Self::default();

        macro_rules! set_diff_field {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                $(
                    if curr.$field() != prev.$field() {
                        diff.$field = Some(curr.$field().to_owned());
                    }
                )*
            };
        }
        for_all_params!(set_diff_field);

        diff
    }

    /// Create a diff from the given initial system params.
    /// All fields will be set to `Some`.
    pub fn from_initial(initial: impl SystemParamsRead) -> Self {
        macro_rules! initial_field {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                Self {
                    $(
                        $field: Some(initial.$field().to_owned()),
                    )*
                }
            };
        }
        for_all_params!(initial_field)
    }
}
