use std::ops::Deref;

use risingwave_pb::meta::PbSystemParams;

use super::reader::{SystemParamsRead, SystemParamsReader};
use crate::for_all_params;

pub struct SystemParamsDiff {
    diff: PbSystemParams,
}

impl Deref for SystemParamsDiff {
    type Target = PbSystemParams;

    fn deref(&self) -> &Self::Target {
        &self.diff
    }
}

impl SystemParamsDiff {
    pub fn diff(prev: impl SystemParamsRead, curr: impl SystemParamsRead) -> Self {
        let mut diff = PbSystemParams::default();

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

        Self { diff }
    }

    pub fn new(diff: PbSystemParams) -> Self {
        Self { diff }
    }

    pub fn from_initial(initial: impl SystemParamsRead) -> Self {
        Self::diff(SystemParamsReader::default(), initial)
    }
}
