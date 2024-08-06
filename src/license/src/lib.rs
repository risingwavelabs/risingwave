// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod feature;
mod manager;

pub use feature::*;
pub use key::*;
pub use manager::*;

mod key {
    use std::convert::Infallible;
    use std::str::FromStr;

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Copy, Deserialize)]
    #[serde(transparent)]
    pub struct LicenseKey<T = String>(pub(crate) T);

    pub type LicenseKeyRef<'a> = LicenseKey<&'a str>;

    impl<T> From<T> for LicenseKey<T> {
        fn from(t: T) -> Self {
            Self(t)
        }
    }

    impl<A, B> PartialEq<LicenseKey<B>> for LicenseKey<A>
    where
        A: AsRef<str>,
        B: AsRef<str>,
    {
        fn eq(&self, other: &LicenseKey<B>) -> bool {
            self.0.as_ref() == other.0.as_ref()
        }
    }

    impl<T: AsRef<str>> Eq for LicenseKey<T> {}

    impl<T: AsRef<str>> LicenseKey<T> {
        fn redact_str(&self) -> &str {
            let s = self.0.as_ref();
            if s.is_empty() {
                ""
            } else {
                "[REDACTED]"
            }
        }
    }

    impl<T: AsRef<str>> std::fmt::Debug for LicenseKey<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.redact_str())
        }
    }

    impl<T: AsRef<str>> std::fmt::Display for LicenseKey<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(self, f)
        }
    }

    impl<T: AsRef<str>> Serialize for LicenseKey<T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.redact_str().serialize(serializer)
        }
    }

    impl FromStr for LicenseKey {
        type Err = Infallible;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Ok(Self(s.to_owned()))
        }
    }

    impl<T: AsRef<str>> Into<String> for LicenseKey<T> {
        fn into(self) -> String {
            self.0.as_ref().to_owned()
        }
    }

    impl<'a> From<LicenseKeyRef<'a>> for LicenseKey {
        fn from(t: LicenseKeyRef<'a>) -> Self {
            Self(t.0.to_owned())
        }
    }

    impl LicenseKey {
        pub fn as_ref(&self) -> LicenseKeyRef<'_> {
            LicenseKey(self.0.as_ref())
        }
    }
}
