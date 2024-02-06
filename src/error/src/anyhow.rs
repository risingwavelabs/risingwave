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

/// Define a newtype wrapper around [`anyhow::Error`].
///
/// # Motivation
///
/// [`anyhow::Error`] is good enough if one just wants to make the error type
/// informative but not necessarily actionable. However, given a value of type
/// [`anyhow::Error`], it is hard to tell which module or crate it comes from,
/// which may blur the boundary between modules when passing it around, leading
/// to abuse.
///
/// # Usage
///
/// ```ignore
/// def_anyhow_newtype! {
///    /// Documentation for the newtype.
///    #[derive(..)]
///    pub MyError;
/// }
/// ```
///
/// This will define a newtype `MyError` around [`anyhow::Error`].
///
/// The newtype can be converted from any type that implements `Into<anyhow::Error>`,
/// so the developing experience is kept almost the same. To construct a new error,
/// one can still use macros like `anyhow::anyhow!` or `risingwave_common::bail!`.
///
/// Since `bail!` and `?` already imply an `into()` call, developers do not need to
/// care about the conversion from [`anyhow::Error`] to the newtype most of the time.
///
/// # Limitation
///
/// Note that the newtype does not implement [`std::error::Error`] just like
/// [`anyhow::Error`]. However, it can be dereferenced to `dyn std::error::Error`
/// to be used in places like `thiserror`'s `#[source]` attribute.
#[macro_export]
macro_rules! def_anyhow_newtype {
    ($(#[$attr:meta])* $vis:vis $name:ident $(;)?) => {
        $(#[$attr])* $vis struct $name(::anyhow::Error);

        impl std::ops::Deref for $name {
            type Target = ::anyhow::Error;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }

        impl<E> From<E> for $name
        where
            E: Into<::anyhow::Error>,
        {
            fn from(value: E) -> Self {
                Self(value.into())
            }
        }

        impl From<$name> for Box<dyn std::error::Error + Send + Sync + 'static> {
            fn from(value: $name) -> Self {
                value.0.into()
            }
        }
    };
}
