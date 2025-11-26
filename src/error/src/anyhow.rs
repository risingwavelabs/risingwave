// Copyright 2025 RisingWave Labs
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
/// # Usage
///
/// ```ignore
/// def_anyhow_newtype! {
///    /// Documentation for the newtype.
///    #[derive(..)]
///    pub MyError,
///
///    // Default context messages for each source error type goes below.
///    mysql::Error => "failed to interact with MySQL",
///    postgres::Error => "failed to interact with PostgreSQL",
///    opendal::Error => transparent, // if it's believed to be self-explanatory
///                                   // and any context is not necessary
/// }
/// ```
///
/// # Construction
///
/// Unlike [`anyhow::Error`], the newtype **CANNOT** be converted from any error
/// types implicitly. Instead, it can only be converted from [`anyhow::Error`]
/// by default.
///
/// - Users are encouraged to use [`anyhow::Context`] to attach detailed
///   information to the source error and make it an [`anyhow::Error`] before
///   converting it to the newtype.
///
/// - Otherwise, specify the default context for each source error type as shown
///   in the example above, which will be expanded into a `From` implementation
///   from the source error type to the newtype. This should **NOT** be preferred
///   in most cases since it's less informative than the ad-hoc context provided
///   with [`anyhow::Context`] at the call site, but it could still be useful
///   during refactoring, or if the source error type is believed to be
///   self-explanatory.
///
/// To construct a new error from scratch, one can still use macros like
/// `anyhow::anyhow!` or `risingwave_common::bail!`. Since `bail!` and `?`
/// already imply an `into()` call, developers do not need to care about the
/// type conversion most of the time.
///
/// ## Example
///
/// ```ignore
/// fn read_offset_from_mysql() -> Result<String, mysql::Error> {
///     ..
/// }
/// fn parse_offset(offset: &str) -> Result<i64, ParseIntError> {
///     ..
/// }
///
/// fn work() -> Result<(), MyError> {
///     // `mysql::Error` can be converted to `MyError` implicitly with `?`
///     // as the default context is provided in the definition.
///     let offset = read_offset_from_mysql()?;
///
///     // Instead, `ParseIntError` cannot be directly converted to `MyError`
///     // with `?`, so the caller must attach context explicitly.
///     //
///     // This makes sense as the semantics of the integer ("offset") holds
///     // important information and are not implied by the error type itself.
///     let offset = parse_offset(&offset).context("failed to parse offset")?;
///
///     if offset < 0 {
///         // Construct a new error with `bail!` macro.
///         bail!("offset `{}` must be non-negative", offset);
///     }
/// }
/// ```
///
/// # Discussion
///
/// - What's the purpose of the newtype?
///   * It is to provide extra type information for errors, which makes it
///     clearer to identify which module or crate the error comes from when
///     it is passed around.
///   * It enforces the developer to attach context (explicitly or by default)
///     when doing type conversion, which makes the error more informative.
///
/// - Is the effect essentially the same as `thiserror`?
///   * Yes, but we're here intentionally making the error type less actionable
///     to make it informative with no fear.
///   * To elaborate, consider the following `thiserror` example:
///     ```ignore
///     #[derive(thiserror::Error, Debug)]
///     pub enum MyError {
///         #[error("failed to interact with MySQL")]
///         MySql(#[from] mysql::Error),
///         #[error(transparent)]
///         Other(#[from] anyhow::Error),
///     }
///     ```
///     This gives the caller an illusion that all errors related to MySQL are
///     under the `MySql` variant, which is not true as one could attach context
///     to an `mysql::Error` with [`anyhow::Context`] and make it go into the
///     `Other` variant.
///
///     By doing type erasure with `anyhow`, we're making it clear that the
///     error is not actionable so such confusion is avoided.
#[macro_export]
macro_rules! def_anyhow_newtype {
    (@from $error:ident transparent) => {
        Self(::anyhow::Error::new($error))
    };
    (@from $error:ident $context:literal) => {
        Self(::anyhow::Error::new($error).context($context))
    };

    (
        $(#[$attr:meta])* $vis:vis $name:ident
        $(, $(#[$attr_inner:meta])* $from:ty => $context:tt)* $(,)?
    ) => {
        #[derive(::thiserror::Error, ::std::fmt::Debug)]
        #[error(transparent)]
        $(#[$attr])* $vis struct $name(#[from] #[backtrace] pub ::anyhow::Error);

        impl $name {
            /// Unwrap the newtype to get the inner [`anyhow::Error`].
            pub fn into_inner(self) -> ::anyhow::Error {
                self.0
            }

            /// Get the type name of the inner error.
            pub fn variant_name(&self) -> &'static str {
                $(
                    $(#[$attr_inner])*
                    if self.0.downcast_ref::<$from>().is_some() {
                        return stringify!($from);
                    }
                )*
                return stringify!($name);
            }
        }

        $(
            $(#[$attr_inner])*
            impl From<$from> for $name {
                fn from(error: $from) -> Self {
                    def_anyhow_newtype!(@from error $context)
                }
            }
        )*
    };
}

/// Define a newtype + it's variant in the specified type.
/// This is useful when you want to define a new error type,
/// but also want to define a variant for it in another enum.
#[macro_export]
macro_rules! def_anyhow_variant {
    (
        $(#[$attr:meta])* $vis:vis $name:ident,
        $enum_name:ident $variant_name:ident
        $(, $from:ty => $context:tt)* $(,)?
    ) => {
        def_anyhow_newtype! {
            $(#[$attr])* $vis $name
            $(, $from => $context)*
        }

        $(
            impl From<$from> for $enum_name {
                fn from(error: $from) -> Self {
                    $enum_name::$variant_name($name::from(error))
                }
            }
        )*
    }
}

pub use {def_anyhow_newtype, def_anyhow_variant};
