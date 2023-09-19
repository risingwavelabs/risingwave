// Copyright 2023 RisingWave Labs
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

use risingwave_expr::ExprError;

pub(super) struct ContextUnavailable(&'static str);

impl From<ContextUnavailable> for ExprError {
    fn from(e: ContextUnavailable) -> Self {
        ExprError::Context(e.0)
    }
}

macro_rules! define_context {
    ($($name:ident : $ty:ty),*) => {
        mod local_keys {
            tokio::task_local! {
                $(
                    pub(super) static $name: $ty
                ),*
            }
        }

        $(
            // The struct is similar to a global variable, so we use upper case here.
            #[allow(non_camel_case_types)]
            pub struct $name;

            impl $name {
                /// A simple wrapper around [`LocalKey::try_with`], and only the `function_impl` mod can access the inner value.
                pub(super) fn try_with<F, R>(self, f: F) -> Result<R, ContextUnavailable>
                where
                    F: FnOnce(&$ty) -> R
                {
                    local_keys::$name.try_with(f).map_err(|_| ContextUnavailable(stringify!($name)))
                }

                pub fn scope<F>(self, value: $ty, f: F) -> tokio::task::futures::TaskLocalFuture<$ty, F>
                where
                    F: std::future::Future
                {
                    local_keys::$name.scope(value, f)
                }
            }
        )*
    };
}

define_context! {
    CATALOG_READER: crate::catalog::CatalogReader
}
