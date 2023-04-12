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

use super::{ExprImpl, ExprVisitor};

/// describe the "volatility" for all expressions. see <https://www.postgresql.org/docs/current/xfunc-volatility.html> and <https://github.com/risingwavelabs/risingwave/issues/9030> for more information.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Volatility {
    Volatile,
    // we do not distinguish the VOLATILE and STABLE function
    // STABLE,
    #[default]
    Immutable,
}

struct VolatilityAnalyzer {}

impl ExprVisitor<Volatility> for VolatilityAnalyzer {
    fn merge(a: Volatility, b: Volatility) -> Volatility {
        if a == Volatility::Volatile || b == Volatility::Volatile {
            return Volatility::Volatile;
        }
        Volatility::Immutable
    }

    fn visit_user_defined_function(
        &mut self,
        _func_call: &super::UserDefinedFunction,
    ) -> Volatility {
        Volatility::Volatile
    }
}

pub fn derive_volatility(expr: &ExprImpl) -> Volatility {
    let mut a = VolatilityAnalyzer {};
    a.visit_expr(expr)
}
