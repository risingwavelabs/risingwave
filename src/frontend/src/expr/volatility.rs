use super::{ExprImpl, ExprVisitor};

/// describe the "volatility" for all expressions. see https://www.postgresql.org/docs/current/xfunc-volatility.html and https://github.com/risingwavelabs/risingwave/issues/9030 for more information.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Volatility {
    VOLATILE,
    // we do not distinguish the VOLATILE and STABLE function
    // STABLE,
    IMMUTABLE,
}

impl Default for Volatility {
    fn default() -> Self {
        Volatility::IMMUTABLE
    }
}
struct VolatilityAnalyzer {}

impl ExprVisitor<Volatility> for VolatilityAnalyzer {
    fn merge(a: Volatility, b: Volatility) -> Volatility {
        if a == Volatility::VOLATILE || b == Volatility::VOLATILE {
            return Volatility::VOLATILE;
        }
        Volatility::IMMUTABLE
    }

    fn visit_user_defined_function(
        &mut self,
        _func_call: &super::UserDefinedFunction,
    ) -> Volatility {
        Volatility::VOLATILE
    }
}

pub fn derive_volatility(expr: &ExprImpl) -> Volatility {
    let mut a = VolatilityAnalyzer {};
    a.visit_expr(expr)
}
