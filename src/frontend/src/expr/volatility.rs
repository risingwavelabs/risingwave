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
