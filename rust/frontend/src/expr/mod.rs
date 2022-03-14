use risingwave_common::types::{DataType, Scalar};
mod input_ref;
pub use input_ref::*;
mod literal;
pub use literal::*;
mod function_call;
pub use function_call::*;
mod agg_call;
pub use agg_call::*;
mod type_inference;
pub use type_inference::*;
mod utils;
pub use utils::*;
mod expr_rewriter;
pub use expr_rewriter::*;
mod expr_visitor;
pub use expr_visitor::*;
pub type ExprType = risingwave_pb::expr::expr_node::Type;

/// the trait of bound exprssions
pub trait Expr: Into<ExprImpl> {
    fn return_type(&self) -> DataType;
}

#[derive(Clone, PartialEq)]
pub enum ExprImpl {
    // ColumnRef(Box<BoundColumnRef>), might be used in binder.
    InputRef(Box<InputRef>),
    Literal(Box<Literal>),
    FunctionCall(Box<FunctionCall>),
    AggCall(Box<AggCall>),
}

impl ExprImpl {
    /// A literal int value.
    #[inline(always)]
    #[allow(dead_code)]
    pub fn literal_int(v: i32) -> Self {
        Self::Literal(Box::new(Literal::new(
            Some(v.to_scalar_value()),
            DataType::Int32,
        )))
    }

    /// A literal boolean value.
    #[inline(always)]
    pub fn literal_bool(v: bool) -> Self {
        Self::Literal(Box::new(Literal::new(
            Some(v.to_scalar_value()),
            DataType::Boolean,
        )))
    }
}

impl Expr for ExprImpl {
    fn return_type(&self) -> DataType {
        match self {
            ExprImpl::InputRef(expr) => expr.return_type(),
            ExprImpl::Literal(expr) => expr.return_type(),
            ExprImpl::FunctionCall(expr) => expr.return_type(),
            ExprImpl::AggCall(expr) => expr.return_type(),
        }
    }
}

impl From<InputRef> for ExprImpl {
    fn from(input_ref: InputRef) -> Self {
        ExprImpl::InputRef(Box::new(input_ref))
    }
}

impl From<Literal> for ExprImpl {
    fn from(literal: Literal) -> Self {
        ExprImpl::Literal(Box::new(literal))
    }
}

impl From<FunctionCall> for ExprImpl {
    fn from(func_call: FunctionCall) -> Self {
        ExprImpl::FunctionCall(Box::new(func_call))
    }
}

impl From<AggCall> for ExprImpl {
    fn from(agg_call: AggCall) -> Self {
        ExprImpl::AggCall(Box::new(agg_call))
    }
}

/// A custom Debug implementation that is more concise and suitable to use with
/// [`std::fmt::Formatter::debug_list`] in plan nodes. If the verbose output is preferred, it is
/// still available via `{:#?}`.
impl std::fmt::Debug for ExprImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return match self {
                Self::InputRef(arg0) => f.debug_tuple("InputRef").field(arg0).finish(),
                Self::Literal(arg0) => f.debug_tuple("Literal").field(arg0).finish(),
                Self::FunctionCall(arg0) => f.debug_tuple("FunctionCall").field(arg0).finish(),
                Self::AggCall(arg0) => f.debug_tuple("AggCall").field(arg0).finish(),
            };
        }
        match self {
            Self::InputRef(input_ref) => write!(f, "${}", input_ref.index()),
            Self::Literal(literal) => {
                use risingwave_common::for_all_scalar_variants;
                use risingwave_common::types::ScalarImpl::*;
                macro_rules! scalar_write_inner {
                    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                        match literal.get_data() {
                            None => write!(f, "null"),
                            $( Some($variant_name(v)) => write!(f, "{:?}", v) ),*
                        }?;
                    };
                }
                for_all_scalar_variants! { scalar_write_inner }
                write!(f, ":{:?}", literal.return_type())
            }
            Self::FunctionCall(func_call) => {
                if let ExprType::Cast = func_call.get_expr_type() {
                    func_call.inputs()[0].fmt(f)?;
                    return write!(f, "::{:?}", func_call.return_type());
                }
                let func_name = format!("{:?}", func_call.get_expr_type());
                let mut builder = f.debug_tuple(&func_name);
                func_call.inputs().iter().for_each(|child| {
                    builder.field(child);
                });
                builder.finish()
            }
            Self::AggCall(agg_call) => {
                let agg_name = format!("{:?}", agg_call.agg_kind());
                let mut builder = f.debug_tuple(&agg_name);
                agg_call.inputs().iter().for_each(|child| {
                    builder.field(child);
                });
                builder.finish()
            }
        }
    }
}

#[cfg(test)]
/// Asserts that the expression is an [`InputRef`] with the given index.
macro_rules! assert_eq_input_ref {
    ($e:expr, $index:expr) => {
        match $e {
            ExprImpl::InputRef(i) => assert_eq!(i.index(), $index),
            _ => assert!(false, "Expected input ref, found {:?}", $e),
        }
    };
}

#[cfg(test)]
pub(crate) use assert_eq_input_ref;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_debug_alternate() {
        let mut e = InputRef::new(1, DataType::Boolean).into();
        e = FunctionCall::new(ExprType::Not, vec![e]).unwrap().into();
        let s = format!("{:#?}", e);
        assert!(s.contains("return_type: Boolean"))
    }
}
