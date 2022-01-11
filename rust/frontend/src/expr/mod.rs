use risingwave_pb::data::DataType;
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
/// the trait of bound exprssions
pub trait BoundExpr {
    fn return_type(&self) -> DataType;
}
#[derive(Clone, Debug)]
pub enum BoundExprImpl {
    // ColumnRef(Box<BoundColumnRef>), might be used in binder.
    InputRef(Box<BoundInputRef>),
    Literal(Box<BoundLiteral>),
    FunctionCall(Box<BoundFunctionCall>),
    AggCall(Box<BoundAggCall>),
}
impl BoundExpr for BoundExprImpl {
    fn return_type(&self) -> DataType {
        match self {
            BoundExprImpl::InputRef(expr) => expr.return_type(),
            BoundExprImpl::Literal(expr) => expr.return_type(),
            BoundExprImpl::FunctionCall(expr) => expr.return_type(),
            BoundExprImpl::AggCall(expr) => expr.return_type(),
        }
    }
}
