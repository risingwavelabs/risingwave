use risingwave_common::types::DataType;
pub use risingwave_pb::expr::expr_node::Type as ExprType;

pub use crate::expr::expr_rewriter::ExprRewriter;
pub use crate::expr::function_call::FunctionCall;
use crate::expr::{Expr, ExprImpl};

/// `SessionTimezone` will be used to resolve session
/// timezone-dependent casts, comparisons or arithmetic.
pub struct SessionTimezone {
    timezone: String,
    /// Whether or not the session timezone was used
    used: bool,
}

impl ExprRewriter for SessionTimezone {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if let Some(expr) = self.with_timezone(func_type, &inputs, ret.clone()) {
            self.used = true;
            expr
        } else {
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }
}

impl SessionTimezone {
    pub fn new(timezone: String) -> Self {
        Self {
            timezone,
            used: false,
        }
    }

    pub fn timezone(&self) -> String {
        self.timezone.clone()
    }

    pub fn used(&self) -> bool {
        self.used
    }

    pub fn warning(&self) -> Option<String> {
        if self.used {
            Some(format!(
                "Your session timezone is {}. It was used in the interpretation of timestamps and dates in your query. If this is unintended, \
                change your timezone to match that of your data's with `set timezone = [timezone]` or \
                rewrite your query with an explicit timezone conversion, e.g. with `AT TIME ZONE`.\n",
                self.timezone
            ))
        } else {
            None
        }
    }

    // Inlines conversions based on session timezone if required by the function
    fn with_timezone(
        &self,
        func_type: ExprType,
        inputs: &Vec<ExprImpl>,
        return_type: DataType,
    ) -> Option<ExprImpl> {
        match func_type {
            ExprType::Cast => {
                assert_eq!(inputs.len(), 1);
                let mut input = inputs[0].clone();
                let input_type = input.return_type();
                match (input_type, return_type.clone()) {
                    (DataType::Timestamptz, DataType::Varchar)
                    | (DataType::Varchar, DataType::Timestamptz) => {
                        Some(self.cast_with_timezone(input, return_type))
                    }
                    (DataType::Date, DataType::Timestamptz)
                    | (DataType::Timestamp, DataType::Timestamptz) => {
                        input = input.cast_explicit(DataType::Timestamp).unwrap();
                        Some(self.at_timezone(input))
                    }
                    (DataType::Timestamptz, DataType::Date)
                    | (DataType::Timestamptz, DataType::Time)
                    | (DataType::Timestamptz, DataType::Timestamp) => {
                        input = self.at_timezone(input);
                        input = input.cast_explicit(return_type).unwrap();
                        Some(input)
                    }
                    _ => None,
                }
            }
            // is cmp
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom => {
                assert_eq!(inputs.len(), 2);
                let mut inputs = inputs.clone();
                for idx in 0..2 {
                    if matches!(inputs[(idx + 1) % 2].return_type(), DataType::Timestamptz)
                        && matches!(
                            inputs[idx % 2].return_type(),
                            DataType::Date | DataType::Timestamp
                        )
                    {
                        let mut to_cast = inputs[idx % 2].clone();
                        // Cast to `Timestamp` first, then use `AT TIME ZONE` to convert to
                        // `Timestamptz`
                        to_cast = to_cast.cast_explicit(DataType::Timestamp).unwrap();
                        inputs[idx % 2] = self.at_timezone(to_cast);
                        return Some(
                            FunctionCall::new_unchecked(func_type, inputs, return_type).into(),
                        );
                    }
                }
                None
            }
            // TODO: handle tstz-related arithmetic with timezone
            ExprType::Add | ExprType::Subtract => None,
            _ => None,
        }
    }

    fn at_timezone(&self, input: ExprImpl) -> ExprImpl {
        FunctionCall::new(
            ExprType::AtTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
        )
        .unwrap()
        .into()
    }

    fn cast_with_timezone(&self, input: ExprImpl, return_type: DataType) -> ExprImpl {
        FunctionCall::new_unchecked(
            ExprType::CastWithTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
            return_type,
        )
        .into()
    }
}
