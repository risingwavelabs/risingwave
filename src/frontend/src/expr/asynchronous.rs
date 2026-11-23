// Copyright 2026 RisingWave Labs
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

use risingwave_expr::sig::FUNCTION_REGISTRY;

use super::{
    Expr, ExprImpl, ExprVisitor, FunctionCall, FunctionCallWithLambda, UserDefinedFunction,
    default_visit_expr,
};

#[derive(Default)]
struct AsyncAnalyzer(bool);

impl ExprVisitor for AsyncAnalyzer {
    fn visit_expr(&mut self, expr: &ExprImpl) {
        if !self.0 {
            default_visit_expr(self, expr);
        }
    }

    fn visit_function_call(&mut self, call: &FunctionCall) {
        let args = call
            .inputs()
            .iter()
            .map(Expr::return_type)
            .collect::<Vec<_>>();
        // SOME/ALL are wrappers built outside the registry; their children determine their mode.
        self.0 |= FUNCTION_REGISTRY
            .get(call.func_type(), &args, &call.return_type())
            .is_ok_and(|sig| sig.is_async);
        if self.0 {
            return;
        }
        for input in call.inputs() {
            self.visit_expr(input);
            if self.0 {
                break;
            }
        }
    }

    fn visit_function_call_with_lambda(&mut self, call: &FunctionCallWithLambda) {
        self.visit_function_call(&call.to_full_function_call());
    }

    fn visit_user_defined_function(&mut self, _: &UserDefinedFunction) {
        // All scalar UDF runtimes implement AsyncExpression, including local language runtimes.
        self.0 = true;
    }
}

impl ExprImpl {
    /// Whether evaluating this scalar expression may suspend. Keep the whole tree together:
    /// extracting async children from CASE/COALESCE/AND/OR would change conditional evaluation.
    pub fn is_async(&self) -> bool {
        let mut analyzer = AsyncAnalyzer::default();
        analyzer.visit_expr(self);
        analyzer.0
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, JsonbVal, StructType};
    use risingwave_expr::expr::{BoxedExpression, build_from_prost};

    use super::*;
    use crate::expr::{ExprType, InputRef};

    #[test]
    fn ai_decide_and_its_lazy_parent_are_async() {
        let result_type: DataType = StructType::new([("q", DataType::Float64)]).into();
        let decide: ExprImpl = FunctionCall::new_unchecked(
            ExprType::AiDecide,
            vec![
                InputRef::new(0, DataType::Jsonb).into(),
                ExprImpl::literal_varchar("http://127.0.0.1:1/evaluate".into()),
                ExprImpl::literal_varchar("test-model".into()),
                ExprImpl::literal_jsonb(JsonbVal::from(serde_json::json!({
                    "q": {"type": "noul", "instructions": "Resolved?"}
                }))),
                ExprImpl::literal_varchar("test-key".into()),
            ],
            result_type.clone(),
        )
        .into();
        let conditional = FunctionCall::new_unchecked(
            ExprType::Case,
            vec![
                InputRef::new(1, DataType::Boolean).into(),
                decide.clone(),
                ExprImpl::literal_null(result_type.clone()),
            ],
            result_type,
        )
        .into();
        for expr in [decide, conditional] {
            assert!(expr.is_async());
            assert!(matches!(
                build_from_prost(&expr.to_expr_proto()).unwrap(),
                BoxedExpression::Async(_)
            ));
        }
    }

    #[test]
    fn classification_matches_built_expressions() {
        let sleep: ExprImpl = FunctionCall::new_unchecked(
            ExprType::PgSleep,
            vec![ExprImpl::literal_f64(0.0)],
            DataType::Int32,
        )
        .into();
        let nested: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Case,
            vec![
                ExprImpl::literal_bool(false),
                sleep.clone(),
                ExprImpl::literal_int(1),
            ],
            DataType::Int32,
        )
        .into();
        let sync: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Add,
            vec![
                InputRef::new(0, DataType::Int32).into(),
                ExprImpl::literal_int(1),
            ],
            DataType::Int32,
        )
        .into();
        for expr in [sleep, nested, sync] {
            let built = build_from_prost(&expr.to_expr_proto()).unwrap();
            assert_eq!(expr.is_async(), matches!(built, BoxedExpression::Async(_)));
        }

        let embedding: ExprImpl = FunctionCall::new_unchecked(
            ExprType::OpenaiEmbedding,
            vec![
                ExprImpl::literal_null(DataType::Jsonb),
                ExprImpl::literal_varchar("text".into()),
            ],
            DataType::Float32.list(),
        )
        .into();
        assert!(embedding.is_async());
        let udf: ExprImpl = UserDefinedFunction::from_expr_proto(
            &risingwave_pb::expr::UserDefinedFunction {
                name: "test_udf".into(),
                ..Default::default()
            },
            DataType::Int32,
        )
        .unwrap()
        .into();
        assert!(udf.is_async());
    }
}
