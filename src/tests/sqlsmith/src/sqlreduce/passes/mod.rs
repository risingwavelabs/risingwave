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

use std::fmt;

use risingwave_sqlparser::ast::Statement;

pub mod pullup;
pub mod remove;
pub mod replace;

pub type AST = Statement;

#[derive(Debug, Clone)]
pub enum Strategy {
    Single,             // one-at-a-time
    Aggressive,         // all combinations
    Consecutive(usize), // delete k consecutive elements
}

pub trait Transform: Send + Sync {
    fn name(&self) -> String;

    fn get_reduction_points(&self, ast: AST) -> Vec<usize>;

    fn apply_on(&self, ast: &mut AST, reduction_points: Vec<usize>) -> AST;

    fn transform(&self, ast: AST, idx: usize, strategy: Strategy) -> Vec<(AST, usize)> {
        let reduction_points = self.get_reduction_points(ast.clone());

        match strategy {
            Strategy::Single => {
                let mut results = Vec::new();
                for (i, rp) in reduction_points.iter().enumerate() {
                    if i < idx {
                        continue;
                    }
                    let new_ast = self.apply_on(&mut ast.clone(), vec![*rp]);
                    results.push((new_ast, i));
                }
                results
            }
            Strategy::Aggressive => {
                let mut results = Vec::new();
                let new_ast = self.apply_on(&mut ast.clone(), reduction_points[idx..].to_vec());
                results.push((new_ast, idx));
                results
            }
            Strategy::Consecutive(k) => {
                let mut results = Vec::new();
                if reduction_points.len() >= k {
                    for i in 0..=reduction_points.len() - k {
                        let new_ast =
                            self.apply_on(&mut ast.clone(), reduction_points[i..i + k].to_vec());
                        results.push((new_ast, i));
                    }
                }
                results
            }
        }
    }
}

impl fmt::Display for dyn Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
