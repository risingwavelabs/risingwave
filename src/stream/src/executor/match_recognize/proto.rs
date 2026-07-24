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

//! Decode the structured row-pattern proto into the executor-side [`Pattern`]. The frontend lowers
//! the SQL `PATTERN` clause directly into this proto tree, so there is no textual round-trip.

use risingwave_pb::stream_plan::match_recognize_pattern_node::Node;
use risingwave_pb::stream_plan::match_recognize_quantifier::Kind;
use risingwave_pb::stream_plan::{MatchRecognizePatternNode, MatchRecognizeQuantifier};

use super::nfa::{Pattern, Quantifier};

/// Build a [`Pattern`] from its protobuf representation.
pub fn pattern_from_protobuf(pb: &MatchRecognizePatternNode) -> Result<Pattern, String> {
    let node = pb
        .node
        .as_ref()
        .ok_or_else(|| "empty MATCH_RECOGNIZE pattern node".to_owned())?;
    Ok(match node {
        Node::Var(v) => Pattern::Var(v.clone()),
        Node::Concat(seq) => Pattern::Concat(patterns_from_protobuf(&seq.patterns)?),
        Node::Alternation(seq) => Pattern::Alt(patterns_from_protobuf(&seq.patterns)?),
        Node::Permute(p) => Pattern::Permute(p.vars.clone()),
        Node::Quantified(q) => {
            let inner = q
                .inner
                .as_ref()
                .ok_or_else(|| "quantified pattern missing inner".to_owned())?;
            let quantifier = quantifier_from_protobuf(
                q.quantifier
                    .as_ref()
                    .ok_or_else(|| "quantified pattern missing quantifier".to_owned())?,
            )?;
            Pattern::Quantified(
                Box::new(pattern_from_protobuf(inner)?),
                quantifier,
                q.reluctant,
            )
        }
    })
}

fn patterns_from_protobuf(patterns: &[MatchRecognizePatternNode]) -> Result<Vec<Pattern>, String> {
    patterns.iter().map(pattern_from_protobuf).collect()
}

fn quantifier_from_protobuf(q: &MatchRecognizeQuantifier) -> Result<Quantifier, String> {
    Ok(match q.kind() {
        Kind::Star => Quantifier::Star,
        Kind::Plus => Quantifier::Plus,
        Kind::Question => Quantifier::Question,
        Kind::Range => Quantifier::Range {
            min: q.min,
            max: q.max,
        },
        Kind::Unspecified => {
            return Err("unspecified MATCH_RECOGNIZE quantifier kind".to_owned());
        }
    })
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::match_recognize_pattern_node::Node;
    use risingwave_pb::stream_plan::{
        MatchRecognizePatternNode, MatchRecognizePatternSeq, MatchRecognizePermutePattern,
        MatchRecognizeQuantifiedPattern, MatchRecognizeQuantifier,
    };

    use super::*;

    fn var(name: &str) -> MatchRecognizePatternNode {
        MatchRecognizePatternNode {
            node: Some(Node::Var(name.to_owned())),
        }
    }

    fn quantifier(kind: Kind, min: u32, max: Option<u32>) -> MatchRecognizeQuantifier {
        MatchRecognizeQuantifier {
            kind: kind as i32,
            min,
            max,
        }
    }

    fn quantified(
        inner: MatchRecognizePatternNode,
        kind: Kind,
        min: u32,
        max: Option<u32>,
        reluctant: bool,
    ) -> MatchRecognizePatternNode {
        MatchRecognizePatternNode {
            node: Some(Node::Quantified(Box::new(
                MatchRecognizeQuantifiedPattern {
                    inner: Some(Box::new(inner)),
                    quantifier: Some(quantifier(kind, min, max)),
                    reluctant,
                },
            ))),
        }
    }

    fn concat(patterns: Vec<MatchRecognizePatternNode>) -> MatchRecognizePatternNode {
        MatchRecognizePatternNode {
            node: Some(Node::Concat(MatchRecognizePatternSeq { patterns })),
        }
    }

    fn alt(patterns: Vec<MatchRecognizePatternNode>) -> MatchRecognizePatternNode {
        MatchRecognizePatternNode {
            node: Some(Node::Alternation(MatchRecognizePatternSeq { patterns })),
        }
    }

    #[test]
    fn decode_concat() {
        assert_eq!(
            pattern_from_protobuf(&concat(vec![var("a"), var("b"), var("c")])).unwrap(),
            Pattern::Concat(vec![
                Pattern::Var("a".to_owned()),
                Pattern::Var("b".to_owned()),
                Pattern::Var("c".to_owned()),
            ])
        );
    }

    #[test]
    fn decode_quantifiers() {
        assert_eq!(
            pattern_from_protobuf(&concat(vec![
                var("a"),
                quantified(var("b"), Kind::Plus, 0, None, false),
                quantified(var("c"), Kind::Question, 0, None, false),
            ]))
            .unwrap(),
            Pattern::Concat(vec![
                Pattern::Var("a".to_owned()),
                Pattern::Quantified(
                    Box::new(Pattern::Var("b".to_owned())),
                    Quantifier::Plus,
                    false
                ),
                Pattern::Quantified(
                    Box::new(Pattern::Var("c".to_owned())),
                    Quantifier::Question,
                    false
                ),
            ])
        );
        assert_eq!(
            pattern_from_protobuf(&quantified(var("a"), Kind::Star, 0, None, true)).unwrap(),
            Pattern::Quantified(
                Box::new(Pattern::Var("a".to_owned())),
                Quantifier::Star,
                true
            )
        );
    }

    #[test]
    fn decode_alternation_and_range() {
        assert_eq!(
            pattern_from_protobuf(&concat(vec![
                alt(vec![var("a"), var("b")]),
                quantified(var("c"), Kind::Range, 1, Some(3), false),
            ]))
            .unwrap(),
            Pattern::Concat(vec![
                Pattern::Alt(vec![
                    Pattern::Var("a".to_owned()),
                    Pattern::Var("b".to_owned())
                ]),
                Pattern::Quantified(
                    Box::new(Pattern::Var("c".to_owned())),
                    Quantifier::Range {
                        min: 1,
                        max: Some(3)
                    },
                    false
                ),
            ])
        );
    }

    #[test]
    fn decode_permute() {
        assert_eq!(
            pattern_from_protobuf(&MatchRecognizePatternNode {
                node: Some(Node::Permute(MatchRecognizePermutePattern {
                    vars: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
                })),
            })
            .unwrap(),
            Pattern::Permute(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
    }

    #[test]
    fn rejects_empty_node() {
        assert!(pattern_from_protobuf(&MatchRecognizePatternNode { node: None }).is_err());
    }
}
