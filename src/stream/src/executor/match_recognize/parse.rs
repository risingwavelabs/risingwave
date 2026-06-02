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

//! Parse a row pattern from its textual form (the `MatchRecognizePattern` Display used on the wire)
//! back into the executor-side [`Pattern`].
//!
//! TODO: the proto currently carries the pattern as a string and this re-parses it. A structured
//! pattern proto message would be cleaner and avoid the Display↔parse coupling — flagged for the
//! upstream design discussion. The grammar here mirrors the frontend `MatchRecognizePattern` Display.

use super::nfa::{Pattern, Quantifier};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Permute,
    LParen,
    RParen,
    Pipe,
    Star,
    Plus,
    Question,
    LBrace,
    RBrace,
    Comma,
    Num(u32),
}

fn tokenize(s: &str) -> Result<Vec<Tok>, String> {
    let mut toks = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\n' => i += 1,
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            '|' => {
                toks.push(Tok::Pipe);
                i += 1;
            }
            '*' => {
                toks.push(Tok::Star);
                i += 1;
            }
            '+' => {
                toks.push(Tok::Plus);
                i += 1;
            }
            '?' => {
                toks.push(Tok::Question);
                i += 1;
            }
            '{' => {
                toks.push(Tok::LBrace);
                i += 1;
            }
            '}' => {
                toks.push(Tok::RBrace);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let n: u32 = chars[start..i]
                    .iter()
                    .collect::<String>()
                    .parse()
                    .map_err(|_| "invalid number in quantifier".to_owned())?;
                toks.push(Tok::Num(n));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                if word.eq_ignore_ascii_case("PERMUTE") {
                    toks.push(Tok::Permute);
                } else {
                    toks.push(Tok::Ident(word));
                }
            }
            other => return Err(format!("unexpected character '{}' in pattern", other)),
        }
    }
    Ok(toks)
}

struct PParser {
    toks: Vec<Tok>,
    pos: usize,
}

impl PParser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, t: &Tok) -> Result<(), String> {
        if self.peek() == Some(t) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected {:?}, found {:?}", t, self.peek()))
        }
    }

    /// alternation := concat ( '|' concat )*
    fn parse_alternation(&mut self) -> Result<Pattern, String> {
        let mut alts = vec![self.parse_concat()?];
        while self.peek() == Some(&Tok::Pipe) {
            self.pos += 1;
            alts.push(self.parse_concat()?);
        }
        Ok(if alts.len() == 1 {
            alts.pop().unwrap()
        } else {
            Pattern::Alt(alts)
        })
    }

    /// concat := repetition+
    fn parse_concat(&mut self) -> Result<Pattern, String> {
        let mut parts = vec![self.parse_repetition()?];
        while matches!(
            self.peek(),
            Some(Tok::Ident(_)) | Some(Tok::LParen) | Some(Tok::Permute)
        ) {
            parts.push(self.parse_repetition()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            Pattern::Concat(parts)
        })
    }

    /// repetition := primary quantifier?
    fn parse_repetition(&mut self) -> Result<Pattern, String> {
        let primary = self.parse_primary()?;
        let q = match self.peek() {
            Some(Tok::Star) => {
                self.pos += 1;
                Some(Quantifier::Star)
            }
            Some(Tok::Plus) => {
                self.pos += 1;
                Some(Quantifier::Plus)
            }
            Some(Tok::Question) => {
                self.pos += 1;
                Some(Quantifier::Question)
            }
            Some(Tok::LBrace) => Some(self.parse_range()?),
            _ => None,
        };
        Ok(match q {
            Some(q) => Pattern::Quantified(Box::new(primary), q),
            None => primary,
        })
    }

    /// primary := Ident | '(' alternation ')' | PERMUTE '(' Ident (',' Ident)* ')'
    fn parse_primary(&mut self) -> Result<Pattern, String> {
        match self.next() {
            Some(Tok::Ident(name)) => Ok(Pattern::Var(name)),
            Some(Tok::LParen) => {
                let inner = self.parse_alternation()?;
                self.expect(&Tok::RParen)?;
                Ok(inner)
            }
            Some(Tok::Permute) => {
                self.expect(&Tok::LParen)?;
                let mut vars = Vec::new();
                loop {
                    match self.next() {
                        Some(Tok::Ident(name)) => vars.push(name),
                        other => return Err(format!("expected pattern variable, found {:?}", other)),
                    }
                    match self.next() {
                        Some(Tok::Comma) => continue,
                        Some(Tok::RParen) => break,
                        other => return Err(format!("expected ',' or ')', found {:?}", other)),
                    }
                }
                Ok(Pattern::Permute(vars))
            }
            other => Err(format!("expected a pattern primary, found {:?}", other)),
        }
    }

    /// range := '{' [n] [',' [m]] '}'  -> {n}, {n,}, {,m}, {n,m}
    fn parse_range(&mut self) -> Result<Quantifier, String> {
        self.expect(&Tok::LBrace)?;
        let min = if let Some(Tok::Num(n)) = self.peek() {
            let n = *n;
            self.pos += 1;
            Some(n)
        } else {
            None
        };
        let q = if self.peek() == Some(&Tok::Comma) {
            self.pos += 1;
            let max = if let Some(Tok::Num(m)) = self.peek() {
                let m = *m;
                self.pos += 1;
                Some(m)
            } else {
                None
            };
            Quantifier::Range {
                min: min.unwrap_or(0),
                max,
            }
        } else {
            // {n} exactly
            match min {
                Some(n) => Quantifier::Range {
                    min: n,
                    max: Some(n),
                },
                None => return Err("empty quantifier {}".to_owned()),
            }
        };
        self.expect(&Tok::RBrace)?;
        Ok(q)
    }
}

/// Parse a row pattern from its textual (Display) form into a [`Pattern`].
pub fn parse_pattern(s: &str) -> Result<Pattern, String> {
    let toks = tokenize(s)?;
    let mut p = PParser { toks, pos: 0 };
    let pat = p.parse_alternation()?;
    if p.pos != p.toks.len() {
        return Err(format!("trailing tokens after pattern: {:?}", &p.toks[p.pos..]));
    }
    Ok(pat)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn var(s: &str) -> Pattern {
        Pattern::Var(s.to_owned())
    }

    #[test]
    fn parse_concat() {
        assert_eq!(
            parse_pattern("a b c").unwrap(),
            Pattern::Concat(vec![var("a"), var("b"), var("c")])
        );
    }

    #[test]
    fn parse_quantifiers() {
        assert_eq!(
            parse_pattern("a b+ c?").unwrap(),
            Pattern::Concat(vec![
                var("a"),
                Pattern::Quantified(Box::new(var("b")), Quantifier::Plus),
                Pattern::Quantified(Box::new(var("c")), Quantifier::Question),
            ])
        );
        assert_eq!(
            parse_pattern("a*").unwrap(),
            Pattern::Quantified(Box::new(var("a")), Quantifier::Star)
        );
    }

    #[test]
    fn parse_alternation_and_group() {
        assert_eq!(
            parse_pattern("(a | b) c*").unwrap(),
            Pattern::Concat(vec![
                Pattern::Alt(vec![var("a"), var("b")]),
                Pattern::Quantified(Box::new(var("c")), Quantifier::Star),
            ])
        );
    }

    #[test]
    fn parse_ranges() {
        assert_eq!(
            parse_pattern("a{2}").unwrap(),
            Pattern::Quantified(
                Box::new(var("a")),
                Quantifier::Range { min: 2, max: Some(2) }
            )
        );
        assert_eq!(
            parse_pattern("a{1,3}").unwrap(),
            Pattern::Quantified(
                Box::new(var("a")),
                Quantifier::Range { min: 1, max: Some(3) }
            )
        );
        assert_eq!(
            parse_pattern("a{2,}").unwrap(),
            Pattern::Quantified(
                Box::new(var("a")),
                Quantifier::Range { min: 2, max: None }
            )
        );
        assert_eq!(
            parse_pattern("a{,4}").unwrap(),
            Pattern::Quantified(
                Box::new(var("a")),
                Quantifier::Range { min: 0, max: Some(4) }
            )
        );
    }

    #[test]
    fn parse_permute() {
        assert_eq!(
            parse_pattern("PERMUTE(a, b, c)").unwrap(),
            Pattern::Permute(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_pattern("a |").is_err());
        assert!(parse_pattern("(a b").is_err());
        assert!(parse_pattern("@").is_err());
    }
}
