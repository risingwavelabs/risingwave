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

//! Compatible layer with parser v1

use winnow::{ModalResult, Stateful};

use crate::parser as parser_v1;

pub trait ParseV1 {
    fn parse_v1<F, O>(&mut self, f: F) -> ModalResult<O>
    where
        for<'a> F: FnOnce(&mut parser_v1::Parser<'a>) -> ModalResult<O>;
}

impl<'a> ParseV1 for parser_v1::Parser<'a> {
    fn parse_v1<F, O>(&mut self, f: F) -> ModalResult<O>
    where
        F: FnOnce(&mut parser_v1::Parser<'a>) -> ModalResult<O>,
    {
        f(self)
    }
}

impl<S, State> ParseV1 for Stateful<S, State>
where
    S: ParseV1,
{
    fn parse_v1<F, O>(&mut self, f: F) -> ModalResult<O>
    where
        for<'a> F: FnOnce(&mut parser_v1::Parser<'a>) -> ModalResult<O>,
    {
        self.input.parse_v1(f)
    }
}
