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

use winnow::stream::{Checkpoint, Offset, SliceLen, Stream, StreamIsPartial, UpdateSlice};

use crate::parser::Parser;
use crate::tokenizer::TokenWithLocation;

#[derive(Copy, Clone, Debug)]
pub struct CheckpointWrapper<'a>(Checkpoint<&'a [TokenWithLocation], &'a [TokenWithLocation]>);

impl<'a> Offset<CheckpointWrapper<'a>> for CheckpointWrapper<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &Self) -> usize {
        self.0.offset_from(&start.0)
    }
}

// Used for diagnostics with `--features winnow/debug`.
impl std::fmt::Debug for Parser<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(token) = self.0.first() {
            write!(f, "{}", token.token)?;
        }
        for token in self.0.iter().skip(1) {
            write!(f, " {}", token.token)?;
        }
        Ok(())
    }
}

impl<'a> Offset<Parser<'a>> for Parser<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &Self) -> usize {
        self.0.offset_from(&start.0)
    }
}

impl<'a> Offset<CheckpointWrapper<'a>> for Parser<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &CheckpointWrapper<'a>) -> usize {
        self.0.offset_from(&start.0)
    }
}

impl SliceLen for Parser<'_> {
    #[inline(always)]
    fn slice_len(&self) -> usize {
        self.0.len()
    }
}

impl<'a> StreamIsPartial for Parser<'a> {
    type PartialState = <&'a [TokenWithLocation] as StreamIsPartial>::PartialState;

    #[inline(always)]
    fn complete(&mut self) -> Self::PartialState {
        self.0.complete()
    }

    #[inline(always)]
    fn restore_partial(&mut self, state: Self::PartialState) {
        self.0.restore_partial(state)
    }

    #[inline(always)]
    fn is_partial_supported() -> bool {
        <&'a [TokenWithLocation] as StreamIsPartial>::is_partial_supported()
    }
}

impl<'a> Stream for Parser<'a> {
    type Checkpoint = CheckpointWrapper<'a>;
    type IterOffsets = <&'a [TokenWithLocation] as Stream>::IterOffsets;
    type Slice = Parser<'a>;
    type Token = <&'a [TokenWithLocation] as Stream>::Token;

    #[inline(always)]
    fn iter_offsets(&self) -> Self::IterOffsets {
        self.0.iter_offsets()
    }

    #[inline(always)]
    fn eof_offset(&self) -> usize {
        self.0.eof_offset()
    }

    #[inline(always)]
    fn next_token(&mut self) -> Option<Self::Token> {
        self.0.next_token()
    }

    #[inline(always)]
    fn offset_for<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Token) -> bool,
    {
        self.0.offset_for(predicate)
    }

    #[inline(always)]
    fn offset_at(&self, tokens: usize) -> Result<usize, winnow::error::Needed> {
        self.0.offset_at(tokens)
    }

    #[inline(always)]
    fn next_slice(&mut self, offset: usize) -> Self::Slice {
        Parser(self.0.next_slice(offset))
    }

    #[inline(always)]
    fn checkpoint(&self) -> Self::Checkpoint {
        CheckpointWrapper(self.0.checkpoint())
    }

    #[inline(always)]
    fn reset(&mut self, checkpoint: &Self::Checkpoint) {
        self.0.reset(&checkpoint.0)
    }

    #[inline(always)]
    fn raw(&self) -> &dyn std::fmt::Debug {
        // We customized the `Debug` implementation in the wrapper, so don't return `self.0` here.
        self
    }

    fn peek_token(&self) -> Option<Self::Token> {
        self.0.peek_token()
    }

    fn peek_slice(&self, offset: usize) -> Self::Slice {
        Parser(self.0.peek_slice(offset))
    }
}

impl UpdateSlice for Parser<'_> {
    #[inline(always)]
    fn update_slice(self, inner: Self::Slice) -> Self {
        Parser(self.0.update_slice(inner.0))
    }
}
