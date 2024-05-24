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

use crate::tokenizer::{Token, TokenWithLocation, Whitespace};

#[derive(Copy, Clone, Debug)]
pub struct CheckpointWrapper<'a>(Checkpoint<&'a [TokenWithLocation], &'a [TokenWithLocation]>);

impl<'a> Offset<CheckpointWrapper<'a>> for CheckpointWrapper<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &Self) -> usize {
        self.0.offset_from(&start.0)
    }
}

/// Customized wrapper that implements [`TokenStream`][super::TokenStream], override [`Debug`] implementation for better diagnostics.
#[derive(Default, Copy, Clone)]
pub struct TokenStreamWrapper<'a> {
    pub tokens: &'a [TokenWithLocation],
}

impl<'a> std::fmt::Debug for TokenStreamWrapper<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for tok in self.tokens {
            let tok = &tok.token;
            if matches!(tok, Token::Whitespace(Whitespace::Newline)) {
                write!(f, "\\n")?;
            } else {
                write!(f, "{}", tok)?;
            }
        }
        Ok(())
    }
}

impl<'a> Offset<TokenStreamWrapper<'a>> for TokenStreamWrapper<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &Self) -> usize {
        self.tokens.offset_from(&start.tokens)
    }
}

impl<'a> Offset<CheckpointWrapper<'a>> for TokenStreamWrapper<'a> {
    #[inline(always)]
    fn offset_from(&self, start: &CheckpointWrapper<'a>) -> usize {
        self.tokens.offset_from(&start.0)
    }
}

impl<'a> SliceLen for TokenStreamWrapper<'a> {
    #[inline(always)]
    fn slice_len(&self) -> usize {
        self.tokens.len()
    }
}

impl<'a> StreamIsPartial for TokenStreamWrapper<'a> {
    type PartialState = <&'a [TokenWithLocation] as StreamIsPartial>::PartialState;

    #[must_use]
    #[inline(always)]
    fn complete(&mut self) -> Self::PartialState {
        self.tokens.complete()
    }

    #[inline(always)]
    fn restore_partial(&mut self, state: Self::PartialState) {
        self.tokens.restore_partial(state)
    }

    #[inline(always)]
    fn is_partial_supported() -> bool {
        <&'a [TokenWithLocation] as StreamIsPartial>::is_partial_supported()
    }
}

impl<'a> Stream for TokenStreamWrapper<'a> {
    type Checkpoint = CheckpointWrapper<'a>;
    type IterOffsets = <&'a [TokenWithLocation] as Stream>::IterOffsets;
    type Slice = TokenStreamWrapper<'a>;
    type Token = <&'a [TokenWithLocation] as Stream>::Token;

    #[inline(always)]
    fn iter_offsets(&self) -> Self::IterOffsets {
        self.tokens.iter_offsets()
    }

    #[inline(always)]
    fn eof_offset(&self) -> usize {
        self.tokens.eof_offset()
    }

    #[inline(always)]
    fn next_token(&mut self) -> Option<Self::Token> {
        self.tokens.next_token()
    }

    #[inline(always)]
    fn offset_for<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Token) -> bool,
    {
        self.tokens.offset_for(predicate)
    }

    #[inline(always)]
    fn offset_at(&self, tokens: usize) -> Result<usize, winnow::error::Needed> {
        self.tokens.offset_at(tokens)
    }

    #[inline(always)]
    fn next_slice(&mut self, offset: usize) -> Self::Slice {
        TokenStreamWrapper {
            tokens: self.tokens.next_slice(offset),
        }
    }

    #[inline(always)]
    fn checkpoint(&self) -> Self::Checkpoint {
        CheckpointWrapper(self.tokens.checkpoint())
    }

    #[inline(always)]
    fn reset(&mut self, checkpoint: &Self::Checkpoint) {
        self.tokens.reset(&checkpoint.0)
    }

    #[inline(always)]
    fn raw(&self) -> &dyn std::fmt::Debug {
        // We customized the `Debug` implementation in the wrapper, so don't return `self.tokens` here.
        self
    }
}

impl<'a> UpdateSlice for TokenStreamWrapper<'a> {
    #[inline(always)]
    fn update_slice(self, inner: Self::Slice) -> Self {
        TokenStreamWrapper {
            tokens: self.tokens.update_slice(inner.tokens),
        }
    }
}
