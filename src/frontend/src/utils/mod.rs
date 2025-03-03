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

mod pretty_serde;
pub use pretty_serde::PrettySerde;
mod column_index_mapping;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;

pub use column_index_mapping::*;
mod condition;
pub mod data_type;
pub use condition::*;
mod connected_components;
pub(crate) use connected_components::*;
mod stream_graph_formatter;
pub use stream_graph_formatter::*;
mod with_options;
use tokio::runtime::Runtime;
pub use with_options::*;
mod rewrite_index;
pub use rewrite_index::*;
mod index_set;
pub use index_set::*;
pub(crate) mod group_by;
pub mod overwrite_options;
pub use group_by::*;
pub use overwrite_options::*;

use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};

pub static FRONTEND_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("rw-frontend")
        .enable_all()
        .build()
        .expect("failed to build frontend runtime")
});

/// Substitute `InputRef` with corresponding `ExprImpl`.
pub struct Substitute {
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        assert_eq!(
            input_ref.return_type(),
            self.mapping[input_ref.index()].return_type(),
            "Type mismatch when substituting {:?} with {:?}",
            input_ref,
            self.mapping[input_ref.index()],
        );
        self.mapping[input_ref.index()].clone()
    }
}

// Traits for easy manipulation of recursive structures

/// A `Layer` is a container with subcomponents of type `Sub`.
/// We usually use `Layer` to represents one layer of a tree-like structure,
/// where the subcomponents are the recursive subtrees.
/// But in general, the subcomponent can be of different type than the `Layer`.
/// Such structural relation between `Sub` and `Layer`
/// allows us to lift transformation on `Sub` to that on `Layer.`
/// A related and even more general notion is `Functor`,
/// which might also be helpful to define in the future.
pub trait Layer: Sized {
    type Sub;

    /// Given a transformation `f : Sub -> Sub`,
    /// we can derive a transformation on the entire `Layer` by acting `f` on all subcomponents.
    fn map<F>(self, f: F) -> Self
    where
        F: FnMut(Self::Sub) -> Self::Sub;

    /// Given a traversal `f : Sub -> ()`,
    /// we can derive a traversal on the entire `Layer`
    /// by sequentially visiting the subcomponents with `f`.
    fn descent<F>(&self, f: F)
    where
        F: FnMut(&Self::Sub);
}

/// A tree-like structure is a `Layer` where the subcomponents are recursively trees.
pub trait Tree = Layer<Sub = Self>;

/// Given a tree-like structure `T`,
/// we usually can specify a transformation `T -> T`
/// by providing a pre-order transformation `pre : T -> T`
/// and a post-order transformation `post : T -> T`.
/// Specifically, the derived transformation `apply : T -> T` first applies `pre`,
/// then maps itself over the subtrees, and finally applies `post`.
/// This allows us to obtain a global transformation acting recursively on all levels
/// by specifying simpler transformations at acts locally.
pub trait Endo<T: Tree> {
    fn pre(&mut self, t: T) -> T {
        t
    }

    fn post(&mut self, t: T) -> T {
        t
    }

    /// The real application function is left undefined.
    /// If we want the derived transformation
    /// we can simply call `tree_apply` in the implementation.
    /// But for more complicated requirements,
    /// e.g. skipping over certain subtrees, custom logic can be added.
    fn apply(&mut self, t: T) -> T;

    /// The derived transformation based on `pre` and `post`.
    fn tree_apply(&mut self, t: T) -> T {
        let t = self.pre(t).map(|s| self.apply(s));
        self.post(t)
    }
}

/// A similar trait to generate traversal over tree-like structure.
/// See `Endo` for more details.
#[allow(unused_variables)]
pub trait Visit<T: Tree> {
    fn pre(&mut self, t: &T) {}

    fn post(&mut self, t: &T) {}

    fn visit(&mut self, t: &T);

    fn tree_visit(&mut self, t: &T) {
        self.pre(t);
        t.descent(|i| self.visit(i));
        self.post(t);
    }
}

// Workaround object safety rules for Eq and Hash, adopted from
// https://github.com/bevyengine/bevy/blob/f7fbfaf9c72035e98c6b6cec0c7d26ff9f5b1c82/crates/bevy_utils/src/label.rs

/// An object safe version of [`Eq`]. This trait is automatically implemented
/// for any `'static` type that implements `Eq`.
pub trait DynEq: Any {
    fn as_any(&self) -> &dyn Any;
    fn dyn_eq(&self, other: &dyn DynEq) -> bool;
}

impl<T: Any + Eq> DynEq for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_eq(&self, other: &dyn DynEq) -> bool {
        let other = other.as_any().downcast_ref::<T>();
        other.is_some_and(|other| self == other)
    }
}

impl PartialEq<dyn DynEq + 'static> for dyn DynEq {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other)
    }
}

impl Eq for dyn DynEq {
    fn assert_receiver_is_total_eq(&self) {}
}

/// An object safe version of [`Hash`]. This trait is automatically implemented
/// for any `'static` type that implements `Hash`.
pub trait DynHash: DynEq {
    fn as_dyn_eq(&self) -> &dyn DynEq;
    fn dyn_hash(&self, state: &mut dyn Hasher);
}

impl<T: DynEq + Hash> DynHash for T {
    fn as_dyn_eq(&self) -> &dyn DynEq {
        self
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        T::hash(self, &mut state);
        self.type_id().hash(&mut state);
    }
}

impl Hash for dyn DynHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}

pub fn ordinal(i: usize) -> String {
    let s = i.to_string();
    let suffix = if s.ends_with('1') && !s.ends_with("11") {
        "st"
    } else if s.ends_with('2') && !s.ends_with("12") {
        "nd"
    } else if s.ends_with('3') && !s.ends_with("13") {
        "rd"
    } else {
        "th"
    };
    s + suffix
}
