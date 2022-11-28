// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;
use std::fmt::{self, Display};

use pretty::RcDoc;

pub trait NodeExplain<'a> {
    #[deprecated]
    fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &'a str) -> fmt::Result {
        self.distill_named(name).render_fmt(80, f)
    }

    fn distill_fields(&self) -> RcDoc<'a, ()>;
    fn distill_named(&self, name: &'a str) -> RcDoc<'a, ()> {
        RcDoc::concat([
            RcDoc::text(name),
            RcDoc::space(),
            self.distill_fields_braced(),
        ])
    }

    fn distill_fields_braced(&self) -> RcDoc<'a, ()> {
        RcDoc::concat([
            RcDoc::text("{"),
            RcDoc::space(),
            self.distill_fields(),
            RcDoc::space(),
            RcDoc::text("}"),
        ])
    }
}

pub fn field_doc_str<'a>(field: &'a str, value: impl Into<Cow<'a, str>>) -> RcDoc<'a, ()> {
    field_doc(field, RcDoc::text(value))
}

pub fn field_doc<'a>(field: &'a str, value: RcDoc<'a, ()>) -> RcDoc<'a, ()> {
    RcDoc::concat([RcDoc::text(field), RcDoc::text(":"), RcDoc::space(), value])
}

pub fn field_doc_display<'a>(field: &'a str, value: &impl Display) -> RcDoc<'a, ()> {
    field_doc_str(field, value.to_string())
}

pub fn field_doc_iter(field: &str, value: impl Iterator<Item = impl Display>) -> RcDoc<'_, ()> {
    field_doc(
        field,
        RcDoc::concat([
            RcDoc::text("["),
            RcDoc::intersperse(
                value.map(|x| RcDoc::text(x.to_string())),
                RcDoc::concat([RcDoc::text(","), RcDoc::softline()]),
            ),
            RcDoc::text("]"),
        ]),
    )
}
