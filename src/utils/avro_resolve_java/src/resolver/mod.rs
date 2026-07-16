use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::schema::{Name, NamesRef, ResolvedSchema};

mod action;
mod aliases;

pub use action::Action;
pub use action::record_adjust::RecordPairMap;
pub use aliases::apply_aliases;

/// Unlike `SchemaKind`, this is physical-only
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SchemaType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed,
    Enum,
    Record,
    Array,
    Map,
    Union,
}

impl SchemaType {
    fn from_schema(s: &Schema, names: &NamesRef<'_>) -> Self {
        use Schema as S;
        use SchemaType as T;

        match s {
            S::Null => T::Null,
            S::Boolean => T::Boolean,
            S::Int => T::Int,
            S::Long => T::Long,
            S::Float => T::Float,
            S::Double => T::Double,
            S::Bytes => T::Bytes,
            S::String => T::String,
            S::Fixed { .. } => T::Fixed,
            S::Enum { .. } => T::Enum,
            S::Record { .. } => T::Record,
            S::Array(_) => T::Array,
            S::Map(_) => T::Map,
            S::Union(_) => T::Union,
            S::Decimal(dec) => Self::from_schema(&dec.inner, names),
            S::BigDecimal => T::Bytes,
            S::Uuid => T::String, // todo
            S::Date => T::Int,
            S::TimeMillis => T::Int,
            S::TimeMicros => T::Long,
            S::TimestampMillis => T::Long,
            S::TimestampMicros => T::Long,
            S::TimestampNanos => T::Long,
            S::LocalTimestampMillis => T::Long,
            S::LocalTimestampMicros => T::Long,
            S::LocalTimestampNanos => T::Long,
            S::Duration => T::Fixed,
            S::Ref { name } => {
                let resolved = names.get(name).unwrap();
                Self::from_schema(resolved, names)
            }
        }
    }
}

// #[easy_ext::ext(Downcast)] pub impl Schema
fn assert_fixed<'s>(s: &'s Schema, names: &NamesRef<'s>) -> &'s apache_avro::schema::FixedSchema {
    match s {
        crate::types!(primitive)
        | Schema::Array(_)
        | Schema::Map(_)
        | Schema::Union(_)
        | Schema::Record(_)
        | Schema::Enum(_) => unreachable!(),
        Schema::Fixed(inner) => inner,
        Schema::Ref { name } => assert_fixed(names[name], names),
        Schema::Decimal(inner) => assert_fixed(&inner.inner, names),
        Schema::Uuid | Schema::Duration => todo!(),
        crate::types!(temporal) | Schema::BigDecimal => unreachable!(),
    }
}

fn assert_enum<'s>(s: &'s Schema, names: &NamesRef<'s>) -> &'s apache_avro::schema::EnumSchema {
    match s {
        crate::types!(primitive)
        | Schema::Array(_)
        | Schema::Map(_)
        | Schema::Union(_)
        | Schema::Record(_)
        | Schema::Fixed(_) => unreachable!(),
        Schema::Enum(inner) => inner,
        Schema::Ref { name } => assert_enum(names[name], names),
        crate::types!(temporal)
        | Schema::BigDecimal
        | Schema::Decimal(_)
        | Schema::Uuid
        | Schema::Duration => unreachable!(),
    }
}

fn assert_record<'s>(s: &'s Schema, names: &NamesRef<'s>) -> &'s apache_avro::schema::RecordSchema {
    match s {
        crate::types!(primitive)
        | Schema::Array(_)
        | Schema::Map(_)
        | Schema::Union(_)
        | Schema::Fixed(_)
        | Schema::Enum(_) => unreachable!(),
        Schema::Record(inner) => inner,
        Schema::Ref { name } => assert_record(names[name], names),
        crate::types!(temporal)
        | Schema::BigDecimal
        | Schema::Decimal(_)
        | Schema::Uuid
        | Schema::Duration => unreachable!(),
    }
}

pub fn resolve<'s>(w: &'s Schema, r: &'s Schema) -> (Action<'s>, RecordPairMap<'s>) {
    // NB: writer = Schema.applyAliases(writer, reader) must be done by caller due to 's lifetime
    let w_resolved = ResolvedSchema::try_from(w).unwrap();
    let r_resolved = ResolvedSchema::try_from(r).unwrap();
    let mut seen = Default::default();
    let root = resolve_inner(
        w,
        w_resolved.get_names(),
        r,
        r_resolved.get_names(),
        &mut seen,
    );
    (root, seen)
}

pub fn resolve_inner<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
    seen: &mut RecordPairMap<'s>,
) -> Action<'s> {
    use SchemaType as T;

    let w_type: SchemaType = SchemaType::from_schema(w, w_names);
    let r_type: SchemaType = SchemaType::from_schema(r, r_names);

    if w_type == T::Union {
        return action::writer_union::resolve(w, w_names, r, r_names, seen);
    }

    if w_type == r_type {
        match w_type {
            T::Null
            | T::Boolean
            | T::Int
            | T::Long
            | T::Float
            | T::Double
            | T::String
            | T::Bytes => action::Action::DoNothing {
                writer: w,
                reader: r,
            },
            T::Fixed => {
                let w_fixed = assert_fixed(w, w_names);
                let r_fixed = assert_fixed(r, r_names);

                if w_fixed.name != r_fixed.name {
                    action::error::ErrorType::NamesDontMatch.act(w, r)
                } else if w_fixed.size != r_fixed.size {
                    action::error::ErrorType::SizesDontMatch.act(w, r)
                } else {
                    action::Action::DoNothing {
                        writer: w,
                        reader: r,
                    }
                }
            }
            T::Array => {
                let Schema::Array(w_array) = w else {
                    unreachable!()
                };
                let Schema::Array(r_array) = r else {
                    unreachable!()
                };

                let et = resolve_inner(&w_array.items, w_names, &r_array.items, r_names, seen);
                action::Action::Container {
                    writer: w,
                    reader: r,
                    element_action: Box::new(et),
                }
            }
            T::Map => {
                let Schema::Map(w_map) = w else {
                    unreachable!()
                };
                let Schema::Map(r_map) = r else {
                    unreachable!()
                };

                let vt = resolve_inner(&w_map.types, w_names, &r_map.types, r_names, seen);
                action::Action::Container {
                    writer: w,
                    reader: r,
                    element_action: Box::new(vt),
                }
            }
            T::Enum => action::enum_adjust::resolve(w, w_names, r, r_names),
            T::Record => {
                let pair = action::record_adjust::resolve(w, w_names, r, r_names, seen);
                action::Action::RecordAdjust { pair }
            }
            T::Union => unreachable!(),
        }
    } else if r_type == T::Union {
        action::reader_union::resolve(w, w_names, r, r_names, seen)
    } else {
        action::promote::resolve(w, w_names, r, r_names)
    }
}

fn union_equiv<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'_>,
    r: &'s Schema,
    r_names: &NamesRef<'_>,
    seen: &mut HashMap<(Name, Name), bool>,
) -> bool {
    use SchemaType as T;

    let wt = SchemaType::from_schema(w, w_names);
    let rt = SchemaType::from_schema(r, r_names);
    if wt != rt {
        return false;
    }

    // todo: name of decimal/uuid/duration over fixed is wrong in apache_avro
    if matches!(wt, T::Record | T::Fixed | T::Enum) && !(w.name().is_none() || w.name() == r.name())
    {
        return false;
    }

    match wt {
        T::Null | T::Boolean | T::Int | T::Long | T::Float | T::Double | T::String | T::Bytes => {
            true
        }
        T::Array => {
            let Schema::Array(w_array) = w else {
                unreachable!()
            };
            let Schema::Array(r_array) = r else {
                unreachable!()
            };

            union_equiv(&w_array.items, w_names, &r_array.items, r_names, seen)
        }
        T::Map => {
            let Schema::Map(w_map) = w else {
                unreachable!()
            };
            let Schema::Map(r_map) = r else {
                unreachable!()
            };

            union_equiv(&w_map.types, w_names, &r_map.types, r_names, seen)
        }
        T::Fixed => {
            let w_fixed = assert_fixed(w, w_names);
            let r_fixed = assert_fixed(r, r_names);

            w_fixed.size == r_fixed.size
        }
        T::Enum => {
            let w_enum = assert_enum(w, w_names);
            let r_enum = assert_enum(r, r_names);

            w_enum.symbols == r_enum.symbols
        }
        T::Union => {
            let Schema::Union(w_union) = w else {
                unreachable!()
            };
            let Schema::Union(r_union) = r else {
                unreachable!()
            };

            let wb = w_union.variants();
            let rb = r_union.variants();
            if wb.len() != rb.len() {
                return false;
            }
            #[expect(clippy::disallowed_methods)]
            wb.iter()
                .zip(rb.iter())
                .all(|(w, r)| union_equiv(w, w_names, r, r_names, seen))
        }
        T::Record => {
            let w_record = assert_record(w, w_names);
            let r_record = assert_record(r, r_names);

            let wsc = (w_record.name.clone(), r_record.name.clone());
            if !seen.contains_key(&wsc) {
                seen.insert(wsc.clone(), true);
                let wb = &w_record.fields;
                let rb = &r_record.fields;
                if wb.len() != rb.len() {
                    seen.insert(wsc.clone(), false);
                } else {
                    #[expect(clippy::disallowed_methods)]
                    for (ww, rr) in wb.iter().zip(rb.iter()) {
                        if ww.name != rr.name
                            || !union_equiv(&ww.schema, w_names, &rr.schema, r_names, seen)
                        {
                            seen.insert(wsc.clone(), false);
                            break;
                        }
                    }
                }
            }
            seen[&wsc]
        }
    }
}
