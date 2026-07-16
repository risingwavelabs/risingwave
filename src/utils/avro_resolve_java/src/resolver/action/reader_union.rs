use apache_avro::Schema;
use apache_avro::schema::{Name, NamesRef};

use super::Action;
use super::record_adjust::RecordPairMap;

pub struct ReaderUnionInner<'s> {
    #[expect(dead_code)]
    writer: &'s Schema,
    #[expect(dead_code)]
    reader: &'s Schema,
    first_match: usize,
    actual_action: Box<Action<'s>>,
}

pub fn new<'s>(
    w: &'s Schema,
    r: &'s Schema,
    first_match: usize,
    actual: Action<'s>,
) -> ReaderUnionInner<'s> {
    ReaderUnionInner {
        writer: w,
        reader: r,
        first_match,
        actual_action: Box::new(actual),
    }
}

pub fn resolve<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
    seen: &mut RecordPairMap<'s>,
) -> Action<'s> {
    assert!(!matches!(w, Schema::Union { .. }));
    let Schema::Union(r_union) = r else {
        unreachable!()
    };

    if let Some(i) = first_matching_branch(w, w_names, r, r_names, seen) {
        Action::ReaderUnion(new(
            w,
            r,
            i,
            super::super::resolve_inner(w, w_names, &r_union.variants()[i], r_names, seen),
        ))
    } else {
        super::error::ErrorType::NoMatchingBranch.act(w, r)
    }
}

fn first_matching_branch<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
    seen: &mut RecordPairMap<'s>,
) -> Option<usize> {
    use super::super::SchemaType as T;
    let Schema::Union(r_union) = r else {
        unreachable!()
    };

    let vt = T::from_schema(w, w_names);
    let mut j = 0;
    let mut structure_match = None;
    for b in r_union.variants() {
        if vt == T::from_schema(b, r_names) {
            if matches!(vt, T::Record | T::Enum | T::Fixed) {
                let vname = w.name().unwrap();
                let bname = b.name().unwrap();
                if vname == bname {
                    return Some(j);
                }

                if vt == T::Record
                    && !hash_match_error(
                        super::record_adjust::resolve(w, w_names, b, r_names, seen),
                        seen,
                    )
                {
                    let v_short_name = &vname.name;
                    let b_short_name = &bname.name;
                    if structure_match.is_none() || v_short_name == b_short_name {
                        structure_match = Some(j);
                    }
                }
            } else {
                return Some(j);
            }
        }
        j += 1;
    }

    if structure_match.is_some() {
        return structure_match;
    }

    j = 0;
    for b in r_union.variants() {
        let bt = T::from_schema(b, r_names);
        if matches!(
            (vt, bt),
            (T::Int, T::Long | T::Float | T::Double)
                | (T::Long, T::Float | T::Double)
                | (T::Float, T::Double)
                | (T::String, T::Bytes)
                | (T::Bytes, T::String)
        ) {
            return Some(j);
        }
        j += 1;
    }

    None
}

fn hash_match_error(pair: (&Name, &Name), seen: &RecordPairMap<'_>) -> bool {
    let ra = match &seen[&pair] {
        Ok(ra) => ra,
        Err(Action::Error(_)) => return true,
        Err(Action::RecordAdjust { .. }) => return false,
        Err(_) => unreachable!(),
    };
    ra.actions().iter().any(|a| match a {
        Action::Error(_) => true,
        Action::RecordAdjust { pair: field_pair } => match &seen[field_pair] {
            Ok(_) => false,
            Err(Action::Error(_)) => true,
            Err(_) => false,
        },
        _ => false,
    })
}

impl ReaderUnionInner<'_> {
    pub fn get_match(&self) -> (usize, &Action<'_>) {
        (self.first_match, &self.actual_action)
    }
}
