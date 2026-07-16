use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::schema::{Name, NamesRef, RecordField};

use super::super::assert_record;
use super::Action;

pub struct RecordAdjustInner<'s> {
    #[expect(dead_code)]
    writer: &'s Schema,
    #[expect(dead_code)]
    reader: &'s Schema,
    field_actions: Vec<Action<'s>>,
    reader_order: Vec<RecordField>,
    first_default: usize,
    defaults: Vec<apache_avro::types::Value>,
}

impl RecordAdjustInner<'_> {
    #[expect(dead_code)]
    fn no_reorder(&self) -> bool {
        self.reader_order
            .iter()
            .enumerate()
            .all(|(i, field)| i == field.position)
    }

    pub fn actions(&self) -> &[Action<'_>] {
        &self.field_actions
    }

    pub fn reader_order(&self) -> &[RecordField] {
        &self.reader_order
    }

    pub fn first_default(&self) -> usize {
        self.first_default
    }

    pub fn defaults(&self) -> &[apache_avro::types::Value] {
        &self.defaults
    }
}

fn new<'s>(
    w: &'s Schema,
    r: &'s Schema,
    fa: Vec<Action<'s>>,
    ro: Vec<RecordField>,
    first_d: usize,
    defaults: Vec<apache_avro::types::Value>,
) -> RecordAdjustInner<'s> {
    RecordAdjustInner {
        writer: w,
        reader: r,
        field_actions: fa,
        reader_order: ro,
        first_default: first_d,
        defaults,
    }
}

pub type RecordPairMap<'s> =
    HashMap<(&'s Name, &'s Name), Result<RecordAdjustInner<'s>, Action<'s>>>;

pub fn resolve<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
    seen: &mut RecordPairMap<'s>,
) -> (&'s Name, &'s Name) {
    let w_record = assert_record(w, w_names);
    let r_record = assert_record(r, r_names);

    let write_read_pair = (&w_record.name, &r_record.name);
    let ret = write_read_pair;
    if seen.contains_key(&write_read_pair) {
        return ret;
    }

    // Same as Java, intentionally NO ErrorType::NamesDontMatch check
    let write_fields = &w_record.fields;
    let read_fields = &r_record.fields;

    let first_default = write_fields
        .iter()
        .filter(|write_field| r_record.lookup.contains_key(&write_field.name))
        .count();
    let mut actions = Vec::with_capacity(write_fields.len());
    let mut reordered = Vec::with_capacity(read_fields.len());
    let mut defaults = Vec::with_capacity(read_fields.len() - first_default);
    let placeholder = Action::RecordAdjust {
        pair: write_read_pair,
    };
    seen.insert(write_read_pair, Err(placeholder));

    let mut matching_read_fields = fixedbitset::FixedBitSet::with_capacity(read_fields.len());
    for write_field in write_fields {
        let read_field_idx = r_record.lookup.get(&write_field.name);
        if let Some(read_field_idx) = read_field_idx {
            matching_read_fields.insert(*read_field_idx);
            let read_field = &r_record.fields[*read_field_idx];
            reordered.push(read_field.clone());
            actions.push(super::super::resolve_inner(
                &write_field.schema,
                w_names,
                &read_field.schema,
                r_names,
                seen,
            ));
        } else {
            actions.push(Action::Skip {
                writer: &write_field.schema,
            });
        }
    }
    // Do NOT use `w_record.lookup` because it contains aliases.
    // Use our own `matching_read_fields` for an exact complement of the loop above.
    for read_field_idx in matching_read_fields.zeroes() {
        let read_field = &r_record.fields[read_field_idx];
        if let Some(def_json) = &read_field.default
            && let Some(def_avro) =
                crate::default::decode_default(&read_field.schema, r_names, def_json)
        {
            defaults.push(def_avro);
            reordered.push(read_field.clone());
        } else {
            let result = super::error::ErrorType::MissingRequiredField(&read_field.name).act(w, r);
            seen.insert(write_read_pair, Err(result));
            return ret;
        }
    }

    let result = new(w, r, actions, reordered, first_default, defaults);
    seen.insert(write_read_pair, Ok(result));
    ret
}

#[expect(dead_code)]
pub fn as_result<'s, 'a>(
    pair: (&'s Name, &'s Name),
    seen: &'a RecordPairMap<'s>,
) -> Result<Option<&'a RecordAdjustInner<'s>>, &'a super::error::ErrorInner<'s>> {
    match &seen[&pair] {
        Ok(inner) => Ok(Some(inner)),
        Err(Action::Error(err)) => Err(err),
        Err(Action::RecordAdjust { .. }) => Ok(None),
        Err(_) => unreachable!(),
    }
}
