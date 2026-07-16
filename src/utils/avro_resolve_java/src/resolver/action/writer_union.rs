use apache_avro::Schema;
use apache_avro::schema::NamesRef;

use super::Action;
use super::record_adjust::RecordPairMap;

pub struct WriterUnionInner<'s> {
    #[expect(dead_code)]
    writer: &'s Schema,
    #[expect(dead_code)]
    reader: &'s Schema,
    actions: Vec<Action<'s>>,
    union_equiv: bool,
}

fn new<'s>(
    w: &'s Schema,
    r: &'s Schema,
    union_equiv: bool,
    a: Vec<Action<'s>>,
) -> WriterUnionInner<'s> {
    WriterUnionInner {
        writer: w,
        reader: r,
        actions: a,
        union_equiv,
    }
}

pub fn resolve<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
    seen: &mut RecordPairMap<'s>,
) -> Action<'s> {
    let mut clean = Default::default();
    let union_equivalent = super::super::union_equiv(w, w_names, r, r_names, &mut clean);

    let Schema::Union(w_union) = w else {
        unreachable!()
    };
    let write_types = w_union.variants();
    let read_types = union_equivalent.then(|| {
        let Schema::Union(r_union) = r else {
            unreachable!()
        };
        r_union.variants()
    });
    let actions = write_types
        .iter()
        .enumerate()
        .map(|(i, ww)| {
            super::super::resolve_inner(
                ww,
                w_names,
                read_types.as_ref().map(|rts| &rts[i]).unwrap_or(r),
                r_names,
                seen,
            )
        })
        .collect();
    super::Action::WriterUnion(new(w, r, union_equivalent, actions))
}

impl WriterUnionInner<'_> {
    pub fn get_action(&self, writer_index: usize) -> &Action<'_> {
        &self.actions[writer_index]
    }

    pub fn union_equiv(&self) -> bool {
        self.union_equiv
    }
}
