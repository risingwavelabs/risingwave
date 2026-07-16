use apache_avro::Schema;
use apache_avro::schema::Name;

pub mod container;
pub mod do_nothing;
pub mod enum_adjust;
pub mod error;
pub mod promote;
pub mod reader_union;
pub mod record_adjust;
pub mod skip;
pub mod writer_union;

pub enum Action<'s> {
    DoNothing {
        writer: &'s Schema,
        reader: &'s Schema,
    },
    Error(error::ErrorInner<'s>),
    Promote {
        writer: &'s Schema,
        reader: &'s Schema,
    },
    Container {
        writer: &'s Schema,
        reader: &'s Schema,
        element_action: Box<Action<'s>>,
    },
    EnumAdjust(enum_adjust::EnumAdjustInner<'s>),
    Skip {
        writer: &'s Schema,
    },
    RecordAdjust {
        pair: (&'s Name, &'s Name),
    },
    WriterUnion(writer_union::WriterUnionInner<'s>),
    ReaderUnion(reader_union::ReaderUnionInner<'s>),
}
