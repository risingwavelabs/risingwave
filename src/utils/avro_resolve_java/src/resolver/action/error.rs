use apache_avro::Schema;

pub enum ErrorType<'s> {
    IncompatibleSchemaTypes,
    NamesDontMatch,
    SizesDontMatch,
    MissingRequiredField(&'s str),
    NoMatchingBranch,
}

pub struct ErrorInner<'s> {
    writer: &'s Schema,
    reader: &'s Schema,
    error: ErrorType<'s>,
}

impl std::fmt::Display for ErrorInner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ErrorType as E;

        match self.error {
            E::IncompatibleSchemaTypes
            | E::NamesDontMatch
            | E::SizesDontMatch
            | E::NoMatchingBranch => {
                write!(
                    f,
                    "Found {}, expecting {}",
                    self.writer
                        .name()
                        .map(|n| n.fullname(None))
                        .unwrap_or_else(|| self.writer.to_string()),
                    self.reader
                        .name()
                        .map(|n| n.fullname(None))
                        .unwrap_or_else(|| self.reader.to_string()),
                )
            }
            E::MissingRequiredField(fname) => {
                write!(
                    f,
                    "Found {}, expecting {}, missing required field {}",
                    self.writer
                        .name()
                        .map(|n| n.fullname(None))
                        .unwrap_or_default(),
                    self.reader
                        .name()
                        .map(|n| n.fullname(None))
                        .unwrap_or_default(),
                    fname,
                )
            }
        }
    }
}

impl<'s> ErrorType<'s> {
    pub fn act(self, w: &'s Schema, r: &'s Schema) -> super::Action<'s> {
        super::Action::Error(ErrorInner {
            writer: w,
            reader: r,
            error: self,
        })
    }
}
