use core::fmt;

use itertools::Itertools;

/// a builder for explain nodes.
pub struct NodeExplainBuilder {
    node_name: String,
    fields: Vec<String>,
}

impl NodeExplainBuilder {
    pub fn new(node_name: String) -> Self {
        Self {
            node_name,
            fields: vec![],
        }
    }

    pub fn field(&mut self, name: &str, value: &str) {
        self.fields.push(name.to_owned() + ": " + value);
    }

    pub fn debug_field(&mut self, name: &str, value: &dyn fmt::Debug) {
        self.field(name, &format!("{:?}", value));
    }

    pub fn display_field(&mut self, name: &str, value: &dyn fmt::Debug) {
        self.field(name, &format!("{:?}", value));
    }

    pub fn write_in_one_row<'a, 'b>(self, f: &'a mut fmt::Formatter<'b>) -> fmt::Result {
        write!(
            f,
            "{} {{ {}}}",
            self.node_name,
            self.fields
                .into_iter()
                .map(|s| s.replace('\n', " "))
                .format(", ")
        )
    }
}

pub trait NodeExplain {
    fn explain(&self, builder: &mut NodeExplainBuilder);
}
