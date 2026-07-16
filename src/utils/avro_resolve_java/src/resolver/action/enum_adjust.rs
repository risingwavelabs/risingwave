use apache_avro::Schema;
use apache_avro::schema::NamesRef;

use super::super::assert_enum;
use super::Action;

pub struct EnumAdjustInner<'s> {
    // The passed schema may be Ref rather than definition.
    // To avoid confusion, we do NOT store either form until necessary.
    // writer: &'s Schema,
    // reader: &'s Schema,
    adj_vals: Vec<Option<AdjVal<'s>>>,
    #[expect(dead_code)]
    no_adjustments_needed: bool,
}

#[derive(Clone, Copy)]
struct AdjVal<'s> {
    reader_index: usize,
    symbol: &'s str,
}

impl From<AdjVal<'_>> for apache_avro::types::Value {
    fn from(value: AdjVal<'_>) -> Self {
        Self::Enum(value.reader_index as _, value.symbol.to_owned())
    }
}

fn new<'s>(rsym_count: usize, adj_vals: Vec<Option<AdjVal<'s>>>) -> EnumAdjustInner<'s> {
    let count = rsym_count.min(adj_vals.len());
    let no_adj = adj_vals.len() <= rsym_count
        && adj_vals
            .iter()
            .take(count)
            .enumerate()
            .all(|(writer_index, v)| v.as_ref().is_some_and(|v| writer_index == v.reader_index));
    EnumAdjustInner {
        // writer: w,
        // reader: r,
        adj_vals,
        no_adjustments_needed: no_adj,
    }
}

pub fn resolve<'s>(
    w: &'s Schema,
    w_names: &NamesRef<'s>,
    r: &'s Schema,
    r_names: &NamesRef<'s>,
) -> Action<'s> {
    let w_enum = assert_enum(w, w_names);
    let r_enum = assert_enum(r, r_names);

    if w_enum.name != r_enum.name {
        return super::error::ErrorType::NamesDontMatch.act(w, r);
    }

    let wsymbols = &w_enum.symbols;
    let rsymbols = &r_enum.symbols;
    let default_index = r_enum
        .default
        .as_ref()
        .and_then(|d| rsymbols.iter().position(|s| s == d));

    let default_value = default_index.map(|i| AdjVal {
        reader_index: i,
        symbol: &rsymbols[i],
    });
    let adj_vals = wsymbols
        .iter()
        .map(|symbol| {
            rsymbols
                .iter()
                .position(|s| s == symbol)
                .map(|reader_index| AdjVal {
                    reader_index,
                    symbol,
                })
                .or(default_value)
        })
        .collect();
    let rsym_count = r_enum.symbols.len();
    super::Action::EnumAdjust(new(rsym_count, adj_vals))
}

impl EnumAdjustInner<'_> {
    pub fn apply(&self, w_idx: u32, w_sym: String) -> Result<apache_avro::types::Value, String> {
        self.adj_vals[w_idx as usize]
            .map(|adj_val| adj_val.into())
            .ok_or_else(|| format!("No match for {w_sym}"))
    }
}
