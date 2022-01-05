pub mod aggregation;
pub mod flush_status;
pub mod join;
pub mod top_n;

use std::cmp::Reverse;

use risingwave_common::array::{Row, RowRef};
use risingwave_common::types::{serialize_datum_into, Datum};
use risingwave_common::util::sort_util::OrderType;

use crate::executor::managed_state::OrderedDatum::{NormalOrder, ReversedOrder};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum OrderedDatum {
    NormalOrder(Datum),
    ReversedOrder(Reverse<Datum>),
}

/// `OrderedRow` is used for the pk in those states whose primary key contains several columns and
/// requires comparison.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrderedRow(Vec<OrderedDatum>);

impl OrderedRow {
    pub fn new(row: Row, order_types: &[OrderType]) -> Self {
        assert_eq!(row.0.len(), order_types.len());
        OrderedRow(
            row.0
                .into_iter()
                .zip(order_types.iter())
                .map(|(datum, order_type)| match order_type {
                    OrderType::Ascending => NormalOrder(datum),
                    OrderType::Descending => ReversedOrder(Reverse(datum)),
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn into_row(self) -> Row {
        Row(self
            .0
            .into_iter()
            .map(|ordered_datum| match ordered_datum {
                NormalOrder(datum) => datum,
                ReversedOrder(datum) => datum.0,
            })
            .collect::<Vec<_>>())
    }

    pub fn as_row_ref(&self) -> RowRef<'_> {
        let datum_refs = self
            .0
            .iter()
            .map(|ordered_datum| {
                let datum = match ordered_datum {
                    NormalOrder(datum) => datum,
                    ReversedOrder(datum) => &datum.0,
                };
                datum.as_ref().map(|scalar| scalar.as_scalar_ref_impl())
            })
            .collect::<Vec<_>>();
        RowRef(datum_refs)
    }

    /// Serialize the row into a memcomparable bytes.
    ///
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    pub fn serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for v in &self.0 {
            let datum = match v {
                NormalOrder(datum) => {
                    serializer.set_reverse(false);
                    datum
                }
                ReversedOrder(datum) => {
                    serializer.set_reverse(true);
                    &datum.0
                }
            };
            serialize_datum_into(datum, &mut serializer)?;
        }
        Ok(serializer.into_inner())
    }

    pub fn reverse_serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut res = self.serialize()?;
        res.iter_mut().for_each(|byte| *byte = !*byte);
        Ok(res)
    }
}
