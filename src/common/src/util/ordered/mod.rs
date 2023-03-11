// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod serde;

use std::cmp::Reverse;

use OrderedDatum::{NormalOrder, ReversedOrder};

pub use self::serde::*;
use super::iter_util::ZipEqFast;
use super::sort_util::Direction;
use crate::row::OwnedRow;
use crate::types::{memcmp_serialize_datum_into, Datum};
use crate::util::sort_util::OrderType;

// TODO(rc): support `NULLS FIRST | LAST`
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum OrderedDatum {
    NormalOrder(Datum),
    ReversedOrder(Reverse<Datum>),
}

impl std::fmt::Debug for OrderedDatum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalOrder(d) => match d {
                Some(s) => write!(f, "{:?}", s),
                None => write!(f, "NULL"),
            },
            ReversedOrder(d) => match &d.0 {
                Some(s) => write!(f, "{:?}", s),
                None => write!(f, "NULL"),
            },
        }
    }
}

/// `OrderedRow` is used for the pk in those states whose primary key contains several columns and
/// requires comparison.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct OrderedRow(Vec<OrderedDatum>);

impl std::fmt::Debug for OrderedRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl OrderedRow {
    pub fn new(row: OwnedRow, order_types: &[OrderType]) -> Self {
        OrderedRow(
            row.into_inner()
                .into_iter()
                .zip_eq_fast(order_types.iter())
                .map(|(datum, order_type)| match order_type.direction() {
                    Direction::Ascending => NormalOrder(datum),
                    Direction::Descending => ReversedOrder(Reverse(datum)),
                })
                .collect::<Vec<_>>(),
        )
    }

    pub fn into_vec(self) -> Vec<Datum> {
        self.0
            .into_iter()
            .map(|ordered_datum| match ordered_datum {
                NormalOrder(datum) => datum,
                ReversedOrder(datum) => datum.0,
            })
            .collect::<Vec<_>>()
    }

    pub fn into_row(self) -> OwnedRow {
        OwnedRow::new(self.into_vec())
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
            memcmp_serialize_datum_into(datum, &mut serializer)?;
        }
        Ok(serializer.into_inner())
    }

    pub fn reverse_serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut res = self.serialize()?;
        res.iter_mut().for_each(|byte| *byte = !*byte);
        Ok(res)
    }

    pub fn prefix(&self, n: usize) -> Self {
        assert!(n <= self.0.len());
        OrderedRow(self.0[..n].to_vec())
    }

    pub fn starts_with(&self, other: &Self) -> bool {
        self.0.starts_with(&other.0)
    }

    pub fn skip(&self, n: usize) -> Self {
        assert!(n < self.0.len());
        OrderedRow(self.0[n..].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ScalarImpl;

    fn make_row(values: Vec<i64>) -> OrderedRow {
        let row = OwnedRow::new(
            values
                .into_iter()
                .map(|v| Some(ScalarImpl::Int64(v)))
                .collect(),
        );
        OrderedRow::new(row, ORDER_TYPES)
    }

    const ORDER_TYPES: &[OrderType] = &[
        OrderType::ascending(),
        OrderType::descending(),
        OrderType::ascending(),
    ];

    #[test]
    fn test_prefix() {
        let row = make_row(vec![1, 2, 3]);

        assert!(row.prefix(0) < row.prefix(1));
        assert!(row.prefix(1) < row.prefix(2));
        assert!(row.prefix(2) < row.prefix(3));
        assert_eq!(row.prefix(3), row);

        let row2 = make_row(vec![1, 3, 3]);
        assert!(row.prefix(1) < row2);
        assert!(row.prefix(2) > row2);
    }

    #[should_panic]
    #[test]
    fn test_prefix_panic() {
        let row = make_row(vec![1, 2, 3]);
        row.prefix(4);
    }
}
