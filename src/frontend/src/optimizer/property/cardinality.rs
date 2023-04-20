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

use std::cmp::{max, min, Ordering};
use std::ops::{Add, Mul, RangeFrom, RangeInclusive, Sub};

/// The upper bound of the [`Cardinality`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hi {
    Limited(usize),
    Unlimited,
}

impl PartialOrd for Hi {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hi {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Unlimited, Self::Unlimited) => Ordering::Equal,
            (Self::Unlimited, Self::Limited(_)) => Ordering::Greater,
            (Self::Limited(_), Self::Unlimited) => Ordering::Less,
            (Self::Limited(lhs), Self::Limited(rhs)) => lhs.cmp(rhs),
        }
    }
}

impl From<Option<usize>> for Hi {
    fn from(value: Option<usize>) -> Self {
        value.map_or(Self::Unlimited, Self::Limited)
    }
}

impl From<usize> for Hi {
    fn from(value: usize) -> Self {
        Self::Limited(value)
    }
}

/// The cardinality of the output rows of a plan node. Bounds are inclusive.
///
/// The default value is `0..`, i.e. the number of rows is unknown.
// TODO: Make this the property of each plan node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cardinality {
    lo: usize,
    hi: Hi,
}

impl Default for Cardinality {
    fn default() -> Self {
        Self {
            lo: 0,
            hi: Hi::Unlimited,
        }
    }
}

impl From<RangeInclusive<usize>> for Cardinality {
    /// Converts an inclusive range to a [`Cardinality`].
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(0..=10);
    /// assert_eq!(card.lo(), 0);
    /// assert_eq!(card.hi(), Some(10));
    /// ```
    fn from(value: RangeInclusive<usize>) -> Self {
        Self::new(*value.start(), *value.end())
    }
}

impl From<RangeFrom<usize>> for Cardinality {
    /// Converts a range with unlimited upper bound to a [`Cardinality`].
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(10..);
    /// assert_eq!(card.lo(), 10);
    /// assert_eq!(card.hi(), None);
    /// ```
    fn from(value: RangeFrom<usize>) -> Self {
        Self::new(value.start, Hi::Unlimited)
    }
}

impl From<usize> for Cardinality {
    /// Converts a single value to a [`Cardinality`] with the same lower and upper bounds, i.e. the
    /// value is exact.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(10);
    /// assert_eq!(card.lo(), 10);
    /// assert_eq!(card.hi(), Some(10));
    /// ```
    fn from(value: usize) -> Self {
        Self::new(value, Hi::Limited(value))
    }
}

impl Cardinality {
    /// Creates a new [`Cardinality`] with the given lower and upper bounds.
    pub fn new(lo: usize, hi: impl Into<Hi>) -> Self {
        let hi: Hi = hi.into();
        debug_assert!(hi >= Hi::from(lo));

        Self { lo, hi }
    }

    /// Returns the lower bound of the cardinality.
    pub fn lo(self) -> usize {
        self.lo
    }

    /// Returns the upper bound of the cardinality, `None` if the upper bound is unlimited.
    pub fn hi(self) -> Option<usize> {
        match self.hi {
            Hi::Limited(hi) => Some(hi),
            Hi::Unlimited => None,
        }
    }

    /// Returns the minimum of the two cardinalities, where the lower and upper bounds are
    /// respectively the minimum of the lower and upper bounds of the two cardinalities.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card1 = Cardinality::from(3..);
    /// let card2 = Cardinality::from(5..=8);
    /// let card3 = Cardinality::from(3..=8);
    /// assert_eq!(card1.min(card2), card3);
    ///
    /// // Limit both the lower and upper bounds to a single value, a.k.a. "limit_to".
    /// let card1 = Cardinality::from(3..=10);
    /// let card2 = Cardinality::from(5);
    /// let card3 = Cardinality::from(3..=5);
    /// assert_eq!(card1.min(card2), card3);
    ///
    /// // Limit the lower bound to a value, a.k.a. "as_low_as".
    /// let card1 = Cardinality::from(3..=10);
    /// let card2 = Cardinality::from(1..);
    /// let card3 = Cardinality::from(1..=10);
    /// assert_eq!(card1.min(card2), card3);
    /// ```
    pub fn min(self, rhs: impl Into<Self>) -> Self {
        let rhs: Self = rhs.into();
        Self::new(min(self.lo(), rhs.lo()), min(self.hi, rhs.hi))
    }

    /// Returns the maximum of the two cardinalities, where the lower and upper bounds are
    /// respectively the maximum of the lower and upper bounds of the two cardinalities.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card1 = Cardinality::from(3..);
    /// let card2 = Cardinality::from(5..=8);
    /// let card3 = Cardinality::from(5..);
    /// assert_eq!(card1.max(card2), card3);
    /// ```
    pub fn max(self, rhs: impl Into<Self>) -> Self {
        let rhs: Self = rhs.into();
        Self::new(max(self.lo(), rhs.lo()), max(self.hi, rhs.hi))
    }
}

impl Add<Cardinality> for Cardinality {
    type Output = Self;

    /// Returns the sum of the two cardinalities, where the lower and upper bounds are
    /// respectively the sum of the lower and upper bounds of the two cardinalities.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card1 = Cardinality::from(3..=5);
    /// let card2 = Cardinality::from(5..=10);
    /// let card3 = Cardinality::from(8..=15);
    /// assert_eq!(card1 + card2, card3);
    ///
    /// let card1 = Cardinality::from(3..);
    /// let card2 = Cardinality::from(5..=10);
    /// let card3 = Cardinality::from(8..);
    /// assert_eq!(card1 + card2, card3);
    /// ```
    fn add(self, rhs: Self) -> Self::Output {
        let lo = self.lo().saturating_add(rhs.lo());
        let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
            lhs.checked_add(rhs)
        } else {
            None
        };
        Self::new(lo, hi)
    }
}

impl Sub<usize> for Cardinality {
    type Output = Self;

    /// Returns the cardinality with both lower and upper bounds subtracted by `rhs`.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(3..=5);
    /// assert_eq!(card - 2, Cardinality::from(1..=3));
    /// assert_eq!(card - 4, Cardinality::from(0..=1));
    /// assert_eq!(card - 6, Cardinality::from(0));
    ///
    /// let card = Cardinality::from(3..);
    /// assert_eq!(card - 2, Cardinality::from(1..));
    /// assert_eq!(card - 4, Cardinality::from(0..));
    /// ```
    fn sub(self, rhs: usize) -> Self::Output {
        let lo = self.lo().saturating_sub(rhs);
        let hi = self.hi().map(|hi| hi.saturating_sub(rhs));
        Self::new(lo, hi)
    }
}

impl Mul<Cardinality> for Cardinality {
    type Output = Cardinality;

    /// Returns the product of the two cardinalities, where the lower and upper bounds are
    /// respectively the product of the lower and upper bounds of the two cardinalities.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card1 = Cardinality::from(2..=4);
    /// let card2 = Cardinality::from(3..=5);
    /// let card3 = Cardinality::from(6..=20);
    /// assert_eq!(card1 * card2, card3);
    ///
    /// let card1 = Cardinality::from(2..);
    /// let card2 = Cardinality::from(3..=5);
    /// let card3 = Cardinality::from(6..);
    /// assert_eq!(card1 * card2, card3);
    ///
    /// let card1 = Cardinality::from((usize::MAX - 1)..=(usize::MAX));
    /// let card2 = Cardinality::from(3..=5);
    /// let card3 = Cardinality::from((usize::MAX)..);
    /// assert_eq!(card1 * card2, card3);
    /// ```
    fn mul(self, rhs: Cardinality) -> Self::Output {
        let lo = self.lo().saturating_mul(rhs.lo());
        let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
            lhs.checked_mul(rhs)
        } else {
            None
        };
        Self::new(lo, hi)
    }
}

impl Mul<usize> for Cardinality {
    type Output = Self;

    /// Returns the cardinality with both lower and upper bounds multiplied by `rhs`.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(3..=5);
    /// assert_eq!(card * 2, Cardinality::from(6..=10));
    ///
    /// let card = Cardinality::from(3..);
    /// assert_eq!(card * 2, Cardinality::from(6..));
    ///
    /// let card = Cardinality::from((usize::MAX - 1)..=(usize::MAX));
    /// assert_eq!(card * 2, Cardinality::from((usize::MAX)..));
    /// ```
    fn mul(self, rhs: usize) -> Self::Output {
        let lo = self.lo().saturating_mul(rhs);
        let hi = self.hi().and_then(|hi| hi.checked_mul(rhs));
        Self::new(lo, hi)
    }
}

impl Cardinality {
    /// Returns the cardinality if it is exact, `None` otherwise.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(3..=3);
    /// assert_eq!(card.get_exact(), Some(3));
    ///
    /// let card = Cardinality::from(3..=4);
    /// assert_eq!(card.get_exact(), None);
    /// ```
    pub fn get_exact(self) -> Option<usize> {
        self.hi().filter(|hi| *hi == self.lo())
    }

    /// Returns `true` if the cardinality is at most `count` rows.
    ///
    /// ```
    /// # use risingwave_frontend::optimizer::property::Cardinality;
    /// let card = Cardinality::from(3..=5);
    /// assert!(card.is_at_most(5));
    /// assert!(card.is_at_most(6));
    /// assert!(!card.is_at_most(4));
    ///
    /// let card = Cardinality::from(3..);
    /// assert!(!card.is_at_most(usize::MAX));
    /// ```
    pub fn is_at_most(self, count: usize) -> bool {
        self.hi().is_some_and(|hi| hi <= count)
    }
}
