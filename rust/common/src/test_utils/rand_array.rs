//! Contains helper methods for generating random arrays in tests.
//!
//! Use [`seed_rand_array`] to generate an random array.

use std::sync::Arc;

use num_traits::FromPrimitive;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::types::{Decimal, IntervalUnit, NativeType, Scalar};

pub trait RandValue {
    fn rand_value<R: Rng>(rand: &mut R) -> Self;
}

impl<T> RandValue for T
where
    T: NativeType,
    Standard: Distribution<T>,
{
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen()
    }
}

impl RandValue for String {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let len: usize = rand.gen::<usize>() % 10 + 1;
        (0..len).map(|_| rand.gen::<char>()).collect()
    }
}

impl RandValue for Decimal {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        Decimal::from_f64((rand.gen::<u32>() as f64) + 0.1f64).unwrap()
    }
}

impl RandValue for IntervalUnit {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let months = (rand.gen::<u32>() % 100) as i32;
        let days = (rand.gen::<u32>() % 200) as i32;
        let ms = (rand.gen::<u64>() % 100000) as i64;
        IntervalUnit::new(months, days, ms)
    }
}

impl RandValue for bool {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen::<i32>() > 0
    }
}

pub fn rand_array<A, R>(rand: &mut R, size: usize) -> A
where
    A: Array,
    R: Rng,
    A::OwnedItem: RandValue,
{
    let mut builder = A::Builder::new(size).unwrap();
    for _ in 0..size {
        let is_null = rand.gen::<bool>();
        if is_null {
            builder.append_null().unwrap();
        } else {
            let value = A::OwnedItem::rand_value(rand);
            builder.append(Some(value.as_scalar_ref())).unwrap();
        }
    }

    builder.finish().unwrap()
}

pub fn seed_rand_array<A>(size: usize, seed: u64) -> A
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let mut rand = SmallRng::seed_from_u64(seed);
    rand_array(&mut rand, size)
}

pub fn seed_rand_array_ref<A>(size: usize, seed: u64) -> ArrayRef
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let array: A = seed_rand_array(size, seed);
    Arc::new(array.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::interval_array::IntervalArray;
    use crate::array::*;
    use crate::for_all_variants;

    #[test]
    fn test_create_array() {
        macro_rules! gen_rand_array {
      ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        $(
          {
            let array = seed_rand_array::<$array>(10, 1024);
            assert_eq!(10, array.len());

          }
        )*
      };
    }

        for_all_variants! { gen_rand_array }
    }
}
