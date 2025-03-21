// Copyright 2025 RisingWave Labs
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

// Copyright (c) 2015 Jonathan Reem

// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:

// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Wrappers for total order on Floats.  See the [`OrderedFloat`] docs for details.

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::iter::{Product, Sum};
use core::num::FpCategory;
use core::ops::{
    Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Rem, RemAssign, Sub, SubAssign,
};
use core::str::FromStr;
use std::error::Error;
use std::fmt::Debug;

use bytes::BytesMut;
pub use num_traits::Float;
use num_traits::{
    Bounded, CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Pow,
    Zero,
};
use postgres_types::{IsNull, ToSql, Type, accepts, to_sql_checked};

// masks for the parts of the IEEE 754 float
const SIGN_MASK: u64 = 0x8000000000000000u64;
const EXP_MASK: u64 = 0x7ff0000000000000u64;
const MAN_MASK: u64 = 0x000fffffffffffffu64;

// canonical raw bit patterns (for hashing)
const CANONICAL_NAN_BITS: u64 = 0x7ff8000000000000u64;
const CANONICAL_ZERO_BITS: u64 = 0x0u64;

/// A wrapper around floats providing implementations of `Eq`, `Ord`, and `Hash`.
///
/// `NaN` is sorted as *greater* than all other values and *equal*
/// to itself, in contradiction with the IEEE standard.
///
/// ```ignore
/// use std::f32::NAN;
///
/// use ordered_float::OrderedFloat;
///
/// let mut v = [OrderedFloat(NAN), OrderedFloat(2.0), OrderedFloat(1.0)];
/// v.sort();
/// assert_eq!(v, [OrderedFloat(1.0), OrderedFloat(2.0), OrderedFloat(NAN)]);
/// ```
///
/// Because `OrderedFloat` implements `Ord` and `Eq`, it can be used as a key in a `HashSet`,
/// `HashMap`, `BTreeMap`, or `BTreeSet` (unlike the primitive `f32` or `f64` types):
///
/// ```ignore
/// # use ordered_float::OrderedFloat;
/// # use std::collections::HashSet;
/// # use std::f32::NAN;
///
/// let mut s: HashSet<OrderedFloat<f32>> = HashSet::new();
/// s.insert(OrderedFloat(NAN));
/// assert!(s.contains(&OrderedFloat(NAN)));
/// ```
#[derive(Debug, Default, Clone, Copy, Serialize)]
#[repr(transparent)]
pub struct OrderedFloat<T>(pub T);

impl<T: Float> OrderedFloat<T> {
    /// Get the value out.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl ToSql for OrderedFloat<f32> {
    accepts!(FLOAT4);

    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }
}

impl ToSql for OrderedFloat<f64> {
    accepts!(FLOAT8);

    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }
}

impl<T: Float> AsRef<T> for OrderedFloat<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T: Float> AsMut<T> for OrderedFloat<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Float> ZeroHeapSize for OrderedFloat<T> {}

impl<'a, T: Float> From<&'a T> for &'a OrderedFloat<T> {
    #[inline]
    fn from(t: &'a T) -> &'a OrderedFloat<T> {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values.
        unsafe { &*(t as *const T as *const OrderedFloat<T>) }
    }
}

impl<'a, T: Float> From<&'a mut T> for &'a mut OrderedFloat<T> {
    #[inline]
    fn from(t: &'a mut T) -> &'a mut OrderedFloat<T> {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values.
        unsafe { &mut *(t as *mut T as *mut OrderedFloat<T>) }
    }
}

impl<T: Float> PartialOrd for OrderedFloat<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Float> Ord for OrderedFloat<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = &self.0;
        let rhs = &other.0;
        match lhs.partial_cmp(rhs) {
            Some(ordering) => ordering,
            None => {
                if lhs.is_nan() {
                    if rhs.is_nan() {
                        Ordering::Equal
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Less
                }
            }
        }
    }
}

impl<T: Float> PartialEq for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &OrderedFloat<T>) -> bool {
        if self.0.is_nan() {
            other.0.is_nan()
        } else {
            self.0 == other.0
        }
    }
}

impl<T: Float> PartialEq<T> for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.0 == *other
    }
}

impl<T: Float> Hash for OrderedFloat<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.is_nan() {
            // normalize to one representation of NaN
            hash_float(&T::nan(), state)
        } else {
            hash_float(&self.0, state)
        }
    }
}

impl<T: Float + fmt::Display> fmt::Display for OrderedFloat<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = self.0;
        if v.is_nan() {
            return write!(f, "NaN");
        }
        if v.is_infinite() {
            if v.is_sign_negative() {
                write!(f, "-")?
            }
            return write!(f, "Infinity");
        }
        self.0.fmt(f)
    }
}

impl From<OrderedFloat<f32>> for f32 {
    #[inline]
    fn from(f: OrderedFloat<f32>) -> f32 {
        f.0
    }
}

impl From<OrderedFloat<f64>> for f64 {
    #[inline]
    fn from(f: OrderedFloat<f64>) -> f64 {
        f.0
    }
}

impl<T: Float> From<T> for OrderedFloat<T> {
    #[inline]
    fn from(val: T) -> Self {
        OrderedFloat(val)
    }
}

impl<T: Float> Eq for OrderedFloat<T> {}

macro_rules! impl_ordered_float_binop {
    ($imp:ident, $method:ident, $assign_imp:ident, $assign_method:ident) => {
        impl<T: $imp> $imp for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<T: $imp> $imp<T> for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for OrderedFloat<T>
        where
            T: $imp<&'a T>,
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a Self> for OrderedFloat<T>
        where
            T: $imp<&'a T>,
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<'a, T> $imp for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<'a, T> $imp<OrderedFloat<T>> for &'a OrderedFloat<T>
        where
            &'a T: $imp<T>,
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: OrderedFloat<T>) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<'a, T> $imp<T> for &'a OrderedFloat<T>
        where
            &'a T: $imp<T>,
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        #[doc(hidden)] // Added accidentally; remove in next major version
        impl<'a, T> $imp<&'a Self> for &'a OrderedFloat<T>
        where
            &'a T: $imp,
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: &'a Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<T: $assign_imp> $assign_imp<T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: T) {
                (self.0).$assign_method(other);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a T) {
                (self.0).$assign_method(other);
            }
        }

        impl<T: $assign_imp> $assign_imp for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: Self) {
                (self.0).$assign_method(other.0);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a Self> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a Self) {
                (self.0).$assign_method(&other.0);
            }
        }
    };
}

impl_ordered_float_binop! {Add, add, AddAssign, add_assign}
impl_ordered_float_binop! {Sub, sub, SubAssign, sub_assign}
impl_ordered_float_binop! {Mul, mul, MulAssign, mul_assign}
impl_ordered_float_binop! {Div, div, DivAssign, div_assign}
impl_ordered_float_binop! {Rem, rem, RemAssign, rem_assign}

impl<T> Pow<OrderedFloat<T>> for OrderedFloat<T>
where
    T: Float,
{
    type Output = OrderedFloat<T>;

    fn pow(self, rhs: Self) -> Self::Output {
        OrderedFloat(self.0.powf(rhs.0))
    }
}

impl<T> CheckedAdd for OrderedFloat<T>
where
    T: Float,
{
    fn checked_add(&self, v: &Self) -> Option<Self> {
        Some(self.add(*v))
    }
}

impl<T> CheckedSub for OrderedFloat<T>
where
    T: Float,
{
    fn checked_sub(&self, v: &Self) -> Option<Self> {
        Some(self.sub(*v))
    }
}

impl<T> CheckedMul for OrderedFloat<T>
where
    T: Float,
{
    fn checked_mul(&self, v: &Self) -> Option<Self> {
        Some(self.mul(*v))
    }
}

impl<T> CheckedDiv for OrderedFloat<T>
where
    T: Float,
{
    fn checked_div(&self, v: &Self) -> Option<Self> {
        Some(self.div(*v))
    }
}

impl<T> CheckedRem for OrderedFloat<T>
where
    T: Float,
{
    fn checked_rem(&self, v: &Self) -> Option<Self> {
        Some(self.rem(*v))
    }
}

impl<T> CheckedNeg for OrderedFloat<T>
where
    T: Float,
{
    fn checked_neg(&self) -> Option<Self> {
        Some(self.neg())
    }
}

/// Adds a float directly.
impl<T: Float + Sum> Sum for OrderedFloat<T> {
    fn sum<I: Iterator<Item = OrderedFloat<T>>>(iter: I) -> Self {
        OrderedFloat(iter.map(|v| v.0).sum())
    }
}

impl<'a, T: Float + Sum + 'a> Sum<&'a OrderedFloat<T>> for OrderedFloat<T> {
    #[inline]
    fn sum<I: Iterator<Item = &'a OrderedFloat<T>>>(iter: I) -> Self {
        iter.cloned().sum()
    }
}

impl<T: Float + Product> Product for OrderedFloat<T> {
    fn product<I: Iterator<Item = OrderedFloat<T>>>(iter: I) -> Self {
        OrderedFloat(iter.map(|v| v.0).product())
    }
}

impl<'a, T: Float + Product + 'a> Product<&'a OrderedFloat<T>> for OrderedFloat<T> {
    #[inline]
    fn product<I: Iterator<Item = &'a OrderedFloat<T>>>(iter: I) -> Self {
        iter.cloned().product()
    }
}

impl<T: Bounded> Bounded for OrderedFloat<T> {
    #[inline]
    fn min_value() -> Self {
        OrderedFloat(T::min_value())
    }

    #[inline]
    fn max_value() -> Self {
        OrderedFloat(T::max_value())
    }
}

impl<T: FromStr> FromStr for OrderedFloat<T> {
    type Err = T::Err;

    /// Convert a &str to `OrderedFloat`. Returns an error if the string fails to parse.
    ///
    /// ```ignore
    /// use ordered_float::OrderedFloat;
    ///
    /// assert!("-10".parse::<OrderedFloat<f32>>().is_ok());
    /// assert!("abc".parse::<OrderedFloat<f32>>().is_err());
    /// assert!("NaN".parse::<OrderedFloat<f32>>().is_ok());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        T::from_str(s).map(OrderedFloat)
    }
}

impl<T: Neg> Neg for OrderedFloat<T> {
    type Output = OrderedFloat<T::Output>;

    #[inline]
    fn neg(self) -> Self::Output {
        OrderedFloat(-self.0)
    }
}

impl<'a, T> Neg for &'a OrderedFloat<T>
where
    &'a T: Neg,
{
    type Output = OrderedFloat<<&'a T as Neg>::Output>;

    #[inline]
    fn neg(self) -> Self::Output {
        OrderedFloat(-(&self.0))
    }
}

impl<T: Zero> Zero for OrderedFloat<T> {
    #[inline]
    fn zero() -> Self {
        OrderedFloat(T::zero())
    }

    #[inline]
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl<T: One> One for OrderedFloat<T> {
    #[inline]
    fn one() -> Self {
        OrderedFloat(T::one())
    }
}

/// Similar to [`num_traits::Float`], but without requiring `NumCast` and `ToPrimitive`.
#[easy_ext::ext(FloatExt)]
pub impl<T: Float> OrderedFloat<T>
where
    Self: Sized + Copy,
{
    fn nan() -> Self {
        OrderedFloat(T::nan())
    }

    fn infinity() -> Self {
        OrderedFloat(T::infinity())
    }

    fn neg_infinity() -> Self {
        OrderedFloat(T::neg_infinity())
    }

    fn neg_zero() -> Self {
        OrderedFloat(T::neg_zero())
    }

    fn min_value() -> Self {
        OrderedFloat(T::min_value())
    }

    fn min_positive_value() -> Self {
        OrderedFloat(T::min_positive_value())
    }

    fn max_value() -> Self {
        OrderedFloat(T::max_value())
    }

    fn is_nan(self) -> bool {
        self.0.is_nan()
    }

    fn is_infinite(self) -> bool {
        self.0.is_infinite()
    }

    fn is_finite(self) -> bool {
        self.0.is_finite()
    }

    fn is_normal(self) -> bool {
        self.0.is_normal()
    }

    fn classify(self) -> FpCategory {
        self.0.classify()
    }

    fn floor(self) -> Self {
        OrderedFloat(self.0.floor())
    }

    fn ceil(self) -> Self {
        OrderedFloat(self.0.ceil())
    }

    fn round(self) -> Self {
        OrderedFloat(self.0.round())
    }

    fn trunc(self) -> Self {
        OrderedFloat(self.0.trunc())
    }

    fn fract(self) -> Self {
        OrderedFloat(self.0.fract())
    }

    fn abs(self) -> Self {
        OrderedFloat(self.0.abs())
    }

    fn signum(self) -> Self {
        OrderedFloat(self.0.signum())
    }

    fn is_sign_positive(self) -> bool {
        self.0.is_sign_positive()
    }

    fn is_sign_negative(self) -> bool {
        self.0.is_sign_negative()
    }

    fn mul_add(self, a: Self, b: Self) -> Self {
        OrderedFloat(self.0.mul_add(a.0, b.0))
    }

    fn recip(self) -> Self {
        OrderedFloat(self.0.recip())
    }

    fn powi(self, n: i32) -> Self {
        OrderedFloat(self.0.powi(n))
    }

    fn powf(self, n: Self) -> Self {
        OrderedFloat(self.0.powf(n.0))
    }

    fn sqrt(self) -> Self {
        OrderedFloat(self.0.sqrt())
    }

    fn exp(self) -> Self {
        OrderedFloat(self.0.exp())
    }

    fn exp2(self) -> Self {
        OrderedFloat(self.0.exp2())
    }

    fn ln(self) -> Self {
        OrderedFloat(self.0.ln())
    }

    fn log(self, base: Self) -> Self {
        OrderedFloat(self.0.log(base.0))
    }

    fn log2(self) -> Self {
        OrderedFloat(self.0.log2())
    }

    fn log10(self) -> Self {
        OrderedFloat(self.0.log10())
    }

    fn max(self, other: Self) -> Self {
        OrderedFloat(self.0.max(other.0))
    }

    fn min(self, other: Self) -> Self {
        OrderedFloat(self.0.min(other.0))
    }

    fn abs_sub(self, other: Self) -> Self {
        OrderedFloat(self.0.abs_sub(other.0))
    }

    fn cbrt(self) -> Self {
        OrderedFloat(self.0.cbrt())
    }

    fn hypot(self, other: Self) -> Self {
        OrderedFloat(self.0.hypot(other.0))
    }

    fn sin(self) -> Self {
        OrderedFloat(self.0.sin())
    }

    fn cos(self) -> Self {
        OrderedFloat(self.0.cos())
    }

    fn tan(self) -> Self {
        OrderedFloat(self.0.tan())
    }

    fn asin(self) -> Self {
        OrderedFloat(self.0.asin())
    }

    fn acos(self) -> Self {
        OrderedFloat(self.0.acos())
    }

    fn atan(self) -> Self {
        OrderedFloat(self.0.atan())
    }

    fn atan2(self, other: Self) -> Self {
        OrderedFloat(self.0.atan2(other.0))
    }

    fn sin_cos(self) -> (Self, Self) {
        let (a, b) = self.0.sin_cos();
        (OrderedFloat(a), OrderedFloat(b))
    }

    fn exp_m1(self) -> Self {
        OrderedFloat(self.0.exp_m1())
    }

    fn ln_1p(self) -> Self {
        OrderedFloat(self.0.ln_1p())
    }

    fn sinh(self) -> Self {
        OrderedFloat(self.0.sinh())
    }

    fn cosh(self) -> Self {
        OrderedFloat(self.0.cosh())
    }

    fn tanh(self) -> Self {
        OrderedFloat(self.0.tanh())
    }

    fn asinh(self) -> Self {
        OrderedFloat(self.0.asinh())
    }

    fn acosh(self) -> Self {
        OrderedFloat(self.0.acosh())
    }

    fn atanh(self) -> Self {
        OrderedFloat(self.0.atanh())
    }

    fn integer_decode(self) -> (u64, i16, i8) {
        self.0.integer_decode()
    }

    fn epsilon() -> Self {
        OrderedFloat(T::epsilon())
    }

    fn to_degrees(self) -> Self {
        OrderedFloat(self.0.to_degrees())
    }

    fn to_radians(self) -> Self {
        OrderedFloat(self.0.to_radians())
    }
}

impl<T: Float + Num> Num for OrderedFloat<T> {
    type FromStrRadixErr = T::FromStrRadixErr;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        T::from_str_radix(str, radix).map(OrderedFloat)
    }
}

impl<T: Float> OrderedFloat<T> {
    /// Normalize `-NaN` and `-0.0` to positive form.
    pub fn normalized(self) -> Self {
        if self.is_nan() {
            // normalize -NaN
            Self::nan()
        } else if self.is_zero() {
            // normalize -0.0
            Self::zero()
        } else {
            self
        }
    }
}

#[inline]
fn hash_float<F: Float, H: Hasher>(f: &F, state: &mut H) {
    raw_double_bits(f).hash(state);
}

#[inline]
fn raw_double_bits<F: Float>(f: &F) -> u64 {
    if f.is_nan() {
        return CANONICAL_NAN_BITS;
    }

    let (man, exp, sign) = f.integer_decode();
    if man == 0 {
        return CANONICAL_ZERO_BITS;
    }

    let exp_u64 = exp as u16 as u64;
    let sign_u64 = (sign > 0) as u64;
    (man & MAN_MASK) | ((exp_u64 << 52) & EXP_MASK) | ((sign_u64 << 63) & SIGN_MASK)
}

mod impl_rand {
    use rand::Rng;
    use rand::distr::uniform::*;
    use rand::distr::{Distribution, Open01, OpenClosed01, StandardUniform};

    use super::OrderedFloat;

    macro_rules! impl_distribution {
        ($dist:ident, $($f:ty),+) => {
            $(
            impl Distribution<OrderedFloat<$f>> for $dist {
                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> OrderedFloat<$f> {
                    OrderedFloat(self.sample(rng))
                }
            }
            )*
        }
    }

    impl_distribution! { StandardUniform, f32, f64 }
    impl_distribution! { Open01, f32, f64 }
    impl_distribution! { OpenClosed01, f32, f64 }

    pub struct UniformOrdered<T>(UniformFloat<T>);
    impl SampleUniform for OrderedFloat<f32> {
        type Sampler = UniformOrdered<f32>;
    }
    impl SampleUniform for OrderedFloat<f64> {
        type Sampler = UniformOrdered<f64>;
    }

    macro_rules! impl_uniform_sampler {
        ($f:ty) => {
            impl UniformSampler for UniformOrdered<$f> {
                type X = OrderedFloat<$f>;

                fn new<B1, B2>(low: B1, high: B2) -> Result<Self, Error>
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformFloat::<$f>::new(low.borrow().0, high.borrow().0).map(UniformOrdered)
                }

                fn new_inclusive<B1, B2>(low: B1, high: B2) -> Result<Self, Error>
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformFloat::<$f>::new_inclusive(low.borrow().0, high.borrow().0)
                        .map(UniformOrdered)
                }

                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
                    OrderedFloat(self.0.sample(rng))
                }
            }
        };
    }

    impl_uniform_sampler! { f32 }
    impl_uniform_sampler! { f64 }
}

mod impl_from {
    use super::*;

    impl From<OrderedFloat<f32>> for OrderedFloat<f64> {
        fn from(s: OrderedFloat<f32>) -> Self {
            Self(s.0.into())
        }
    }

    macro_rules! impl_from_lossless {
        ($ty:ty, $f:ty) => {
            impl From<$ty> for OrderedFloat<$f> {
                fn from(n: $ty) -> Self {
                    let inner: $f = n.into();
                    Self(inner)
                }
            }
        };
    }

    macro_rules! impl_from_approx {
        ($ty:ty, $f:ty) => {
            impl From<$ty> for OrderedFloat<$f> {
                fn from(n: $ty) -> Self {
                    let inner: $f = n as _;
                    Self(inner)
                }
            }
        };
    }

    impl_from_lossless!(i16, f32);
    impl_from_approx!(i32, f32);
    impl_from_approx!(i64, f32);
    impl_from_approx!(u64, f32);

    impl_from_lossless!(i16, f64);
    impl_from_lossless!(i32, f64);
    impl_from_approx!(i64, f64);
    impl_from_approx!(u64, f64);

    impl TryFrom<OrderedFloat<f64>> for OrderedFloat<f32> {
        type Error = &'static str;

        fn try_from(s: OrderedFloat<f64>) -> Result<Self, Self::Error> {
            let inner = s.0 as f32;
            if inner.is_infinite() && s.0.is_finite() {
                Err("double precision to real out of range: overflow")
            } else if inner == 0.0 && s.0 != 0.0 {
                Err("double precision to real out of range: underflow")
            } else {
                Ok(Self(inner))
            }
        }
    }

    macro_rules! impl_try_from_even {
        ($f:ty, $ty:ty) => {
            impl TryFrom<OrderedFloat<$f>> for $ty {
                type Error = &'static str;

                fn try_from(n: OrderedFloat<$f>) -> Result<Self, Self::Error> {
                    let n = n.0.round_ties_even();
                    // `-MIN` can be represented exactly but `MAX` cannot.
                    // So we test `>= -MIN` rather than `> MAX`.
                    if n.is_nan() || n < (<$ty>::MIN as $f) || n >= -(<$ty>::MIN as $f) {
                        Err("float to integral out of range")
                    } else {
                        Ok(n as _)
                    }
                }
            }
        };
    }

    impl_try_from_even!(f32, i16);
    impl_try_from_even!(f32, i32);
    impl_try_from_even!(f32, i64);
    impl_try_from_even!(f64, i16);
    impl_try_from_even!(f64, i32);
    impl_try_from_even!(f64, i64);
}

mod impl_into_ordered {
    use super::*;

    pub trait IntoOrdered: 'static + Float {
        fn into_ordered(self) -> OrderedFloat<Self>;
    }

    impl<F> IntoOrdered for F
    where
        F: 'static + Float,
    {
        fn into_ordered(self) -> OrderedFloat<Self> {
            self.into()
        }
    }
}

pub use impl_into_ordered::IntoOrdered;
use risingwave_common_estimate_size::ZeroHeapSize;
use serde::Serialize;

#[cfg(test)]
mod tests {
    use risingwave_common_estimate_size::EstimateSize;

    use crate::types::IntoOrdered;
    use crate::types::ordered_float::OrderedFloat;

    #[test]
    fn test_cast_to_f64() {
        // i64 -> f64.
        let ret: OrderedFloat<f64> = OrderedFloat::<f64>::from(5_i64);
        assert_eq!(ret, OrderedFloat::<f64>::from(5_f64));

        // decimal -> f64.
        let ret: OrderedFloat<f64> =
            OrderedFloat::<f64>::try_from(crate::types::Decimal::from(5)).unwrap();
        assert_eq!(ret, OrderedFloat::<f64>::from(5_f64));
    }

    #[test]
    fn test_nan_eq() {
        let nan_prim = f64::NAN;
        assert_ne!(nan_prim, nan_prim);

        let nan = OrderedFloat::<f64>::from(nan_prim);
        assert_eq!(nan, nan);

        use crate::types::FloatExt as _;
        assert_eq!(nan.round(), nan.round());
    }

    fn test_into_f32(expected: [u8; 4], v: impl Into<OrderedFloat<f32>>) {
        assert_eq!(expected, v.into().0.to_be_bytes());
    }

    fn test_into_f64(expected: [u8; 8], v: impl Into<OrderedFloat<f64>>) {
        assert_eq!(expected, v.into().0.to_be_bytes());
    }

    fn test_from_f32<T>(float_bytes: [u8; 4], expected: Option<T>)
    where
        T: TryFrom<OrderedFloat<f32>> + Eq + std::fmt::Debug,
    {
        let v = f32::from_be_bytes(float_bytes).into_ordered();
        assert_eq!(v.try_into().ok(), expected);
    }

    #[test]
    fn test_ordered_float_cast() {
        // Expectations obtained from PostgreSQL: select float4send('-32768'::int2::float4);
        test_into_f32([0xc7, 0x00, 0x00, 0x00], i16::MIN);
        test_into_f32([0xc6, 0xff, 0xfe, 0x00], -i16::MAX);
        test_into_f32([0x46, 0xff, 0xfe, 0x00], i16::MAX);
        test_into_f32([0xcf, 0x00, 0x00, 0x00], i32::MIN);
        test_into_f32([0xcf, 0x00, 0x00, 0x00], -i32::MAX); // approx, so same as above
        test_into_f32([0x4f, 0x00, 0x00, 0x00], i32::MAX);
        test_into_f32([0xdf, 0x00, 0x00, 0x00], i64::MIN);
        test_into_f32([0xdf, 0x00, 0x00, 0x00], -i64::MAX); // approx
        test_into_f32([0x5f, 0x00, 0x00, 0x00], i64::MAX);

        test_into_f64([0xc0, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], i16::MIN);
        test_into_f64([0xc0, 0xdf, 0xff, 0xc0, 0x00, 0x00, 0x00, 0x00], -i16::MAX);
        test_into_f64([0x40, 0xdf, 0xff, 0xc0, 0x00, 0x00, 0x00, 0x00], i16::MAX);
        test_into_f64([0xc1, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], i32::MIN);
        test_into_f64([0xc1, 0xdf, 0xff, 0xff, 0xff, 0xc0, 0x00, 0x00], -i32::MAX);
        test_into_f64([0x41, 0xdf, 0xff, 0xff, 0xff, 0xc0, 0x00, 0x00], i32::MAX);
        test_into_f64([0xc3, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], i64::MIN);
        test_into_f64([0xc3, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], -i64::MAX); // approx
        test_into_f64([0x43, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], i64::MAX);

        // neg i16
        test_from_f32([0xc7, 0x00, 0x00, 0x81], None::<i16>);
        test_from_f32([0xc7, 0x00, 0x00, 0x80], Some(i16::MIN)); // -32768.5
        test_from_f32([0xc7, 0x00, 0x00, 0x00], Some(i16::MIN));
        // pos i16
        test_from_f32([0x46, 0xff, 0xfe, 0x00], Some(i16::MAX));
        test_from_f32([0x46, 0xff, 0xfe, 0xff], Some(i16::MAX));
        test_from_f32([0x46, 0xff, 0xff, 0x00], None::<i16>); // 32767.5

        // neg i32
        test_from_f32([0xcf, 0x00, 0x00, 0x01], None::<i32>);
        test_from_f32([0xcf, 0x00, 0x00, 0x00], Some(i32::MIN));
        // pos i32
        test_from_f32([0x4e, 0xff, 0xff, 0xff], Some(0x7fff_ff80_i32));
        test_from_f32([0x4f, 0x00, 0x00, 0x00], None::<i32>);

        // neg i64
        test_from_f32([0xdf, 0x00, 0x00, 0x01], None::<i64>);
        test_from_f32([0xdf, 0x00, 0x00, 0x00], Some(i64::MIN));
        // pos i64
        test_from_f32([0x5e, 0xff, 0xff, 0xff], Some(0x7fff_ff80_0000_0000_i64));
        test_from_f32([0x5f, 0x00, 0x00, 0x00], None::<i64>);
    }

    #[test]
    fn test_ordered_float_estimate_size() {
        let ordered_float = OrderedFloat::<f64>::from(5_i64);
        assert_eq!(ordered_float.estimated_size(), 8);
    }
}
