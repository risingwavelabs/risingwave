use crate::error::Result;
use crate::types::Decimal;

#[inline(always)]
pub fn round_digits<D: Into<i32>>(input: Decimal, digits: D) -> Result<Decimal> {
    let digits = digits.into();
    if digits < 0 {
        Ok(Decimal::zero())
    } else {
        Ok(input.round_dp(digits as u32))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::types::Decimal;
    use crate::vector_op::round::round_digits;

    fn do_test(input: &str, digits: i32, expected_output: &str) {
        let v = Decimal::from_str(input).unwrap();
        let rounded_value = round_digits(v, digits).unwrap();
        assert_eq!(expected_output, rounded_value.to_string().as_str());
    }

    #[test]
    fn test_round_digits() {
        do_test("21.666666666666666666666666667", 4, "21.6667");
        do_test("84818.33333333333333333333333", 4, "84818.3333");
        do_test("21.372736", -1, "0");
    }
}
