package com.risingwave.common.config;

import com.google.common.base.Verify;

public class Validators {
  private Validators() {}

  public static final Validator<Integer> intRangeValidator(int minimum, int maximum) {
    return value ->
        Verify.verify(
            value >= minimum && value < maximum,
            "Invalid value %s, range: [%s, %s].",
            minimum,
            maximum);
  }
}
