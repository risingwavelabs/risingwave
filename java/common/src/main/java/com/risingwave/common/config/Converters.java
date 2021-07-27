package com.risingwave.common.config;

public class Converters {
  private Converters() {}

  public static final Converter<Integer> INT_CONVERTER = Integer::parseInt;
}
