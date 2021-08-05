package com.risingwave.common.config;

import com.google.common.collect.Lists;
import java.util.List;

public class Parsers {
  private Parsers() {}

  public static final Parser<Integer> INT_PARSER = Integer::parseInt;

  /**
   * A simple address converter. The addresses should have following format:
   * "localhost:1234,10.7.1.2:4567".
   */
  public static final Parser<List<String>> ADDRESSES_PARSER =
      value -> Lists.newArrayList(value.split(","));
}
