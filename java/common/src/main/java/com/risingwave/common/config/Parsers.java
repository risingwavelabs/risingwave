package com.risingwave.common.config;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import java.util.List;

public class Parsers {
  private Parsers() {}

  public static final Parser<Integer> INT_PARSER = Integer::parseInt;

  /**
   * A simple address converter. The addresses should have the following format:
   * "localhost:1234,10.7.1.2:4567".
   */
  public static final Parser<List<String>> ADDRESSES_PARSER =
      value -> Lists.newArrayList(value.split(","));

  public static <E extends Enum<E>> Parser<E> enumParserOf(Class<E> klass) {
    return new EnumParser<>(klass);
  }

  private static class EnumParser<E extends Enum<E>> implements Parser<E> {
    private final Class<E> enumClass;

    private EnumParser(Class<E> enumClass) {
      this.enumClass = requireNonNull(enumClass, "enumClass");
    }

    @Override
    public E convert(String value) {
      return Enum.valueOf(enumClass, value);
    }
  }
}
