package com.risingwave.common.config;

/**
 * A parser parse string to target config value.
 *
 * @param <T> Target entry type.
 * @see Parsers
 */
@FunctionalInterface
public interface Parser<T> {
  T convert(String value);
}
