package com.risingwave.common.config;

@FunctionalInterface
public interface Converter<T> {
  T convert(String value);
}
