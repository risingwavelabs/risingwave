package com.risingwave.common.config;

@FunctionalInterface
public interface Validator<T> {
  void validate(T value);
}
