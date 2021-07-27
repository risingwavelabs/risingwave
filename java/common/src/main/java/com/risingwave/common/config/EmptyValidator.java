package com.risingwave.common.config;

public class EmptyValidator<T> implements Validator<T> {
  @Override
  public void validate(T value) {}
}
