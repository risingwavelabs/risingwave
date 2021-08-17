package com.risingwave.common.entity;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Objects;

public abstract class RootLikeBase<T> implements PathLike<T> {
  private final T value;

  public RootLikeBase(T value) {
    this.value = requireNonNull(value, "value can't be null!");
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RootLikeBase<?> rootNodeBase = (RootLikeBase<?>) o;
    return Objects.equal(value, rootNodeBase.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
