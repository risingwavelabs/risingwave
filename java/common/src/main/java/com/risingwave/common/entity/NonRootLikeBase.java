package com.risingwave.common.entity;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Objects;

public abstract class NonRootLikeBase<T, P extends PathLike<T>> implements PathLike<T> {
  private final T value;
  private final P parent;

  public NonRootLikeBase(T value, P parent) {
    this.value = requireNonNull(value, "value can't be null!");
    this.parent = requireNonNull(parent, "parent can't be null!");
  }

  @Override
  public T getValue() {
    return value;
  }

  public P getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NonRootLikeBase<?, ?> that = (NonRootLikeBase<?, ?>) o;
    return Objects.equal(value, that.value) && Objects.equal(parent, that.parent);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value, parent);
  }
}
