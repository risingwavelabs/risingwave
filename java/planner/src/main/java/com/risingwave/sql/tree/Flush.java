package com.risingwave.sql.tree;

/** Planner node for FLUSH statement. */
public class Flush<T> extends Statement {
  public Flush() {}

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFlush(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "Flush{}";
  }
}
