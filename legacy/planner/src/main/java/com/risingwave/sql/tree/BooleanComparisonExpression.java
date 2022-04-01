package com.risingwave.sql.tree;

import java.util.Objects;

/** Pg's boolean comparison operators. */
public class BooleanComparisonExpression extends Expression {

  /** Boolean comparison operators. */
  public enum Type {
    IS_TRUE("IS TRUE"),
    IS_NOT_TRUE("IS NOT TRUE"),
    IS_FALSE("IS FALSE"),
    IS_NOT_FALSE("IS NOT FALSE"),
    IS_UNKNOWN("IS UNKNOWN"),
    IS_NOT_UNKNOWN("IS NOT UNKNOWN");

    private final String functionName;

    Type(String functionName) {
      this.functionName = functionName;
    }

    public String getFunctionName() {
      return functionName;
    }
  }

  private final Type comparisonType;
  private final Expression child;

  public BooleanComparisonExpression(Type comparisonType, Expression child) {
    this.comparisonType = comparisonType;
    this.child = child;
  }

  public Type getComparisonType() {
    return comparisonType;
  }

  public Expression getChild() {
    return child;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitBooleanComparisonExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BooleanComparisonExpression that = (BooleanComparisonExpression) o;
    return getComparisonType() == that.getComparisonType() && getChild().equals(that.getChild());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getComparisonType(), getChild());
  }
}
