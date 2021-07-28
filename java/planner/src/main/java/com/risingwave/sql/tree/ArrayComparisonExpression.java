/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.sql.tree;

/**
 *
 *
 * <pre>
 *      expression cmpOp quantifier ( arrayExpression )
 * </pre>
 *
 * <p>E.g.
 *
 * <pre>
 *      x = ANY ([1, 2, 3])
 * </pre>
 *
 * <p>Use {@link #getLeft()} to access expression. Use {@link #getRight()} to access arrayExpression
 */
public class ArrayComparisonExpression extends ComparisonExpression implements ArrayComparison {

  private final Quantifier quantifier;

  public ArrayComparisonExpression(
      Type type, Quantifier quantifier, Expression expression, Expression arrayExpression) {
    super(type, expression, arrayExpression);
    this.quantifier = quantifier;
  }

  @Override
  public Quantifier quantifier() {
    return quantifier;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitArrayComparisonExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ArrayComparisonExpression that = (ArrayComparisonExpression) o;

    if (quantifier != that.quantifier) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (quantifier != null ? quantifier.hashCode() : 0);
    return result;
  }
}
