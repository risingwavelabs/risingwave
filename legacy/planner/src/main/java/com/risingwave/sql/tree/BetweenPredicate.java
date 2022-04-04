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

import static java.util.Objects.requireNonNull;

import java.util.Objects;

public class BetweenPredicate extends Expression {
  private final Expression value;
  private final Expression min;
  private final Expression max;

  public BetweenPredicate(Expression value, Expression min, Expression max) {
    this.value = requireNonNull(value, "value is null");
    this.min = requireNonNull(min, "min is null");
    this.max = requireNonNull(max, "max is null");
  }

  public Expression getValue() {
    return value;
  }

  public Expression getMin() {
    return min;
  }

  public Expression getMax() {
    return max;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitBetweenPredicate(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BetweenPredicate that = (BetweenPredicate) o;
    return Objects.equals(value, that.value)
        && Objects.equals(min, that.min)
        && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, min, max);
  }
}
