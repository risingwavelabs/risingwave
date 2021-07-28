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
import java.util.Optional;

/** IF(v1,v2[,v3]): CASE WHEN v1 THEN v2 [ELSE v3] END */
public class IfExpression extends Expression {

  private final Expression condition;
  private final Expression trueValue;
  private final Optional<Expression> falseValue;

  public IfExpression(Expression condition, Expression trueValue, Optional<Expression> falseValue) {
    this.condition = requireNonNull(condition, "condition is null");
    this.trueValue = requireNonNull(trueValue, "trueValue is null");
    this.falseValue = falseValue;
  }

  public Expression getCondition() {
    return condition;
  }

  public Expression getTrueValue() {
    return trueValue;
  }

  public Optional<Expression> getFalseValue() {
    return falseValue;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIfExpression(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IfExpression that = (IfExpression) o;
    return Objects.equals(condition, that.condition)
        && Objects.equals(trueValue, that.trueValue)
        && Objects.equals(falseValue, that.falseValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(condition, trueValue, falseValue);
  }
}
