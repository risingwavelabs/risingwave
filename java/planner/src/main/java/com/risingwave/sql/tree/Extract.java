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

import java.util.Locale;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Extract extends Expression {

  private final Expression expression;
  private final Field field;

  public enum Field {
    CENTURY,
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY,
    DAY_OF_MONTH,
    DAY_OF_WEEK,
    DOW,
    DAY_OF_YEAR,
    DOY,
    HOUR,
    MINUTE,
    SECOND,
    TIMEZONE_HOUR,
    TIMEZONE_MINUTE,
    EPOCH
  }

  public Extract(Expression expression, StringLiteral field) {
    // field: ident is converted to StringLiteral in SqlBase.g
    this.expression = requireNonNull(expression, "expression is null");
    this.field = Field.valueOf(field.getValue().toUpperCase(Locale.ENGLISH));
  }

  public Expression getExpression() {
    return expression;
  }

  public Field getField() {
    return field;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExtract(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Extract extract = (Extract) o;
    return Objects.equals(expression, extract.expression) && field == extract.field;
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, field);
  }
}
