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

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

public class ShowTables extends Statement {

  @Nullable private final QualifiedName schema;
  @Nullable private final String likePattern;
  private final Optional<Expression> whereExpression;

  public ShowTables(
      @Nullable QualifiedName schema,
      @Nullable String likePattern,
      Optional<Expression> whereExpression) {
    this.schema = schema;
    this.whereExpression = whereExpression;
    this.likePattern = likePattern;
  }

  @Nullable
  public QualifiedName schema() {
    return schema;
  }

  @Nullable
  public String likePattern() {
    return likePattern;
  }

  public Optional<Expression> whereExpression() {
    return whereExpression;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowTables(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowTables that = (ShowTables) o;
    return Objects.equals(schema, that.schema)
        && Objects.equals(likePattern, that.likePattern)
        && Objects.equals(whereExpression, that.whereExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, likePattern, whereExpression);
  }

  @Override
  public String toString() {
    return "ShowTables{"
        + "schema="
        + schema
        + ", likePattern='"
        + likePattern
        + '\''
        + ", whereExpression="
        + whereExpression
        + '}';
  }
}
