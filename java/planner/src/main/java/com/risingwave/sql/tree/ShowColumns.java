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
import javax.annotation.Nullable;

public class ShowColumns extends Statement {

  private final QualifiedName table;
  @Nullable private final QualifiedName schema;
  @Nullable private final String likePattern;
  private final Optional<Expression> where;

  public ShowColumns(
      QualifiedName table,
      @Nullable QualifiedName schema,
      Optional<Expression> where,
      @Nullable String likePattern) {
    this.table = requireNonNull(table, "table is null");
    this.schema = schema;
    this.likePattern = likePattern;
    this.where = where;
  }

  public QualifiedName table() {
    return table;
  }

  @Nullable
  public QualifiedName schema() {
    return schema;
  }

  @Nullable
  public String likePattern() {
    return likePattern;
  }

  public Optional<Expression> where() {
    return where;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowColumns(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowColumns that = (ShowColumns) o;
    return Objects.equals(table, that.table)
        && Objects.equals(schema, that.schema)
        && Objects.equals(likePattern, that.likePattern)
        && Objects.equals(where, that.where);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, schema, likePattern, where);
  }

  @Override
  public String toString() {
    return "ShowColumns{"
        + "table="
        + table
        + ", schema="
        + schema
        + ", pattern='"
        + likePattern
        + '\''
        + ", where="
        + where
        + '}';
  }
}
