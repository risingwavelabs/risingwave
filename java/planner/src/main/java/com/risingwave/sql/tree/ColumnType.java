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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class ColumnType<T> extends Expression {

  private final String name;
  private final List<Integer> parameters;

  public ColumnType(String name) {
    this(name, Collections.emptyList());
  }

  public ColumnType(String name, List<Integer> parameters) {
    this.name = name;
    this.parameters = parameters;
  }

  public String name() {
    return name;
  }

  public List<Integer> parameters() {
    return parameters;
  }

  public boolean parametrized() {
    return !parameters.isEmpty();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitColumnType(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnType<?> that = (ColumnType<?>) o;
    return Objects.equals(name, that.name) && Objects.equals(parameters, that.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, parameters);
  }

  @SuppressWarnings("unchecked")
  public <U> ColumnType<U> map(Function<? super T, ? extends U> mapper) {
    return (ColumnType<U>) this;
  }

  public <U> ColumnType<U> mapExpressions(
      ColumnType<U> mappedType, Function<? super T, ? extends U> mapper) {
    return mappedType;
  }
}
