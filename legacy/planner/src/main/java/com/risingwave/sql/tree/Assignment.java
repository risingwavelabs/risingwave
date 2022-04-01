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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Assignment<T> extends Node {

  private final T columnName;
  private final List<T> expressions;

  /**
   * Constructor for SET SESSION/LOCAL statements one or more expressions are allowed on the right
   * side of the assignment DEFAULT -> empty list of expressions VALUE -> single item in expressions
   * list value can be either string literal, numeric literal, or ident VALUE, VALUE, ... -> two or
   * more items in expressions list
   */
  public Assignment(T columnName, List<T> expressions) {
    this.columnName = requireNonNull(columnName, "columnName is null");
    this.expressions = expressions;
  }

  /**
   * Constructor for SET GLOBAL statements only single expression is allowed on right side of
   * assignment
   */
  public Assignment(T columnName, T expression) {
    this.columnName = requireNonNull(columnName, "columnName is null");
    this.expressions = Collections.singletonList(requireNonNull(expression, "expression is null"));
  }

  public T columnName() {
    return columnName;
  }

  public T expression() {
    return expressions.isEmpty() ? null : expressions.get(0);
  }

  public List<T> expressions() {
    return expressions;
  }

  public <U> Assignment<U> map(Function<? super T, ? extends U> mapper) {
    return new Assignment<>(
        mapper.apply(columnName), expressions.stream().map(mapper).collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Assignment<?> that = (Assignment<?>) o;
    return Objects.equals(columnName, that.columnName)
        && Objects.equals(expressions, that.expressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, expressions);
  }

  @Override
  public String toString() {
    return "Assignment{" + "column=" + columnName + ", expressions=" + expressions + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAssignment(this, context);
  }
}
