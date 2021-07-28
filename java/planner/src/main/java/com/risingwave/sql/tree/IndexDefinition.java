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

import com.risingwave.common.collections.Lists2;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class IndexDefinition<T> extends TableElement<T> {

  private final String ident;
  private final String method;
  private final List<T> columns;
  private final GenericProperties<T> properties;

  public IndexDefinition(
      String ident, String method, List<T> columns, GenericProperties<T> properties) {
    this.ident = ident;
    this.method = method;
    this.columns = columns;
    this.properties = properties;
  }

  public String ident() {
    return ident;
  }

  public String method() {
    return method;
  }

  public List<T> columns() {
    return columns;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexDefinition<?> that = (IndexDefinition<?>) o;
    return Objects.equals(ident, that.ident)
        && Objects.equals(method, that.method)
        && Objects.equals(columns, that.columns)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ident, method, columns, properties);
  }

  @Override
  public String toString() {
    return "IndexDefinition{"
        + "ident='"
        + ident
        + '\''
        + ", method='"
        + method
        + '\''
        + ", columns="
        + columns
        + ", properties="
        + properties
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIndexDefinition(this, context);
  }

  @Override
  public <U> TableElement<U> map(Function<? super T, ? extends U> mapper) {
    return new IndexDefinition<>(
        ident, method, Lists2.map(columns, mapper), properties.map(mapper));
  }

  @Override
  public void visit(Consumer<? super T> consumer) {
    columns.forEach(consumer);
    properties.properties().values().forEach(consumer);
  }
}
