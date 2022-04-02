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
import java.util.function.Consumer;
import java.util.function.Function;

public class IndexColumnConstraint<T> extends ColumnConstraint<T> {

  private static final IndexColumnConstraint<?> OFF =
      new IndexColumnConstraint<>("OFF", GenericProperties.empty());

  public static <T> IndexColumnConstraint<T> off() {
    return (IndexColumnConstraint<T>) OFF;
  }

  private final String indexMethod;
  private final GenericProperties<T> properties;

  public IndexColumnConstraint(String indexMethod, GenericProperties<T> properties) {
    this.indexMethod = indexMethod;
    this.properties = properties;
  }

  public String indexMethod() {
    return indexMethod;
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
    IndexColumnConstraint<?> that = (IndexColumnConstraint<?>) o;
    return Objects.equals(indexMethod, that.indexMethod)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexMethod, properties);
  }

  @Override
  public String toString() {
    return "IndexColumnConstraint{"
        + "method='"
        + indexMethod
        + '\''
        + ", properties="
        + properties
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIndexColumnConstraint(this, context);
  }

  @Override
  public <U> ColumnConstraint<U> map(Function<? super T, ? extends U> mapper) {
    return new IndexColumnConstraint<>(indexMethod, properties.map(mapper));
  }

  @Override
  public void visit(Consumer<? super T> consumer) {
    properties.properties().values().forEach(consumer);
  }
}
