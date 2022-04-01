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
import java.util.function.Function;

public class OptimizeStatement<T> extends Statement {

  private final List<Table<T>> tables;
  private final GenericProperties<T> properties;

  public OptimizeStatement(List<Table<T>> tables, GenericProperties<T> properties) {
    this.tables = tables;
    this.properties = properties;
  }

  public List<Table<T>> tables() {
    return tables;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  public <U> OptimizeStatement<U> map(Function<? super T, ? extends U> mapper) {
    return new OptimizeStatement<>(Lists2.map(tables, x -> x.map(mapper)), properties.map(mapper));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OptimizeStatement<?> that = (OptimizeStatement<?>) o;
    return Objects.equals(tables, that.tables) && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tables, properties);
  }

  @Override
  public String toString() {
    return "OptimizeStatement{" + "tables=" + tables + ", properties=" + properties + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitOptimizeStatement(this, context);
  }
}
