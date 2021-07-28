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

public class AlterTable<T> extends Statement {

  private final Table<T> table;
  private final GenericProperties<T> genericProperties;
  private final List<String> resetProperties;

  public AlterTable(Table<T> table, GenericProperties<T> genericProperties) {
    this.table = table;
    this.genericProperties = genericProperties;
    this.resetProperties = Collections.emptyList();
  }

  public AlterTable(Table<T> table, List<String> resetProperties) {
    this.table = table;
    this.resetProperties = resetProperties;
    this.genericProperties = GenericProperties.empty();
  }

  private AlterTable(
      Table<T> table, GenericProperties<T> genericProperties, List<String> resetProperties) {
    this.table = table;
    this.genericProperties = genericProperties;
    this.resetProperties = resetProperties;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAlterTable(this, context);
  }

  public Table<T> table() {
    return table;
  }

  public GenericProperties<T> genericProperties() {
    return genericProperties;
  }

  public List<String> resetProperties() {
    return resetProperties;
  }

  public <U> AlterTable<U> map(Function<? super T, ? extends U> mapper) {
    return new AlterTable<>(table.map(mapper), genericProperties.map(mapper), resetProperties);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlterTable<?> that = (AlterTable<?>) o;
    return Objects.equals(table, that.table)
        && Objects.equals(genericProperties, that.genericProperties)
        && Objects.equals(resetProperties, that.resetProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, genericProperties, resetProperties);
  }

  @Override
  public String toString() {
    return "AlterTable{"
        + "table="
        + table
        + ", genericProperties="
        + genericProperties
        + ", resetProperties="
        + resetProperties
        + '}';
  }
}
