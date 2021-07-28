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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class Table<T> extends QueryBody {

  private final QualifiedName name;
  private final boolean excludePartitions;
  private final List<Assignment<T>> partitionProperties;

  public Table(QualifiedName name) {
    this(name, true);
  }

  public Table(QualifiedName name, boolean excludePartitions) {
    this.name = name;
    this.excludePartitions = excludePartitions;
    this.partitionProperties = Collections.emptyList();
  }

  public Table(QualifiedName name, List<Assignment<T>> partitionProperties) {
    this.name = name;
    this.excludePartitions = false;
    this.partitionProperties = partitionProperties;
  }

  public QualifiedName getName() {
    return name;
  }

  public boolean excludePartitions() {
    return excludePartitions;
  }

  public List<Assignment<T>> partitionProperties() {
    return partitionProperties;
  }

  public <U> Table<U> map(Function<? super T, ? extends U> mapper) {
    if (partitionProperties.isEmpty()) {
      return new Table<>(name, excludePartitions);
    } else {
      return new Table<>(name, Lists2.map(partitionProperties, x -> x.map(mapper)));
    }
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTable(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Table<?> table = (Table<?>) o;
    return Objects.equals(name, table.name)
        && Objects.equals(partitionProperties, table.partitionProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, partitionProperties);
  }

  @Override
  public String toString() {
    return "Table{"
        + "only="
        + excludePartitions
        + ", "
        + name
        + ", partitionProperties="
        + partitionProperties
        + '}';
  }
}
