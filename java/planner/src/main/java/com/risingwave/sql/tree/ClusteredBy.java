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
import java.util.function.Function;

public final class ClusteredBy<T> extends Node {

  private final Optional<T> column;
  private final Optional<T> numberOfShards;

  public ClusteredBy(Optional<T> column, Optional<T> numberOfShards) {
    this.column = column;
    this.numberOfShards = numberOfShards;
  }

  public Optional<T> column() {
    return column;
  }

  public Optional<T> numberOfShards() {
    return numberOfShards;
  }

  public <U> ClusteredBy<U> map(Function<? super T, ? extends U> mapper) {
    return new ClusteredBy<>(column.map(mapper), numberOfShards.map(mapper));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusteredBy<?> that = (ClusteredBy<?>) o;
    return Objects.equals(column, that.column)
        && Objects.equals(numberOfShards, that.numberOfShards);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, numberOfShards);
  }

  @Override
  public String toString() {
    return "ClusteredBy{" + "column=" + column + ", number of shards=" + numberOfShards + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitClusteredBy(this, context);
  }
}
