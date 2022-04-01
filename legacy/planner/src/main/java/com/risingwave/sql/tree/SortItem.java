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

public class SortItem extends Node {

  public enum Ordering {
    ASCENDING,
    DESCENDING
  }

  public enum NullOrdering {
    FIRST,
    LAST,
    UNDEFINED
  }

  private final Expression sortKey;
  private final Ordering ordering;
  private final NullOrdering nullOrdering;

  public SortItem(Expression sortKey, Ordering ordering, NullOrdering nullOrdering) {
    this.ordering = ordering;
    this.sortKey = sortKey;
    this.nullOrdering = nullOrdering;
  }

  public Expression getSortKey() {
    return sortKey;
  }

  public Ordering getOrdering() {
    return ordering;
  }

  public NullOrdering getNullOrdering() {
    return nullOrdering;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSortItem(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SortItem sortItem = (SortItem) o;
    return Objects.equals(sortKey, sortItem.sortKey)
        && ordering == sortItem.ordering
        && nullOrdering == sortItem.nullOrdering;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortKey, ordering, nullOrdering);
  }

  @Override
  public String toString() {
    return "SortItem{"
        + "sortKey="
        + sortKey
        + ", ordering="
        + ordering
        + ", nullOrdering="
        + nullOrdering
        + '}';
  }
}
