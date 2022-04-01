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

public class Union extends SetOperation {

  private final Relation left;
  private final Relation right;
  private final boolean isDistinct;

  public Union(Relation left, Relation right, boolean isDistinct) {
    this.left = requireNonNull(left, "relation must not be null");
    this.right = requireNonNull(right, "relation must not be null");
    this.isDistinct = isDistinct;
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitUnion(this, context);
  }

  @Override
  public String toString() {
    return "Union{" + "left=" + left + ", right=" + right + ", isDistinct=" + isDistinct + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Union union = (Union) o;
    return isDistinct == union.isDistinct
        && Objects.equals(left, union.left)
        && Objects.equals(right, union.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right, isDistinct);
  }
}
