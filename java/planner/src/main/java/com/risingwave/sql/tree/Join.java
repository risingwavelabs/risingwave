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
import java.util.Optional;

public class Join extends Relation {

  public Join(Type type, Relation left, Relation right, Optional<JoinCriteria> criteria) {
    if (type.equals(Type.CROSS)) {
      if (criteria.isPresent()) {
        throw new IllegalArgumentException("Cross join cannot have join criteria");
      }
    } else {
      if (!criteria.isPresent()) {
        throw new IllegalArgumentException("No join criteria specified");
      }
    }

    this.type = type;
    this.left = requireNonNull(left, "left is null");
    this.right = requireNonNull(right, "right is null");
    this.criteria = criteria;
  }

  public enum Type {
    CROSS,
    INNER,
    LEFT,
    RIGHT,
    FULL
  }

  private final Type type;
  private final Relation left;
  private final Relation right;
  private final Optional<JoinCriteria> criteria;

  public Type getType() {
    return type;
  }

  public Relation getLeft() {
    return left;
  }

  public Relation getRight() {
    return right;
  }

  public Optional<JoinCriteria> getCriteria() {
    return criteria;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Join join = (Join) o;
    return type == join.type
        && Objects.equals(left, join.left)
        && Objects.equals(right, join.right)
        && Objects.equals(criteria, join.criteria);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, left, right, criteria);
  }

  @Override
  public String toString() {
    return "Join{"
        + "type="
        + type
        + ", left="
        + left
        + ", right="
        + right
        + ", criteria="
        + criteria
        + '}';
  }
}
