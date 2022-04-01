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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Update extends Statement {

  private final Relation relation;
  private final List<Assignment<Expression>> assignments;
  private final Optional<Expression> where;
  private final List<SelectItem> returning;

  public Update(
      Relation relation,
      List<Assignment<Expression>> assignments,
      Optional<Expression> where,
      List<SelectItem> returning) {
    this.relation = requireNonNull(relation, "relation is null");
    this.assignments = assignments;
    this.where = where;
    this.returning = returning;
  }

  public Relation relation() {
    return relation;
  }

  public List<Assignment<Expression>> assignments() {
    return assignments;
  }

  public Optional<Expression> whereClause() {
    return where;
  }

  public List<SelectItem> returningClause() {
    return returning;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Update update = (Update) o;
    return Objects.equals(relation, update.relation)
        && Objects.equals(assignments, update.assignments)
        && Objects.equals(where, update.where)
        && Objects.equals(returning, update.returning);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, assignments, where, returning);
  }

  @Override
  public String toString() {
    return "Update{"
        + "relation="
        + relation
        + ", assignments="
        + assignments
        + ", where="
        + where
        + ", returning="
        + returning
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitUpdate(this, context);
  }
}
