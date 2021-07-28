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

public class AliasedRelation extends Relation {

  private final Relation relation;
  private final String alias;
  private final List<String> columnNames;

  public AliasedRelation(Relation relation, String alias, List<String> columnNames) {
    this.relation = requireNonNull(relation, "relation is null");
    this.alias = requireNonNull(alias, "alias is null");
    this.columnNames = columnNames;
  }

  public Relation getRelation() {
    return relation;
  }

  public String getAlias() {
    return alias;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAliasedRelation(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AliasedRelation that = (AliasedRelation) o;
    return Objects.equals(relation, that.relation)
        && Objects.equals(alias, that.alias)
        && Objects.equals(columnNames, that.columnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, alias, columnNames);
  }

  @Override
  public String toString() {
    return "AliasedRelation{"
        + "relation="
        + relation
        + ", alias='"
        + alias
        + '\''
        + ", columnNames="
        + columnNames
        + '}';
  }
}
