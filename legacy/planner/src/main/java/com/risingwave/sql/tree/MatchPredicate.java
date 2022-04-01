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
import javax.annotation.Nullable;

public class MatchPredicate extends Expression {

  private final List<MatchPredicateColumnIdent> idents;
  private final Expression value;
  private final GenericProperties<Expression> properties;
  private final String matchType;

  public MatchPredicate(
      List<MatchPredicateColumnIdent> idents,
      Expression value,
      @Nullable String matchType,
      GenericProperties<Expression> properties) {
    if (idents.isEmpty()) {
      throw new IllegalArgumentException("at least one ident must be given");
    }
    this.idents = idents;
    this.value = requireNonNull(value, "query_term is null");
    this.matchType = matchType;
    this.properties = properties;
  }

  public List<MatchPredicateColumnIdent> idents() {
    return idents;
  }

  public Expression value() {
    return value;
  }

  @Nullable
  public String matchType() {
    return matchType;
  }

  public GenericProperties<Expression> properties() {
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
    MatchPredicate that = (MatchPredicate) o;
    return Objects.equals(idents, that.idents)
        && Objects.equals(value, that.value)
        && Objects.equals(properties, that.properties)
        && Objects.equals(matchType, that.matchType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(idents, value, properties, matchType);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitMatchPredicate(this, context);
  }
}
