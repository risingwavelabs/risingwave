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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QuerySpecification extends QueryBody {

  private final Select select;
  private final List<Relation> from;
  private final Optional<Expression> where;
  private final List<Expression> groupBy;
  private final Optional<Expression> having;
  private final List<SortItem> orderBy;
  private final Optional<Expression> limit;
  private final Optional<Expression> offset;
  private final Map<String, Window> windows;

  public QuerySpecification(
      Select select,
      List<Relation> from,
      Optional<Expression> where,
      List<Expression> groupBy,
      Optional<Expression> having,
      Map<String, Window> windows,
      List<SortItem> orderBy,
      Optional<Expression> limit,
      Optional<Expression> offset) {
    this.select = requireNonNull(select, "select is null");
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.having = having;
    this.windows = windows;
    this.orderBy = orderBy;
    this.limit = limit;
    this.offset = offset;
  }

  public Select getSelect() {
    return select;
  }

  public List<Relation> getFrom() {
    return from;
  }

  public Optional<Expression> getWhere() {
    return where;
  }

  public List<Expression> getGroupBy() {
    return groupBy;
  }

  public Optional<Expression> getHaving() {
    return having;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<Expression> getLimit() {
    return limit;
  }

  public Optional<Expression> getOffset() {
    return offset;
  }

  public Map<String, Window> getWindows() {
    return windows;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitQuerySpecification(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QuerySpecification that = (QuerySpecification) o;
    return Objects.equals(select, that.select)
        && Objects.equals(from, that.from)
        && Objects.equals(where, that.where)
        && Objects.equals(groupBy, that.groupBy)
        && Objects.equals(having, that.having)
        && Objects.equals(orderBy, that.orderBy)
        && Objects.equals(limit, that.limit)
        && Objects.equals(offset, that.offset)
        && Objects.equals(windows, that.windows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(select, from, where, groupBy, having, orderBy, limit, offset, windows);
  }

  @Override
  public String toString() {
    return "QuerySpecification{"
        + "select="
        + select
        + ", from="
        + from
        + ", where="
        + where
        + ", groupBy="
        + groupBy
        + ", having="
        + having
        + ", orderBy="
        + orderBy
        + ", limit="
        + limit
        + ", offset="
        + offset
        + ", windows="
        + windows
        + '}';
  }
}
