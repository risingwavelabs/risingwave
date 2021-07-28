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

public final class CreateView extends Statement {

  private final QualifiedName name;
  private final Query query;
  private final boolean replaceExisting;

  public CreateView(QualifiedName name, Query query, boolean replaceExisting) {
    this.name = name;
    this.query = query;
    this.replaceExisting = replaceExisting;
  }

  public QualifiedName name() {
    return name;
  }

  public Query query() {
    return query;
  }

  public boolean replaceExisting() {
    return replaceExisting;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CreateView that = (CreateView) o;

    if (replaceExisting != that.replaceExisting) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return query.equals(that.query);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + query.hashCode();
    result = 31 * result + (replaceExisting ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "CreateView{"
        + "name="
        + name
        + ", query="
        + query
        + ", replaceExisting="
        + replaceExisting
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateView(this, context);
  }
}
