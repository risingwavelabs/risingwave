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

import java.util.function.Function;

/** columntype that contains many values of a single type */
public class CollectionColumnType<T> extends ColumnType<T> {

  private final ColumnType<T> innerType;

  public CollectionColumnType(ColumnType<T> innerType) {
    super("ARRAY");
    this.innerType = innerType;
  }

  public ColumnType<T> innerType() {
    return innerType;
  }

  @Override
  public <U> ColumnType<U> map(Function<? super T, ? extends U> mapper) {
    return new CollectionColumnType<>(innerType.map(mapper));
  }

  @Override
  public <U> ColumnType<U> mapExpressions(
      ColumnType<U> mappedType, Function<? super T, ? extends U> mapper) {
    CollectionColumnType<U> collectionColumnType = (CollectionColumnType<U>) mappedType;
    return new CollectionColumnType<>(
        innerType.mapExpressions(collectionColumnType.innerType, mapper));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    CollectionColumnType that = (CollectionColumnType) o;

    return innerType.equals(that.innerType);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + innerType.hashCode();
    return result;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCollectionColumnType(this, context);
  }
}
