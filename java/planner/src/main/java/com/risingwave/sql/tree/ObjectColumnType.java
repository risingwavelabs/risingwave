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

import com.risingwave.common.collections.Lists2;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

public class ObjectColumnType<T> extends ColumnType<T> {

  private final Optional<ColumnPolicy> objectType;
  private final List<ColumnDefinition<T>> nestedColumns;

  public ObjectColumnType(@Nullable String objectType, List<ColumnDefinition<T>> nestedColumns) {
    super("object");
    this.objectType =
        objectType == null ? Optional.empty() : Optional.of(ColumnPolicy.of(objectType));
    this.nestedColumns = nestedColumns;
  }

  public Optional<ColumnPolicy> objectType() {
    return objectType;
  }

  public List<ColumnDefinition<T>> nestedColumns() {
    return nestedColumns;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitObjectColumnType(this, context);
  }

  @Override
  public <U> ObjectColumnType<U> map(Function<? super T, ? extends U> mapper) {
    String objectTypeString = null;
    if (objectType.isPresent()) {
      objectTypeString = objectType.get().lowerCaseName();
    }
    return new ObjectColumnType<>(objectTypeString, Lists2.map(nestedColumns, x -> x.map(mapper)));
  }

  @Override
  public <U> ColumnType<U> mapExpressions(
      ColumnType<U> mappedType, Function<? super T, ? extends U> mapper) {
    ObjectColumnType<U> mappedObjectType = (ObjectColumnType<U>) mappedType;
    String objectTypeString = null;
    if (objectType.isPresent()) {
      objectTypeString = objectType.get().lowerCaseName();
    }
    ArrayList<ColumnDefinition<U>> nestedMappedColumns = new ArrayList<>(nestedColumns.size());
    for (int i = 0; i < nestedColumns.size(); i++) {
      ColumnDefinition<U> columnDefinition =
          (ColumnDefinition<U>)
              nestedColumns.get(i).mapExpressions(mappedObjectType.nestedColumns.get(i), mapper);
      nestedMappedColumns.add(columnDefinition);
    }
    return new ObjectColumnType<>(objectTypeString, nestedMappedColumns);
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

    ObjectColumnType that = (ObjectColumnType) o;

    if (!objectType.equals(that.objectType)) {
      return false;
    }
    return nestedColumns.equals(that.nestedColumns);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + objectType.hashCode();
    result = 31 * result + nestedColumns.hashCode();
    return result;
  }
}
