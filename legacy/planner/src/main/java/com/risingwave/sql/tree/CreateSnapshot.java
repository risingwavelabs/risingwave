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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CreateSnapshot<T> extends Statement {

  private final QualifiedName name;
  private final GenericProperties<T> properties;
  private final List<Table<T>> tables;

  public CreateSnapshot(QualifiedName name, GenericProperties<T> properties) {
    this.name = name;
    this.properties = properties;
    this.tables = Collections.emptyList();
  }

  public CreateSnapshot(
      QualifiedName name, List<Table<T>> tables, GenericProperties<T> properties) {
    this.name = name;
    this.tables = tables;
    this.properties = properties;
  }

  public QualifiedName name() {
    return this.name;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  public List<Table<T>> tables() {
    return tables;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateSnapshot<?> that = (CreateSnapshot<?>) o;
    return Objects.equals(name, that.name)
        && Objects.equals(properties, that.properties)
        && Objects.equals(tables, that.tables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, properties, tables);
  }

  @Override
  public String toString() {
    return "CreateSnapshot{"
        + "name="
        + name
        + ", properties="
        + properties
        + ", tables="
        + tables
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateSnapshot(this, context);
  }
}
