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

public class RestoreSnapshot<T> extends Statement {

  public enum Mode {
    ALL,
    TABLE,
    METADATA,
    CUSTOM
  }

  private final QualifiedName name;
  private final GenericProperties<T> properties;
  private final Mode mode;
  private final List<String> types;
  private final List<Table<T>> tables;

  public RestoreSnapshot(QualifiedName name, Mode mode, GenericProperties<T> properties) {
    this(name, mode, properties, Collections.emptyList(), Collections.emptyList());
  }

  public RestoreSnapshot(
      QualifiedName name, Mode mode, GenericProperties<T> properties, List<String> types) {
    this(name, mode, properties, types, Collections.emptyList());
  }

  public RestoreSnapshot(
      QualifiedName name,
      Mode mode,
      GenericProperties<T> properties,
      List<String> types,
      List<Table<T>> tables) {
    this.name = name;
    this.mode = mode;
    this.types = types;
    this.tables = tables;
    this.properties = properties;
  }

  public QualifiedName name() {
    return this.name;
  }

  public GenericProperties<T> properties() {
    return properties;
  }

  public Mode mode() {
    return mode;
  }

  public List<String> types() {
    return types;
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
    RestoreSnapshot<?> that = (RestoreSnapshot<?>) o;
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
    return "RestoreSnapshot{"
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
    return visitor.visitRestoreSnapshot(this, context);
  }
}
