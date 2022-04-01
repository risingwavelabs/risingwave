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

/**
 * The drop view statement extended from CrateDB. Note that we modify the grammar from CrateDB, such
 * that we allow drop only one table once.
 */
public final class DropView extends Statement {

  private final QualifiedName name;
  private final boolean ifExists;
  private final boolean materialized;

  public DropView(QualifiedName name, boolean ifExists, boolean materialized) {
    this.name = name;
    this.ifExists = ifExists;
    this.materialized = materialized;
  }

  public QualifiedName name() {
    return name;
  }

  public boolean ifExists() {
    return ifExists;
  }

  public boolean isMaterialized() {
    return materialized;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DropView dropView = (DropView) o;

    if (ifExists != dropView.ifExists) {
      return false;
    }
    return name.equals(dropView.name);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (ifExists ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DropView{" + "name=" + name + ", ifExists=" + ifExists + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropView(this, context);
  }
}
