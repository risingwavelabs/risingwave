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

public abstract class PrivilegeStatement extends Statement {

  protected final List<String> userNames;
  protected final List<String> privilegeTypes;
  private final List<QualifiedName> tableOrSchemaNames;
  private final String clazz;
  protected final boolean all;

  public PrivilegeStatement(
      List<String> userNames, String clazz, List<QualifiedName> tableOrSchemaNames) {
    this.userNames = userNames;
    privilegeTypes = Collections.emptyList();
    all = true;
    this.clazz = clazz;
    this.tableOrSchemaNames = tableOrSchemaNames;
  }

  public PrivilegeStatement(
      List<String> userNames,
      List<String> privilegeTypes,
      String clazz,
      List<QualifiedName> tableOrSchemaNames) {
    this.userNames = userNames;
    this.privilegeTypes = privilegeTypes;
    this.all = false;
    this.clazz = clazz;
    this.tableOrSchemaNames = tableOrSchemaNames;
  }

  public List<String> privileges() {
    return privilegeTypes;
  }

  public List<String> userNames() {
    return userNames;
  }

  public boolean all() {
    return all;
  }

  public List<QualifiedName> privilegeIdents() {
    return tableOrSchemaNames;
  }

  public String clazz() {
    return clazz;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrivilegeStatement that = (PrivilegeStatement) o;
    return all == that.all
        && Objects.equals(userNames, that.userNames)
        && Objects.equals(privilegeTypes, that.privilegeTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(userNames, privilegeTypes, all);
  }
}
