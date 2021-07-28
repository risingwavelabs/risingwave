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

import java.util.List;
import java.util.Objects;

public class CharFilters<T> extends AnalyzerElement<T> {

  private final List<NamedProperties<T>> charFilters;

  public CharFilters(List<NamedProperties<T>> charFilters) {
    this.charFilters = charFilters;
  }

  public List<NamedProperties<T>> charFilters() {
    return charFilters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CharFilters<?> that = (CharFilters<?>) o;
    return Objects.equals(charFilters, that.charFilters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(charFilters);
  }

  @Override
  public String toString() {
    return "CharFilters{" + "charFilters=" + charFilters + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCharFilters(this, context);
  }
}
