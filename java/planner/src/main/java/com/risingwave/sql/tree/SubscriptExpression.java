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

import java.util.Objects;

/**
 *
 *
 * <pre>{@code
 *     <base>[<index>]
 *
 * Examples:
 *
 *     obj['fieldName']
 *     arr[2]
 * }</pre>
 */
public class SubscriptExpression extends Expression {

  private Expression base;
  private Expression index;

  public SubscriptExpression(Expression base, Expression index) {
    this.base = base;
    this.index = index;
  }

  public Expression base() {
    return base;
  }

  public Expression index() {
    return index;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSubscriptExpression(this, context);
  }

  @Override
  public int hashCode() {
    int result = base != null ? base.hashCode() : 0;
    result = 31 * result + (index != null ? index.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SubscriptExpression that = (SubscriptExpression) o;

    if (!Objects.equals(index, that.index)) {
      return false;
    }

    return Objects.equals(base, that.base);
  }
}
