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

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class TableElement<T> extends Node {

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableElement(this, context);
  }

  /**
   * Map all generic properties using the given mapper. If a table element contains any expressions
   * which should not be mapped in general (e.g. generated/default expression), NULL them here and
   * use the concrete {@link #mapExpressions(TableElement, Function)} to map them using a concrete
   * mapper.
   */
  public abstract <U> TableElement<U> map(Function<? super T, ? extends U> mapper);

  /**
   * Map any expressions which were NOT processed by the {@link #map(Function)} function in general
   * like e.g. generated or default expressions.
   *
   * @param mappedElement An already mapped table element were some expressions were left out.
   * @param mapper The mapper function
   * @return The mapped table element including possible newly mapped expressions.
   */
  public <U> TableElement<U> mapExpressions(
      TableElement<U> mappedElement, Function<? super T, ? extends U> mapper) {
    return mappedElement;
  }

  public abstract void visit(Consumer<? super T> consumer);
}
