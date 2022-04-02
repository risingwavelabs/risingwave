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
import java.util.Optional;
import javax.annotation.Nullable;

public final class Window extends Statement {

  private final String windowRef;
  private final List<Expression> partitions;
  private final List<SortItem> orderBy;
  private final Optional<WindowFrame> windowFrame;

  public Window(
      @Nullable String windowRef,
      List<Expression> partitions,
      List<SortItem> orderBy,
      Optional<WindowFrame> windowFrame) {
    this.partitions = partitions;
    this.orderBy = orderBy;
    this.windowFrame = windowFrame;
    this.windowRef = windowRef;
  }

  @Nullable
  public String windowRef() {
    return windowRef;
  }

  public List<Expression> getPartitions() {
    return partitions;
  }

  public List<SortItem> getOrderBy() {
    return orderBy;
  }

  public Optional<WindowFrame> getWindowFrame() {
    return windowFrame;
  }

  /**
   * Merges the provided window definition into the current one by following the next merge rules:
   *
   * <ul>
   *   <li>The current window must not specify the partition by clause.
   *   <li>The provided window must not specify the window frame, if the current window definition
   *       is not empty.
   *   <li>The provided window cannot override the order by clause or window frame.
   * </ul>
   *
   * @return A new {@link Window} window definition that contains merged elements of both current
   *     and provided windows or a provided window definition if the current definition is empty.
   * @throws IllegalArgumentException If the merge rules are violated.
   */
  public Window merge(Window that) {
    if (this.empty()) {
      return that;
    }

    final List<Expression> partitionBy;
    if (!this.partitions.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot override PARTITION BY clause of window " + this.windowRef);
    } else {
      partitionBy = that.getPartitions();
    }

    final List<SortItem> orderBy;
    if (that.getOrderBy().isEmpty()) {
      orderBy = this.getOrderBy();
    } else {
      if (!this.getOrderBy().isEmpty()) {
        throw new IllegalArgumentException(
            "Cannot override ORDER BY clause of window " + this.windowRef);
      }
      orderBy = that.getOrderBy();
    }

    if (that.getWindowFrame().isPresent()) {
      throw new IllegalArgumentException(
          "Cannot copy window " + this.windowRef() + " because it has a frame clause");
    }

    return new Window(that.windowRef, partitionBy, orderBy, this.getWindowFrame());
  }

  private boolean empty() {
    return partitions.isEmpty() && orderBy.isEmpty() && !windowFrame.isPresent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Window window = (Window) o;

    if (!partitions.equals(window.partitions)) {
      return false;
    }
    if (!orderBy.equals(window.orderBy)) {
      return false;
    }
    return windowFrame.equals(window.windowFrame);
  }

  @Override
  public int hashCode() {
    int result = partitions.hashCode();
    result = 31 * result + orderBy.hashCode();
    result = 31 * result + windowFrame.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Window{"
        + "partitions="
        + partitions
        + ", orderBy="
        + orderBy
        + ", windowFrame="
        + windowFrame
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }
}
