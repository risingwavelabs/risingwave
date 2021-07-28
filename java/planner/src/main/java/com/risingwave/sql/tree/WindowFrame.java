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

import java.util.Optional;

public class WindowFrame extends Node {

  public enum Mode {
    RANGE,
    ROWS
  }

  private final Mode mode;
  private final FrameBound start;
  private final Optional<FrameBound> end;

  public WindowFrame(Mode mode, FrameBound start, Optional<FrameBound> end) {
    this.mode = mode;
    this.start = start;
    this.end = end;
  }

  public Mode mode() {
    return mode;
  }

  public FrameBound getStart() {
    return start;
  }

  public Optional<FrameBound> getEnd() {
    return end;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    WindowFrame that = (WindowFrame) o;

    if (mode != that.mode) {
      return false;
    }
    if (!start.equals(that.start)) {
      return false;
    }
    return end.equals(that.end);
  }

  @Override
  public int hashCode() {
    int result = mode.hashCode();
    result = 31 * result + start.hashCode();
    result = 31 * result + end.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "WindowFrame{" + "frameType=" + mode + ", start=" + start + ", end=" + end + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindowFrame(this, context);
  }
}
