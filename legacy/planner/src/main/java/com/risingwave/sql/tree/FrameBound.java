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

import static com.risingwave.common.collections.Lists2.findFirstGteProbeValue;
import static com.risingwave.common.collections.Lists2.findFirstLteProbeValue;
import static com.risingwave.common.collections.Lists2.findFirstNonPeer;
import static com.risingwave.common.collections.Lists2.findFirstPreviousPeer;
import static com.risingwave.sql.tree.WindowFrame.Mode.ROWS;

import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;

public class FrameBound extends Node {

  public enum Type {
    UNBOUNDED_PRECEDING {
      @Override
      public <T> int getStart(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        return pStart;
      }

      @Override
      public <T> int getEnd(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        throw new IllegalStateException("UNBOUNDED PRECEDING cannot be the start of a frame");
      }
    },
    /**
     *
     *
     * <pre>{@code
     * { ROWS | RANGE } <offset> PRECEDING
     *
     * ROWS mode:
     *   ROWS <offset> PRECEDING
     *   the start of the frame is *literally* <offset> number of rows before the current row
     *
     * RANGE MODE:
     *   ORDER BY x RANGE <offset> PRECEDING
     *   Every row before the current row where the value for `x` is >= `x@currentRow - <offset>`
     *   is within the frame
     * }</pre>
     */
    PRECEDING {
      @Override
      public <T> int getStart(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        if (mode == ROWS) {
          assert offset instanceof Number
              : "In ROWS mode the offset must be a non-null, non-negative number";
          return Math.max(pStart, currentRowIdx - ((Number) offset).intValue());
        } else {
          int firstGteProbeValue =
              findFirstGteProbeValue(rows, pStart, currentRowIdx, offsetProbeValue, cmp);
          if (firstGteProbeValue == -1) {
            return currentRowIdx;
          } else {
            return firstGteProbeValue;
          }
        }
      }

      @Override
      public <T> int getEnd(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        throw new UnsupportedOperationException(
            "`<offset> PRECEDING` cannot be used to calculate the end of a window frame");
      }
    },
    /*
     * In RANGE mode:
     *    - Frame starts with the current row's first peer (Row that is equal based on the ORDER BY
     *      clause)
     *    - Frame ends with the current row's first peer
     * In ROWS mode:
     *    - The current row
     */
    CURRENT_ROW {
      @Override
      public <T> int getStart(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        if (mode == ROWS) {
          return currentRowIdx;
        }

        if (pStart == currentRowIdx) {
          return pStart;
        } else {
          if (cmp != null) {
            return Math.max(pStart, findFirstPreviousPeer(rows, currentRowIdx, cmp));
          } else {
            return currentRowIdx;
          }
        }
      }

      @Override
      public <T> int getEnd(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        if (mode == ROWS) {
          return currentRowIdx + 1;
        }

        return findFirstNonPeer(rows, currentRowIdx, pEnd, cmp);
      }
    },
    /**
     *
     *
     * <pre>{@code
     * { ROWS | RANGE } [..] <offset> FOLLOWING
     *
     * ROWS mode:
     *   ROWS [...] <offset> FOLLOWING
     *   the end of the frame is *literally* <offset> number of rows after the current row
     *
     * RANGE MODE:
     *   ORDER BY x RANGE [...] <offset> FOLLOWING
     *   Every row after the current row where the value for `x` is <= `x@currentRow + <offset>` is
     *   within the frame
     * }</pre>
     */
    FOLLOWING {
      @Override
      public <T> int getStart(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        throw new UnsupportedOperationException(
            "`<offset> FOLLOWING` cannot be used to calculate the start of a window frame");
      }

      @Override
      public <T> int getEnd(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        // end index is exclusive so we increment it by one when finding the interval end index
        if (mode == ROWS) {
          assert offset instanceof Number
              : "In ROWS mode the offset must be a non-null, non-negative number";
          return Math.min(pEnd, currentRowIdx + ((Number) offset).intValue() + 1);
        } else {
          return findFirstLteProbeValue(rows, pEnd, currentRowIdx, offsetProbeValue, cmp) + 1;
        }
      }
    },
    UNBOUNDED_FOLLOWING {
      @Override
      public <T> int getStart(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        throw new IllegalStateException("UNBOUNDED FOLLOWING cannot be the start of a frame");
      }

      @Override
      public <T> int getEnd(
          WindowFrame.Mode mode,
          int pStart,
          int pEnd,
          int currentRowIdx,
          @Nullable Object offset,
          @Nullable T offsetProbeValue,
          @Nullable Comparator<T> cmp,
          List<T> rows) {
        return pEnd;
      }
    };

    public abstract <T> int getStart(
        WindowFrame.Mode mode,
        int pStart,
        int pEnd,
        int currentRowIdx,
        @Nullable Object offset,
        @Nullable T offsetProbeValue,
        @Nullable Comparator<T> cmp,
        List<T> rows);

    public abstract <T> int getEnd(
        WindowFrame.Mode mode,
        int pStart,
        int pEnd,
        int currentRowIdx,
        @Nullable Object offset,
        @Nullable T offsetProbeValue,
        @Nullable Comparator<T> cmp,
        List<T> rows);
  }

  private final Type type;

  @Nullable private final Expression value;

  public FrameBound(Type type) {
    this(type, null);
  }

  public FrameBound(Type type, @Nullable Expression value) {
    this.type = type;
    this.value = value;
  }

  public Type getType() {
    return type;
  }

  @Nullable
  public Expression getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FrameBound that = (FrameBound) o;

    if (type != that.type) {
      return false;
    }
    return value != null ? value.equals(that.value) : that.value == null;
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "FrameBound{" + "type=" + type + ", value=" + value + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFrameBound(this, context);
  }
}
