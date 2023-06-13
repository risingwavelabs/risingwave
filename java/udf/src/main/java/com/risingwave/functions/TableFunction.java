package com.risingwave.functions;

import java.util.ArrayList;
import java.util.List;

/**
 * Base interface for a user-defined table function. A user-defined table function
 * maps zero, one, or multiple scalar values to zero, one, or multiple rows (or
 * structured types). If an output record consists of only one field, the
 * structured record can be omitted, and a scalar value can be emitted that will
 * be implicitly wrapped into a row by the runtime.
 *
 * <p>
 * The behavior of a {@link TableFunction} can be defined by implementing a
 * custom evaluation method. An evaluation method must be declared publicly, not
 * static, and named <code>eval</code>. Multiple overloaded methods named
 * <code>eval</code> are not supported yet.
 *
 * <p>
 * By default, input and output data types are automatically extracted using
 * reflection. This includes the generic argument {@code T} of the class for
 * determining an output data type. Input arguments are derived from one or more
 * {@code eval()} methods.
 *
 * <p>
 * The following examples show how to specify a table function:
 *
 * <pre>
 * {@code
 * // a function that accepts an INT arguments and emits the range from 0 to the
 * // given number.
 * class Series implements TableFunction<Integer> {
 *     public void eval(int x) {
 *         for (int i = 0; i < n; i++) {
 *             collect(i);
 *         }
 *     }
 * }
 * 
 * // a function that accepts an String arguments and emits the words of the
 * // given string.
 * class Split implements TableFunction<Split.Row> {
 *     public static class Row {
 *         public String word;
 *         public int length;
 *     }
 * 
 *     public void eval(String str) {
 *         for (var s : str.split(" ")) {
 *             Row row = new Row();
 *             row.word = s;
 *             row.length = s.length();
 *             collect(row);
 *         }
 *     }
 * }
 * }</pre>
 */
public abstract interface TableFunction<T> extends UserDefinedFunction {

    /** Collector used to emit rows. */
    List<Object> rows = new ArrayList<>();

    /** Takes all emitted rows. */
    default Object[] take() {
        var result = this.rows.toArray();
        this.rows.clear();
        return result;
    }

    /** Returns the number of emitted rows. */
    default int size() {
        return this.rows.size();
    }

    /** Emits an output row. */
    default void collect(T row) {
        this.rows.add(row);
    }
}
