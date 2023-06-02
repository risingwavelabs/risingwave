package com.risingwave.functions;

import java.util.ArrayList;
import java.util.List;

public abstract class TableFunction<T> extends UserDefinedFunction {

    // Collector used to emit rows.
    private transient List<Object> rows = new ArrayList<>();

    // Takes all emitted rows.
    public final Object[] take() {
        var result = this.rows.toArray();
        this.rows.clear();
        return result;
    }

    // Returns the number of emitted rows.
    public final int size() {
        return this.rows.size();
    }

    // Emits an (implicit or explicit) output row.
    protected final void collect(T row) {
        this.rows.add(row);
    }
}
