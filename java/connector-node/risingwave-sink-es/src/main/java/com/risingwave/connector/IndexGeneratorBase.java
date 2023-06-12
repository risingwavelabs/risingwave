package com.risingwave.connector;

import java.util.Objects;

/** Base class for {@link IndexGenerator}. */
public abstract class IndexGeneratorBase implements IndexGenerator {

    private static final long serialVersionUID = 1L;
    protected final String index;

    public IndexGeneratorBase(String index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexGeneratorBase)) {
            return false;
        }
        IndexGeneratorBase that = (IndexGeneratorBase) o;
        return index.equals(that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
