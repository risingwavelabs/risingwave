package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkRow;

import java.io.Serializable;

/** This interface is responsible to generate index name from given {@link SinkRow} record. */

interface IndexGenerator extends Serializable {

    /**
     * Initialize the index generator, this will be called only once before {@link
     * #generate(SinkRow)} is called.
     */
    default void open() {}

    /** Generate index name according to the given row. */
    String generate(SinkRow row);
}
