package com.risingwave.binding;

public class Binding {
    static {
        System.loadLibrary("risingwave_java_binding");
    }

    // Return a pointer to the iterator
    static native long iteratorNew();

    static native NextResult iteratorNext(long pointer);

    static native void iteratorClose(long pointer);
}
