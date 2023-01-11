package com.risingwave.java.binding;

public class Binding {
    static {
        System.loadLibrary("risingwave_java_binding");
    }

    // iterator method
    // Return a pointer to the iterator
    static native long iteratorNew();

    // return a pointer to the next row
    static native long iteratorNext(long pointer);

    static native void iteratorClose(long pointer);

    // row method
    static native byte[] rowGetKey(long pointer);

    static native boolean rowIsNull(long pointer, int index);

    static native long rowGetInt64Value(long pointer, int index);

    static native String rowGetStringValue(long pointer, int index);

    static native void rowClose(long pointer);
}
