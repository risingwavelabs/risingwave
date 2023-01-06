package com.risingwave.java.binding;

public class Binding {
    static {
        System.loadLibrary("risingwave_java_binding");
    }

    // iterator method
    // Return a pointer to the iterator
    static native long iteratorNew();

    // return a pointer to the next record
    static native long iteratorNext(long pointer);

    static native void iteratorClose(long pointer);

    // record method
    static native byte[] recordGetKey(long pointer);

    static native boolean recordIsNull(long pointer, int index);

    static native long recordGetInt64Value(long pointer, int index);

    static native String recordGetStringValue(long pointer, int index);

    static native void recordClose(long pointer);
}
