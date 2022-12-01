package com.risingwave.binding;

public class NextResult {
    private final byte[] key;
    private final byte[] value;

    private final boolean isNone;

    public NextResult() {
        this.key = null;
        this.value = null;
        this.isNone = true;
    }

    public NextResult(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
        this.isNone = false;
    }

    public boolean isNone() {
        return isNone;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }


}
