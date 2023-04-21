package com.risingwave.sourcenode.common;

import io.grpc.Status;

public abstract class ValidatorUtils {
    public static RuntimeException invalidArgument(String descrption) {
        return Status.INVALID_ARGUMENT.withDescription(descrption).asRuntimeException();
    }

    public static RuntimeException internalError(String descrption) {
        return Status.INTERNAL.withDescription(descrption).asRuntimeException();
    }
}
