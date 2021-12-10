package com.risingwave.common.exception;

import com.google.protobuf.InvalidProtocolBufferException;
import com.risingwave.proto.common.Status;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

/** PgException is the postgres-specific exception. Every instance binds with an error code. */
public class PgException extends RuntimeException {
  private static final String RW_ERROR_GRPC_HEADER = "risingwave-error-bin";

  private static final Metadata.BinaryMarshaller<Status> STATUS_BINARY_MARSHALLER =
      new Metadata.BinaryMarshaller<Status>() {
        @Override
        public byte[] toBytes(Status value) {
          return value.toByteArray();
        }

        @Override
        public Status parseBytes(byte[] serialized) {
          try {
            return Status.parseFrom(serialized);
          } catch (InvalidProtocolBufferException e) {
            throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
          }
        }
      };

  private final PgErrorCode code;

  public PgException(PgErrorCode code, String format, Object... args) {
    super(String.format(format, args));
    this.code = code;
  }

  /** Construct from an existing exception, IOException, e.g. */
  public PgException(PgErrorCode code, Throwable exp) {
    super(exp);
    this.code = code;
  }

  public PgErrorCode getCode() {
    return code;
  }

  public static PgException from(Exception e) {
    if (e instanceof StatusRuntimeException) {
      return PgException.fromGrpcException((StatusRuntimeException) e);
    }

    return new PgException(PgErrorCode.INTERNAL_ERROR, e);
  }

  public static PgException fromGrpcException(StatusRuntimeException grpcException) {
    var metadata = grpcException.getTrailers();
    if (metadata != null) {
      var key = Metadata.Key.of(RW_ERROR_GRPC_HEADER, STATUS_BINARY_MARSHALLER);
      if (metadata.containsKey(key)) {
        var status = metadata.get(key);
        return new PgException(PgErrorCode.INTERNAL_ERROR, status.getMessage());
      }
    }

    return new PgException(PgErrorCode.INTERNAL_ERROR, grpcException);
  }
}
