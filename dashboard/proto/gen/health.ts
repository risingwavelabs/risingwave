/* eslint-disable */

export const protobufPackage = "health";

export interface HealthCheckRequest {
  service: string;
}

export interface HealthCheckResponse {
  status: HealthCheckResponse_ServingStatus;
}

export const HealthCheckResponse_ServingStatus = {
  UNKNOWN: "UNKNOWN",
  SERVING: "SERVING",
  NOT_SERVING: "NOT_SERVING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type HealthCheckResponse_ServingStatus =
  typeof HealthCheckResponse_ServingStatus[keyof typeof HealthCheckResponse_ServingStatus];

export function healthCheckResponse_ServingStatusFromJSON(object: any): HealthCheckResponse_ServingStatus {
  switch (object) {
    case 0:
    case "UNKNOWN":
      return HealthCheckResponse_ServingStatus.UNKNOWN;
    case 1:
    case "SERVING":
      return HealthCheckResponse_ServingStatus.SERVING;
    case 2:
    case "NOT_SERVING":
      return HealthCheckResponse_ServingStatus.NOT_SERVING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return HealthCheckResponse_ServingStatus.UNRECOGNIZED;
  }
}

export function healthCheckResponse_ServingStatusToJSON(object: HealthCheckResponse_ServingStatus): string {
  switch (object) {
    case HealthCheckResponse_ServingStatus.UNKNOWN:
      return "UNKNOWN";
    case HealthCheckResponse_ServingStatus.SERVING:
      return "SERVING";
    case HealthCheckResponse_ServingStatus.NOT_SERVING:
      return "NOT_SERVING";
    case HealthCheckResponse_ServingStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

function createBaseHealthCheckRequest(): HealthCheckRequest {
  return { service: "" };
}

export const HealthCheckRequest = {
  fromJSON(object: any): HealthCheckRequest {
    return { service: isSet(object.service) ? String(object.service) : "" };
  },

  toJSON(message: HealthCheckRequest): unknown {
    const obj: any = {};
    message.service !== undefined && (obj.service = message.service);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HealthCheckRequest>, I>>(object: I): HealthCheckRequest {
    const message = createBaseHealthCheckRequest();
    message.service = object.service ?? "";
    return message;
  },
};

function createBaseHealthCheckResponse(): HealthCheckResponse {
  return { status: HealthCheckResponse_ServingStatus.UNKNOWN };
}

export const HealthCheckResponse = {
  fromJSON(object: any): HealthCheckResponse {
    return {
      status: isSet(object.status)
        ? healthCheckResponse_ServingStatusFromJSON(object.status)
        : HealthCheckResponse_ServingStatus.UNKNOWN,
    };
  },

  toJSON(message: HealthCheckResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = healthCheckResponse_ServingStatusToJSON(message.status));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HealthCheckResponse>, I>>(object: I): HealthCheckResponse {
    const message = createBaseHealthCheckResponse();
    message.status = object.status ?? HealthCheckResponse_ServingStatus.UNKNOWN;
    return message;
  },
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends { $case: string } ? { [K in keyof Omit<T, "$case">]?: DeepPartial<T[K]> } & { $case: T["$case"] }
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
