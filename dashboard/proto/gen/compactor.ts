/* eslint-disable */

export const protobufPackage = "compactor";

export interface CompactorRuntimeConfig {
  maxConcurrentTaskNumber: number;
}

export interface SetRuntimeConfigRequest {
  config: CompactorRuntimeConfig | undefined;
}

export interface SetRuntimeConfigResponse {
}

function createBaseCompactorRuntimeConfig(): CompactorRuntimeConfig {
  return { maxConcurrentTaskNumber: 0 };
}

export const CompactorRuntimeConfig = {
  fromJSON(object: any): CompactorRuntimeConfig {
    return {
      maxConcurrentTaskNumber: isSet(object.maxConcurrentTaskNumber) ? Number(object.maxConcurrentTaskNumber) : 0,
    };
  },

  toJSON(message: CompactorRuntimeConfig): unknown {
    const obj: any = {};
    message.maxConcurrentTaskNumber !== undefined &&
      (obj.maxConcurrentTaskNumber = Math.round(message.maxConcurrentTaskNumber));
    return obj;
  },

  create<I extends Exact<DeepPartial<CompactorRuntimeConfig>, I>>(base?: I): CompactorRuntimeConfig {
    return CompactorRuntimeConfig.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<CompactorRuntimeConfig>, I>>(object: I): CompactorRuntimeConfig {
    const message = createBaseCompactorRuntimeConfig();
    message.maxConcurrentTaskNumber = object.maxConcurrentTaskNumber ?? 0;
    return message;
  },
};

function createBaseSetRuntimeConfigRequest(): SetRuntimeConfigRequest {
  return { config: undefined };
}

export const SetRuntimeConfigRequest = {
  fromJSON(object: any): SetRuntimeConfigRequest {
    return { config: isSet(object.config) ? CompactorRuntimeConfig.fromJSON(object.config) : undefined };
  },

  toJSON(message: SetRuntimeConfigRequest): unknown {
    const obj: any = {};
    message.config !== undefined &&
      (obj.config = message.config ? CompactorRuntimeConfig.toJSON(message.config) : undefined);
    return obj;
  },

  create<I extends Exact<DeepPartial<SetRuntimeConfigRequest>, I>>(base?: I): SetRuntimeConfigRequest {
    return SetRuntimeConfigRequest.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<SetRuntimeConfigRequest>, I>>(object: I): SetRuntimeConfigRequest {
    const message = createBaseSetRuntimeConfigRequest();
    message.config = (object.config !== undefined && object.config !== null)
      ? CompactorRuntimeConfig.fromPartial(object.config)
      : undefined;
    return message;
  },
};

function createBaseSetRuntimeConfigResponse(): SetRuntimeConfigResponse {
  return {};
}

export const SetRuntimeConfigResponse = {
  fromJSON(_: any): SetRuntimeConfigResponse {
    return {};
  },

  toJSON(_: SetRuntimeConfigResponse): unknown {
    const obj: any = {};
    return obj;
  },

  create<I extends Exact<DeepPartial<SetRuntimeConfigResponse>, I>>(base?: I): SetRuntimeConfigResponse {
    return SetRuntimeConfigResponse.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<SetRuntimeConfigResponse>, I>>(_: I): SetRuntimeConfigResponse {
    const message = createBaseSetRuntimeConfigResponse();
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
