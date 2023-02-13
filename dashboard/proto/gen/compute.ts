/* eslint-disable */

export const protobufPackage = "compute";

export interface ShowConfigRequest {
}

export interface ShowConfigResponse {
  batchConfig: string;
  streamConfig: string;
}

function createBaseShowConfigRequest(): ShowConfigRequest {
  return {};
}

export const ShowConfigRequest = {
  fromJSON(_: any): ShowConfigRequest {
    return {};
  },

  toJSON(_: ShowConfigRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ShowConfigRequest>, I>>(_: I): ShowConfigRequest {
    const message = createBaseShowConfigRequest();
    return message;
  },
};

function createBaseShowConfigResponse(): ShowConfigResponse {
  return { batchConfig: "", streamConfig: "" };
}

export const ShowConfigResponse = {
  fromJSON(object: any): ShowConfigResponse {
    return {
      batchConfig: isSet(object.batchConfig) ? String(object.batchConfig) : "",
      streamConfig: isSet(object.streamConfig) ? String(object.streamConfig) : "",
    };
  },

  toJSON(message: ShowConfigResponse): unknown {
    const obj: any = {};
    message.batchConfig !== undefined && (obj.batchConfig = message.batchConfig);
    message.streamConfig !== undefined && (obj.streamConfig = message.streamConfig);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ShowConfigResponse>, I>>(object: I): ShowConfigResponse {
    const message = createBaseShowConfigResponse();
    message.batchConfig = object.batchConfig ?? "";
    message.streamConfig = object.streamConfig ?? "";
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
