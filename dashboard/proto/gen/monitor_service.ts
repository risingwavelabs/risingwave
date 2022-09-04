/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";

export const protobufPackage = "monitor_service";

export interface StackTraceRequest {
}

export interface StackTraceResponse {
  actorTraces: { [key: number]: string };
  rpcTraces: { [key: string]: string };
}

export interface StackTraceResponse_ActorTracesEntry {
  key: number;
  value: string;
}

export interface StackTraceResponse_RpcTracesEntry {
  key: string;
  value: string;
}

export interface ProfilingRequest {
  /** How long the profiling should last. */
  sleepS: number;
}

export interface ProfilingResponse {
  result: Uint8Array;
}

function createBaseStackTraceRequest(): StackTraceRequest {
  return {};
}

export const StackTraceRequest = {
  encode(_: StackTraceRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StackTraceRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStackTraceRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(_: any): StackTraceRequest {
    return {};
  },

  toJSON(_: StackTraceRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StackTraceRequest>, I>>(_: I): StackTraceRequest {
    const message = createBaseStackTraceRequest();
    return message;
  },
};

function createBaseStackTraceResponse(): StackTraceResponse {
  return { actorTraces: {}, rpcTraces: {} };
}

export const StackTraceResponse = {
  encode(message: StackTraceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.actorTraces).forEach(([key, value]) => {
      StackTraceResponse_ActorTracesEntry.encode({ key: key as any, value }, writer.uint32(10).fork()).ldelim();
    });
    Object.entries(message.rpcTraces).forEach(([key, value]) => {
      StackTraceResponse_RpcTracesEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StackTraceResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStackTraceResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          const entry1 = StackTraceResponse_ActorTracesEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.actorTraces[entry1.key] = entry1.value;
          }
          break;
        case 2:
          const entry2 = StackTraceResponse_RpcTracesEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.rpcTraces[entry2.key] = entry2.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StackTraceResponse {
    return {
      actorTraces: isObject(object.actorTraces)
        ? Object.entries(object.actorTraces).reduce<{ [key: number]: string }>((acc, [key, value]) => {
          acc[Number(key)] = String(value);
          return acc;
        }, {})
        : {},
      rpcTraces: isObject(object.rpcTraces)
        ? Object.entries(object.rpcTraces).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: StackTraceResponse): unknown {
    const obj: any = {};
    obj.actorTraces = {};
    if (message.actorTraces) {
      Object.entries(message.actorTraces).forEach(([k, v]) => {
        obj.actorTraces[k] = v;
      });
    }
    obj.rpcTraces = {};
    if (message.rpcTraces) {
      Object.entries(message.rpcTraces).forEach(([k, v]) => {
        obj.rpcTraces[k] = v;
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StackTraceResponse>, I>>(object: I): StackTraceResponse {
    const message = createBaseStackTraceResponse();
    message.actorTraces = Object.entries(object.actorTraces ?? {}).reduce<{ [key: number]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = String(value);
        }
        return acc;
      },
      {},
    );
    message.rpcTraces = Object.entries(object.rpcTraces ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseStackTraceResponse_ActorTracesEntry(): StackTraceResponse_ActorTracesEntry {
  return { key: 0, value: "" };
}

export const StackTraceResponse_ActorTracesEntry = {
  encode(message: StackTraceResponse_ActorTracesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StackTraceResponse_ActorTracesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStackTraceResponse_ActorTracesEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = reader.string();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StackTraceResponse_ActorTracesEntry {
    return { key: isSet(object.key) ? Number(object.key) : 0, value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: StackTraceResponse_ActorTracesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StackTraceResponse_ActorTracesEntry>, I>>(
    object: I,
  ): StackTraceResponse_ActorTracesEntry {
    const message = createBaseStackTraceResponse_ActorTracesEntry();
    message.key = object.key ?? 0;
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseStackTraceResponse_RpcTracesEntry(): StackTraceResponse_RpcTracesEntry {
  return { key: "", value: "" };
}

export const StackTraceResponse_RpcTracesEntry = {
  encode(message: StackTraceResponse_RpcTracesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StackTraceResponse_RpcTracesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStackTraceResponse_RpcTracesEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.string();
          break;
        case 2:
          message.value = reader.string();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StackTraceResponse_RpcTracesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: StackTraceResponse_RpcTracesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StackTraceResponse_RpcTracesEntry>, I>>(
    object: I,
  ): StackTraceResponse_RpcTracesEntry {
    const message = createBaseStackTraceResponse_RpcTracesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseProfilingRequest(): ProfilingRequest {
  return { sleepS: 0 };
}

export const ProfilingRequest = {
  encode(message: ProfilingRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sleepS !== 0) {
      writer.uint32(8).uint64(message.sleepS);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ProfilingRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseProfilingRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sleepS = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ProfilingRequest {
    return { sleepS: isSet(object.sleepS) ? Number(object.sleepS) : 0 };
  },

  toJSON(message: ProfilingRequest): unknown {
    const obj: any = {};
    message.sleepS !== undefined && (obj.sleepS = Math.round(message.sleepS));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProfilingRequest>, I>>(object: I): ProfilingRequest {
    const message = createBaseProfilingRequest();
    message.sleepS = object.sleepS ?? 0;
    return message;
  },
};

function createBaseProfilingResponse(): ProfilingResponse {
  return { result: new Uint8Array() };
}

export const ProfilingResponse = {
  encode(message: ProfilingResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.result.length !== 0) {
      writer.uint32(10).bytes(message.result);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ProfilingResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseProfilingResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.result = reader.bytes();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ProfilingResponse {
    return { result: isSet(object.result) ? bytesFromBase64(object.result) : new Uint8Array() };
  },

  toJSON(message: ProfilingResponse): unknown {
    const obj: any = {};
    message.result !== undefined &&
      (obj.result = base64FromBytes(message.result !== undefined ? message.result : new Uint8Array()));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProfilingResponse>, I>>(object: I): ProfilingResponse {
    const message = createBaseProfilingResponse();
    message.result = object.result ?? new Uint8Array();
    return message;
  },
};

declare var self: any | undefined;
declare var window: any | undefined;
declare var global: any | undefined;
var globalThis: any = (() => {
  if (typeof globalThis !== "undefined") {
    return globalThis;
  }
  if (typeof self !== "undefined") {
    return self;
  }
  if (typeof window !== "undefined") {
    return window;
  }
  if (typeof global !== "undefined") {
    return global;
  }
  throw "Unable to locate global object";
})();

function bytesFromBase64(b64: string): Uint8Array {
  if (globalThis.Buffer) {
    return Uint8Array.from(globalThis.Buffer.from(b64, "base64"));
  } else {
    const bin = globalThis.atob(b64);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; ++i) {
      arr[i] = bin.charCodeAt(i);
    }
    return arr;
  }
}

function base64FromBytes(arr: Uint8Array): string {
  if (globalThis.Buffer) {
    return globalThis.Buffer.from(arr).toString("base64");
  } else {
    const bin: string[] = [];
    arr.forEach((byte) => {
      bin.push(String.fromCharCode(byte));
    });
    return globalThis.btoa(bin.join(""));
  }
}

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends { $case: string } ? { [K in keyof Omit<T, "$case">]?: DeepPartial<T[K]> } & { $case: T["$case"] }
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function longToNumber(long: Long): number {
  if (long.gt(Number.MAX_SAFE_INTEGER)) {
    throw new globalThis.Error("Value is larger than Number.MAX_SAFE_INTEGER");
  }
  return long.toNumber();
}

// If you get a compile-error about 'Constructor<Long> and ... have no overlap',
// add '--ts_proto_opt=esModuleInterop=true' as a flag when calling 'protoc'.
if (_m0.util.Long !== Long) {
  _m0.util.Long = Long as any;
  _m0.configure();
}

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
