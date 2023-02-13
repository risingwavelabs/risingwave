/* eslint-disable */

export const protobufPackage = "source";

export interface ConnectorSplit {
  splitType: string;
  encodedSplit: Uint8Array;
}

export interface ConnectorSplits {
  splits: ConnectorSplit[];
}

export interface SourceActorInfo {
  actorId: number;
  splits: ConnectorSplits | undefined;
}

function createBaseConnectorSplit(): ConnectorSplit {
  return { splitType: "", encodedSplit: new Uint8Array() };
}

export const ConnectorSplit = {
  fromJSON(object: any): ConnectorSplit {
    return {
      splitType: isSet(object.splitType) ? String(object.splitType) : "",
      encodedSplit: isSet(object.encodedSplit) ? bytesFromBase64(object.encodedSplit) : new Uint8Array(),
    };
  },

  toJSON(message: ConnectorSplit): unknown {
    const obj: any = {};
    message.splitType !== undefined && (obj.splitType = message.splitType);
    message.encodedSplit !== undefined &&
      (obj.encodedSplit = base64FromBytes(
        message.encodedSplit !== undefined ? message.encodedSplit : new Uint8Array(),
      ));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ConnectorSplit>, I>>(object: I): ConnectorSplit {
    const message = createBaseConnectorSplit();
    message.splitType = object.splitType ?? "";
    message.encodedSplit = object.encodedSplit ?? new Uint8Array();
    return message;
  },
};

function createBaseConnectorSplits(): ConnectorSplits {
  return { splits: [] };
}

export const ConnectorSplits = {
  fromJSON(object: any): ConnectorSplits {
    return { splits: Array.isArray(object?.splits) ? object.splits.map((e: any) => ConnectorSplit.fromJSON(e)) : [] };
  },

  toJSON(message: ConnectorSplits): unknown {
    const obj: any = {};
    if (message.splits) {
      obj.splits = message.splits.map((e) => e ? ConnectorSplit.toJSON(e) : undefined);
    } else {
      obj.splits = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ConnectorSplits>, I>>(object: I): ConnectorSplits {
    const message = createBaseConnectorSplits();
    message.splits = object.splits?.map((e) => ConnectorSplit.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSourceActorInfo(): SourceActorInfo {
  return { actorId: 0, splits: undefined };
}

export const SourceActorInfo = {
  fromJSON(object: any): SourceActorInfo {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      splits: isSet(object.splits) ? ConnectorSplits.fromJSON(object.splits) : undefined,
    };
  },

  toJSON(message: SourceActorInfo): unknown {
    const obj: any = {};
    message.actorId !== undefined && (obj.actorId = Math.round(message.actorId));
    message.splits !== undefined && (obj.splits = message.splits ? ConnectorSplits.toJSON(message.splits) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceActorInfo>, I>>(object: I): SourceActorInfo {
    const message = createBaseSourceActorInfo();
    message.actorId = object.actorId ?? 0;
    message.splits = (object.splits !== undefined && object.splits !== null)
      ? ConnectorSplits.fromPartial(object.splits)
      : undefined;
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
