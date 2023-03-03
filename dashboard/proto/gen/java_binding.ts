/* eslint-disable */
import { Table } from "./catalog";
import { HummockVersion } from "./hummock";

export const protobufPackage = "java_binding";

/** When `left` or `right` is none, it represents unbounded. */
export interface KeyRange {
  left: Uint8Array;
  right: Uint8Array;
  leftBound: KeyRange_Bound;
  rightBound: KeyRange_Bound;
}

export const KeyRange_Bound = {
  UNSPECIFIED: "UNSPECIFIED",
  UNBOUNDED: "UNBOUNDED",
  INCLUDED: "INCLUDED",
  EXCLUDED: "EXCLUDED",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type KeyRange_Bound = typeof KeyRange_Bound[keyof typeof KeyRange_Bound];

export function keyRange_BoundFromJSON(object: any): KeyRange_Bound {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return KeyRange_Bound.UNSPECIFIED;
    case 1:
    case "UNBOUNDED":
      return KeyRange_Bound.UNBOUNDED;
    case 2:
    case "INCLUDED":
      return KeyRange_Bound.INCLUDED;
    case 3:
    case "EXCLUDED":
      return KeyRange_Bound.EXCLUDED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return KeyRange_Bound.UNRECOGNIZED;
  }
}

export function keyRange_BoundToJSON(object: KeyRange_Bound): string {
  switch (object) {
    case KeyRange_Bound.UNSPECIFIED:
      return "UNSPECIFIED";
    case KeyRange_Bound.UNBOUNDED:
      return "UNBOUNDED";
    case KeyRange_Bound.INCLUDED:
      return "INCLUDED";
    case KeyRange_Bound.EXCLUDED:
      return "EXCLUDED";
    case KeyRange_Bound.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface ReadPlan {
  objectStoreUrl: string;
  dataDir: string;
  keyRange: KeyRange | undefined;
  tableId: number;
  epoch: number;
  version: HummockVersion | undefined;
  tableCatalog: Table | undefined;
  vnodeIds: number[];
}

function createBaseKeyRange(): KeyRange {
  return {
    left: new Uint8Array(),
    right: new Uint8Array(),
    leftBound: KeyRange_Bound.UNSPECIFIED,
    rightBound: KeyRange_Bound.UNSPECIFIED,
  };
}

export const KeyRange = {
  fromJSON(object: any): KeyRange {
    return {
      left: isSet(object.left) ? bytesFromBase64(object.left) : new Uint8Array(),
      right: isSet(object.right) ? bytesFromBase64(object.right) : new Uint8Array(),
      leftBound: isSet(object.leftBound) ? keyRange_BoundFromJSON(object.leftBound) : KeyRange_Bound.UNSPECIFIED,
      rightBound: isSet(object.rightBound) ? keyRange_BoundFromJSON(object.rightBound) : KeyRange_Bound.UNSPECIFIED,
    };
  },

  toJSON(message: KeyRange): unknown {
    const obj: any = {};
    message.left !== undefined &&
      (obj.left = base64FromBytes(message.left !== undefined ? message.left : new Uint8Array()));
    message.right !== undefined &&
      (obj.right = base64FromBytes(message.right !== undefined ? message.right : new Uint8Array()));
    message.leftBound !== undefined && (obj.leftBound = keyRange_BoundToJSON(message.leftBound));
    message.rightBound !== undefined && (obj.rightBound = keyRange_BoundToJSON(message.rightBound));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<KeyRange>, I>>(object: I): KeyRange {
    const message = createBaseKeyRange();
    message.left = object.left ?? new Uint8Array();
    message.right = object.right ?? new Uint8Array();
    message.leftBound = object.leftBound ?? KeyRange_Bound.UNSPECIFIED;
    message.rightBound = object.rightBound ?? KeyRange_Bound.UNSPECIFIED;
    return message;
  },
};

function createBaseReadPlan(): ReadPlan {
  return {
    objectStoreUrl: "",
    dataDir: "",
    keyRange: undefined,
    tableId: 0,
    epoch: 0,
    version: undefined,
    tableCatalog: undefined,
    vnodeIds: [],
  };
}

export const ReadPlan = {
  fromJSON(object: any): ReadPlan {
    return {
      objectStoreUrl: isSet(object.objectStoreUrl) ? String(object.objectStoreUrl) : "",
      dataDir: isSet(object.dataDir) ? String(object.dataDir) : "",
      keyRange: isSet(object.keyRange) ? KeyRange.fromJSON(object.keyRange) : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
      version: isSet(object.version) ? HummockVersion.fromJSON(object.version) : undefined,
      tableCatalog: isSet(object.tableCatalog) ? Table.fromJSON(object.tableCatalog) : undefined,
      vnodeIds: Array.isArray(object?.vnodeIds) ? object.vnodeIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ReadPlan): unknown {
    const obj: any = {};
    message.objectStoreUrl !== undefined && (obj.objectStoreUrl = message.objectStoreUrl);
    message.dataDir !== undefined && (obj.dataDir = message.dataDir);
    message.keyRange !== undefined && (obj.keyRange = message.keyRange ? KeyRange.toJSON(message.keyRange) : undefined);
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    message.version !== undefined &&
      (obj.version = message.version ? HummockVersion.toJSON(message.version) : undefined);
    message.tableCatalog !== undefined &&
      (obj.tableCatalog = message.tableCatalog ? Table.toJSON(message.tableCatalog) : undefined);
    if (message.vnodeIds) {
      obj.vnodeIds = message.vnodeIds.map((e) => Math.round(e));
    } else {
      obj.vnodeIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReadPlan>, I>>(object: I): ReadPlan {
    const message = createBaseReadPlan();
    message.objectStoreUrl = object.objectStoreUrl ?? "";
    message.dataDir = object.dataDir ?? "";
    message.keyRange = (object.keyRange !== undefined && object.keyRange !== null)
      ? KeyRange.fromPartial(object.keyRange)
      : undefined;
    message.tableId = object.tableId ?? 0;
    message.epoch = object.epoch ?? 0;
    message.version = (object.version !== undefined && object.version !== null)
      ? HummockVersion.fromPartial(object.version)
      : undefined;
    message.tableCatalog = (object.tableCatalog !== undefined && object.tableCatalog !== null)
      ? Table.fromPartial(object.tableCatalog)
      : undefined;
    message.vnodeIds = object.vnodeIds?.map((e) => e) || [];
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
