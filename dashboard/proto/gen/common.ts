/* eslint-disable */

export const protobufPackage = "common";

export const WorkerType = {
  UNSPECIFIED: "UNSPECIFIED",
  FRONTEND: "FRONTEND",
  COMPUTE_NODE: "COMPUTE_NODE",
  RISE_CTL: "RISE_CTL",
  COMPACTOR: "COMPACTOR",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type WorkerType = typeof WorkerType[keyof typeof WorkerType];

export function workerTypeFromJSON(object: any): WorkerType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return WorkerType.UNSPECIFIED;
    case 1:
    case "FRONTEND":
      return WorkerType.FRONTEND;
    case 2:
    case "COMPUTE_NODE":
      return WorkerType.COMPUTE_NODE;
    case 3:
    case "RISE_CTL":
      return WorkerType.RISE_CTL;
    case 4:
    case "COMPACTOR":
      return WorkerType.COMPACTOR;
    case -1:
    case "UNRECOGNIZED":
    default:
      return WorkerType.UNRECOGNIZED;
  }
}

export function workerTypeToJSON(object: WorkerType): string {
  switch (object) {
    case WorkerType.UNSPECIFIED:
      return "UNSPECIFIED";
    case WorkerType.FRONTEND:
      return "FRONTEND";
    case WorkerType.COMPUTE_NODE:
      return "COMPUTE_NODE";
    case WorkerType.RISE_CTL:
      return "RISE_CTL";
    case WorkerType.COMPACTOR:
      return "COMPACTOR";
    case WorkerType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface Status {
  code: Status_Code;
  message: string;
}

export const Status_Code = {
  UNSPECIFIED: "UNSPECIFIED",
  OK: "OK",
  UNKNOWN_WORKER: "UNKNOWN_WORKER",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Status_Code = typeof Status_Code[keyof typeof Status_Code];

export function status_CodeFromJSON(object: any): Status_Code {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return Status_Code.UNSPECIFIED;
    case 1:
    case "OK":
      return Status_Code.OK;
    case 2:
    case "UNKNOWN_WORKER":
      return Status_Code.UNKNOWN_WORKER;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Status_Code.UNRECOGNIZED;
  }
}

export function status_CodeToJSON(object: Status_Code): string {
  switch (object) {
    case Status_Code.UNSPECIFIED:
      return "UNSPECIFIED";
    case Status_Code.OK:
      return "OK";
    case Status_Code.UNKNOWN_WORKER:
      return "UNKNOWN_WORKER";
    case Status_Code.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface HostAddress {
  host: string;
  port: number;
}

/** Encode which host machine an actor resides. */
export interface ActorInfo {
  actorId: number;
  host: HostAddress | undefined;
}

export interface ParallelUnit {
  id: number;
  workerNodeId: number;
}

export interface WorkerNode {
  id: number;
  type: WorkerType;
  host: HostAddress | undefined;
  state: WorkerNode_State;
  parallelUnits: ParallelUnit[];
}

export const WorkerNode_State = {
  UNSPECIFIED: "UNSPECIFIED",
  STARTING: "STARTING",
  RUNNING: "RUNNING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type WorkerNode_State = typeof WorkerNode_State[keyof typeof WorkerNode_State];

export function workerNode_StateFromJSON(object: any): WorkerNode_State {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return WorkerNode_State.UNSPECIFIED;
    case 1:
    case "STARTING":
      return WorkerNode_State.STARTING;
    case 2:
    case "RUNNING":
      return WorkerNode_State.RUNNING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return WorkerNode_State.UNRECOGNIZED;
  }
}

export function workerNode_StateToJSON(object: WorkerNode_State): string {
  switch (object) {
    case WorkerNode_State.UNSPECIFIED:
      return "UNSPECIFIED";
    case WorkerNode_State.STARTING:
      return "STARTING";
    case WorkerNode_State.RUNNING:
      return "RUNNING";
    case WorkerNode_State.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface Buffer {
  compression: Buffer_CompressionType;
  body: Uint8Array;
}

export const Buffer_CompressionType = {
  UNSPECIFIED: "UNSPECIFIED",
  NONE: "NONE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Buffer_CompressionType = typeof Buffer_CompressionType[keyof typeof Buffer_CompressionType];

export function buffer_CompressionTypeFromJSON(object: any): Buffer_CompressionType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return Buffer_CompressionType.UNSPECIFIED;
    case 1:
    case "NONE":
      return Buffer_CompressionType.NONE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Buffer_CompressionType.UNRECOGNIZED;
  }
}

export function buffer_CompressionTypeToJSON(object: Buffer_CompressionType): string {
  switch (object) {
    case Buffer_CompressionType.UNSPECIFIED:
      return "UNSPECIFIED";
    case Buffer_CompressionType.NONE:
      return "NONE";
    case Buffer_CompressionType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

/** Vnode mapping for stream fragments. Stores mapping from virtual node to parallel unit id. */
export interface ParallelUnitMapping {
  fragmentId: number;
  originalIndices: number[];
  data: number[];
}

function createBaseStatus(): Status {
  return { code: Status_Code.UNSPECIFIED, message: "" };
}

export const Status = {
  fromJSON(object: any): Status {
    return {
      code: isSet(object.code) ? status_CodeFromJSON(object.code) : Status_Code.UNSPECIFIED,
      message: isSet(object.message) ? String(object.message) : "",
    };
  },

  toJSON(message: Status): unknown {
    const obj: any = {};
    message.code !== undefined && (obj.code = status_CodeToJSON(message.code));
    message.message !== undefined && (obj.message = message.message);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Status>, I>>(object: I): Status {
    const message = createBaseStatus();
    message.code = object.code ?? Status_Code.UNSPECIFIED;
    message.message = object.message ?? "";
    return message;
  },
};

function createBaseHostAddress(): HostAddress {
  return { host: "", port: 0 };
}

export const HostAddress = {
  fromJSON(object: any): HostAddress {
    return { host: isSet(object.host) ? String(object.host) : "", port: isSet(object.port) ? Number(object.port) : 0 };
  },

  toJSON(message: HostAddress): unknown {
    const obj: any = {};
    message.host !== undefined && (obj.host = message.host);
    message.port !== undefined && (obj.port = Math.round(message.port));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HostAddress>, I>>(object: I): HostAddress {
    const message = createBaseHostAddress();
    message.host = object.host ?? "";
    message.port = object.port ?? 0;
    return message;
  },
};

function createBaseActorInfo(): ActorInfo {
  return { actorId: 0, host: undefined };
}

export const ActorInfo = {
  fromJSON(object: any): ActorInfo {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
    };
  },

  toJSON(message: ActorInfo): unknown {
    const obj: any = {};
    message.actorId !== undefined && (obj.actorId = Math.round(message.actorId));
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ActorInfo>, I>>(object: I): ActorInfo {
    const message = createBaseActorInfo();
    message.actorId = object.actorId ?? 0;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    return message;
  },
};

function createBaseParallelUnit(): ParallelUnit {
  return { id: 0, workerNodeId: 0 };
}

export const ParallelUnit = {
  fromJSON(object: any): ParallelUnit {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      workerNodeId: isSet(object.workerNodeId) ? Number(object.workerNodeId) : 0,
    };
  },

  toJSON(message: ParallelUnit): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.workerNodeId !== undefined && (obj.workerNodeId = Math.round(message.workerNodeId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ParallelUnit>, I>>(object: I): ParallelUnit {
    const message = createBaseParallelUnit();
    message.id = object.id ?? 0;
    message.workerNodeId = object.workerNodeId ?? 0;
    return message;
  },
};

function createBaseWorkerNode(): WorkerNode {
  return {
    id: 0,
    type: WorkerType.UNSPECIFIED,
    host: undefined,
    state: WorkerNode_State.UNSPECIFIED,
    parallelUnits: [],
  };
}

export const WorkerNode = {
  fromJSON(object: any): WorkerNode {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      type: isSet(object.type) ? workerTypeFromJSON(object.type) : WorkerType.UNSPECIFIED,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
      state: isSet(object.state) ? workerNode_StateFromJSON(object.state) : WorkerNode_State.UNSPECIFIED,
      parallelUnits: Array.isArray(object?.parallelUnits)
        ? object.parallelUnits.map((e: any) => ParallelUnit.fromJSON(e))
        : [],
    };
  },

  toJSON(message: WorkerNode): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.type !== undefined && (obj.type = workerTypeToJSON(message.type));
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    message.state !== undefined && (obj.state = workerNode_StateToJSON(message.state));
    if (message.parallelUnits) {
      obj.parallelUnits = message.parallelUnits.map((e) => e ? ParallelUnit.toJSON(e) : undefined);
    } else {
      obj.parallelUnits = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<WorkerNode>, I>>(object: I): WorkerNode {
    const message = createBaseWorkerNode();
    message.id = object.id ?? 0;
    message.type = object.type ?? WorkerType.UNSPECIFIED;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    message.state = object.state ?? WorkerNode_State.UNSPECIFIED;
    message.parallelUnits = object.parallelUnits?.map((e) => ParallelUnit.fromPartial(e)) || [];
    return message;
  },
};

function createBaseBuffer(): Buffer {
  return { compression: Buffer_CompressionType.UNSPECIFIED, body: new Uint8Array() };
}

export const Buffer = {
  fromJSON(object: any): Buffer {
    return {
      compression: isSet(object.compression)
        ? buffer_CompressionTypeFromJSON(object.compression)
        : Buffer_CompressionType.UNSPECIFIED,
      body: isSet(object.body) ? bytesFromBase64(object.body) : new Uint8Array(),
    };
  },

  toJSON(message: Buffer): unknown {
    const obj: any = {};
    message.compression !== undefined && (obj.compression = buffer_CompressionTypeToJSON(message.compression));
    message.body !== undefined &&
      (obj.body = base64FromBytes(message.body !== undefined ? message.body : new Uint8Array()));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Buffer>, I>>(object: I): Buffer {
    const message = createBaseBuffer();
    message.compression = object.compression ?? Buffer_CompressionType.UNSPECIFIED;
    message.body = object.body ?? new Uint8Array();
    return message;
  },
};

function createBaseParallelUnitMapping(): ParallelUnitMapping {
  return { fragmentId: 0, originalIndices: [], data: [] };
}

export const ParallelUnitMapping = {
  fromJSON(object: any): ParallelUnitMapping {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      originalIndices: Array.isArray(object?.originalIndices) ? object.originalIndices.map((e: any) => Number(e)) : [],
      data: Array.isArray(object?.data) ? object.data.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ParallelUnitMapping): unknown {
    const obj: any = {};
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    if (message.originalIndices) {
      obj.originalIndices = message.originalIndices.map((e) => Math.round(e));
    } else {
      obj.originalIndices = [];
    }
    if (message.data) {
      obj.data = message.data.map((e) => Math.round(e));
    } else {
      obj.data = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ParallelUnitMapping>, I>>(object: I): ParallelUnitMapping {
    const message = createBaseParallelUnitMapping();
    message.fragmentId = object.fragmentId ?? 0;
    message.originalIndices = object.originalIndices?.map((e) => e) || [];
    message.data = object.data?.map((e) => e) || [];
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
