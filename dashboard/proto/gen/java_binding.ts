/* eslint-disable */
import { Table } from "./catalog";
import { HummockVersion } from "./hummock";

export const protobufPackage = "java_binding";

export interface ReadPlan {
  tableId: number;
  version: HummockVersion | undefined;
  tableCatalog: Table | undefined;
}

function createBaseReadPlan(): ReadPlan {
  return { tableId: 0, version: undefined, tableCatalog: undefined };
}

export const ReadPlan = {
  fromJSON(object: any): ReadPlan {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      version: isSet(object.version) ? HummockVersion.fromJSON(object.version) : undefined,
      tableCatalog: isSet(object.tableCatalog) ? Table.fromJSON(object.tableCatalog) : undefined,
    };
  },

  toJSON(message: ReadPlan): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.version !== undefined &&
      (obj.version = message.version ? HummockVersion.toJSON(message.version) : undefined);
    message.tableCatalog !== undefined &&
      (obj.tableCatalog = message.tableCatalog ? Table.toJSON(message.tableCatalog) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReadPlan>, I>>(object: I): ReadPlan {
    const message = createBaseReadPlan();
    message.tableId = object.tableId ?? 0;
    message.version = (object.version !== undefined && object.version !== null)
      ? HummockVersion.fromPartial(object.version)
      : undefined;
    message.tableCatalog = (object.tableCatalog !== undefined && object.tableCatalog !== null)
      ? Table.fromPartial(object.tableCatalog)
      : undefined;
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
