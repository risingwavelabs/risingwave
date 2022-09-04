/* eslint-disable */
import * as _m0 from "protobufjs/minimal";
import { DataType } from "./data";

export const protobufPackage = "plan_common";

export enum JoinType {
  /**
   * UNSPECIFIED - Note that it comes from Calcite's JoinRelType.
   * DO NOT HAVE direction for SEMI and ANTI now.
   */
  UNSPECIFIED = 0,
  INNER = 1,
  LEFT_OUTER = 2,
  RIGHT_OUTER = 3,
  FULL_OUTER = 4,
  LEFT_SEMI = 5,
  LEFT_ANTI = 6,
  RIGHT_SEMI = 7,
  RIGHT_ANTI = 8,
  UNRECOGNIZED = -1,
}

export function joinTypeFromJSON(object: any): JoinType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return JoinType.UNSPECIFIED;
    case 1:
    case "INNER":
      return JoinType.INNER;
    case 2:
    case "LEFT_OUTER":
      return JoinType.LEFT_OUTER;
    case 3:
    case "RIGHT_OUTER":
      return JoinType.RIGHT_OUTER;
    case 4:
    case "FULL_OUTER":
      return JoinType.FULL_OUTER;
    case 5:
    case "LEFT_SEMI":
      return JoinType.LEFT_SEMI;
    case 6:
    case "LEFT_ANTI":
      return JoinType.LEFT_ANTI;
    case 7:
    case "RIGHT_SEMI":
      return JoinType.RIGHT_SEMI;
    case 8:
    case "RIGHT_ANTI":
      return JoinType.RIGHT_ANTI;
    case -1:
    case "UNRECOGNIZED":
    default:
      return JoinType.UNRECOGNIZED;
  }
}

export function joinTypeToJSON(object: JoinType): string {
  switch (object) {
    case JoinType.UNSPECIFIED:
      return "UNSPECIFIED";
    case JoinType.INNER:
      return "INNER";
    case JoinType.LEFT_OUTER:
      return "LEFT_OUTER";
    case JoinType.RIGHT_OUTER:
      return "RIGHT_OUTER";
    case JoinType.FULL_OUTER:
      return "FULL_OUTER";
    case JoinType.LEFT_SEMI:
      return "LEFT_SEMI";
    case JoinType.LEFT_ANTI:
      return "LEFT_ANTI";
    case JoinType.RIGHT_SEMI:
      return "RIGHT_SEMI";
    case JoinType.RIGHT_ANTI:
      return "RIGHT_ANTI";
    case JoinType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum OrderType {
  ORDER_UNSPECIFIED = 0,
  ASCENDING = 1,
  DESCENDING = 2,
  UNRECOGNIZED = -1,
}

export function orderTypeFromJSON(object: any): OrderType {
  switch (object) {
    case 0:
    case "ORDER_UNSPECIFIED":
      return OrderType.ORDER_UNSPECIFIED;
    case 1:
    case "ASCENDING":
      return OrderType.ASCENDING;
    case 2:
    case "DESCENDING":
      return OrderType.DESCENDING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return OrderType.UNRECOGNIZED;
  }
}

export function orderTypeToJSON(object: OrderType): string {
  switch (object) {
    case OrderType.ORDER_UNSPECIFIED:
      return "ORDER_UNSPECIFIED";
    case OrderType.ASCENDING:
      return "ASCENDING";
    case OrderType.DESCENDING:
      return "DESCENDING";
    case OrderType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export enum RowFormatType {
  ROW_UNSPECIFIED = 0,
  JSON = 1,
  PROTOBUF = 2,
  DEBEZIUM_JSON = 3,
  AVRO = 4,
  UNRECOGNIZED = -1,
}

export function rowFormatTypeFromJSON(object: any): RowFormatType {
  switch (object) {
    case 0:
    case "ROW_UNSPECIFIED":
      return RowFormatType.ROW_UNSPECIFIED;
    case 1:
    case "JSON":
      return RowFormatType.JSON;
    case 2:
    case "PROTOBUF":
      return RowFormatType.PROTOBUF;
    case 3:
    case "DEBEZIUM_JSON":
      return RowFormatType.DEBEZIUM_JSON;
    case 4:
    case "AVRO":
      return RowFormatType.AVRO;
    case -1:
    case "UNRECOGNIZED":
    default:
      return RowFormatType.UNRECOGNIZED;
  }
}

export function rowFormatTypeToJSON(object: RowFormatType): string {
  switch (object) {
    case RowFormatType.ROW_UNSPECIFIED:
      return "ROW_UNSPECIFIED";
    case RowFormatType.JSON:
      return "JSON";
    case RowFormatType.PROTOBUF:
      return "PROTOBUF";
    case RowFormatType.DEBEZIUM_JSON:
      return "DEBEZIUM_JSON";
    case RowFormatType.AVRO:
      return "AVRO";
    case RowFormatType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

/** Field is a column in the streaming or batch plan. */
export interface Field {
  dataType: DataType | undefined;
  name: string;
}

export interface ColumnDesc {
  columnType: DataType | undefined;
  columnId: number;
  /** we store the column name in column desc now just for debug, but in future we should store it in ColumnCatalog but not here */
  name: string;
  /** For STRUCT type. */
  fieldDescs: ColumnDesc[];
  /**
   * The user-defined type's name. Empty if the column type is a builtin type.
   * For example, when the type is created from a protobuf schema file,
   * this field will store the message name.
   */
  typeName: string;
}

export interface ColumnCatalog {
  columnDesc: ColumnDesc | undefined;
  isHidden: boolean;
}

export interface StorageTableDesc {
  tableId: number;
  columns: ColumnDesc[];
  /** TODO: may refactor primary key representations */
  orderKey: ColumnOrder[];
  distKeyIndices: number[];
  retentionSeconds: number;
}

export interface ColumnOrder {
  /** maybe other name */
  orderType: OrderType;
  index: number;
}

function createBaseField(): Field {
  return { dataType: undefined, name: "" };
}

export const Field = {
  encode(message: Field, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.dataType !== undefined) {
      DataType.encode(message.dataType, writer.uint32(10).fork()).ldelim();
    }
    if (message.name !== "") {
      writer.uint32(18).string(message.name);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Field {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseField();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.dataType = DataType.decode(reader, reader.uint32());
          break;
        case 2:
          message.name = reader.string();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Field {
    return {
      dataType: isSet(object.dataType) ? DataType.fromJSON(object.dataType) : undefined,
      name: isSet(object.name) ? String(object.name) : "",
    };
  },

  toJSON(message: Field): unknown {
    const obj: any = {};
    message.dataType !== undefined && (obj.dataType = message.dataType ? DataType.toJSON(message.dataType) : undefined);
    message.name !== undefined && (obj.name = message.name);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Field>, I>>(object: I): Field {
    const message = createBaseField();
    message.dataType = (object.dataType !== undefined && object.dataType !== null)
      ? DataType.fromPartial(object.dataType)
      : undefined;
    message.name = object.name ?? "";
    return message;
  },
};

function createBaseColumnDesc(): ColumnDesc {
  return { columnType: undefined, columnId: 0, name: "", fieldDescs: [], typeName: "" };
}

export const ColumnDesc = {
  encode(message: ColumnDesc, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.columnType !== undefined) {
      DataType.encode(message.columnType, writer.uint32(10).fork()).ldelim();
    }
    if (message.columnId !== 0) {
      writer.uint32(16).int32(message.columnId);
    }
    if (message.name !== "") {
      writer.uint32(26).string(message.name);
    }
    for (const v of message.fieldDescs) {
      ColumnDesc.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    if (message.typeName !== "") {
      writer.uint32(42).string(message.typeName);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ColumnDesc {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseColumnDesc();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnType = DataType.decode(reader, reader.uint32());
          break;
        case 2:
          message.columnId = reader.int32();
          break;
        case 3:
          message.name = reader.string();
          break;
        case 4:
          message.fieldDescs.push(ColumnDesc.decode(reader, reader.uint32()));
          break;
        case 5:
          message.typeName = reader.string();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ColumnDesc {
    return {
      columnType: isSet(object.columnType) ? DataType.fromJSON(object.columnType) : undefined,
      columnId: isSet(object.columnId) ? Number(object.columnId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      fieldDescs: Array.isArray(object?.fieldDescs) ? object.fieldDescs.map((e: any) => ColumnDesc.fromJSON(e)) : [],
      typeName: isSet(object.typeName) ? String(object.typeName) : "",
    };
  },

  toJSON(message: ColumnDesc): unknown {
    const obj: any = {};
    message.columnType !== undefined &&
      (obj.columnType = message.columnType ? DataType.toJSON(message.columnType) : undefined);
    message.columnId !== undefined && (obj.columnId = Math.round(message.columnId));
    message.name !== undefined && (obj.name = message.name);
    if (message.fieldDescs) {
      obj.fieldDescs = message.fieldDescs.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.fieldDescs = [];
    }
    message.typeName !== undefined && (obj.typeName = message.typeName);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnDesc>, I>>(object: I): ColumnDesc {
    const message = createBaseColumnDesc();
    message.columnType = (object.columnType !== undefined && object.columnType !== null)
      ? DataType.fromPartial(object.columnType)
      : undefined;
    message.columnId = object.columnId ?? 0;
    message.name = object.name ?? "";
    message.fieldDescs = object.fieldDescs?.map((e) => ColumnDesc.fromPartial(e)) || [];
    message.typeName = object.typeName ?? "";
    return message;
  },
};

function createBaseColumnCatalog(): ColumnCatalog {
  return { columnDesc: undefined, isHidden: false };
}

export const ColumnCatalog = {
  encode(message: ColumnCatalog, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.columnDesc !== undefined) {
      ColumnDesc.encode(message.columnDesc, writer.uint32(10).fork()).ldelim();
    }
    if (message.isHidden === true) {
      writer.uint32(16).bool(message.isHidden);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ColumnCatalog {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseColumnCatalog();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnDesc = ColumnDesc.decode(reader, reader.uint32());
          break;
        case 2:
          message.isHidden = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ColumnCatalog {
    return {
      columnDesc: isSet(object.columnDesc) ? ColumnDesc.fromJSON(object.columnDesc) : undefined,
      isHidden: isSet(object.isHidden) ? Boolean(object.isHidden) : false,
    };
  },

  toJSON(message: ColumnCatalog): unknown {
    const obj: any = {};
    message.columnDesc !== undefined &&
      (obj.columnDesc = message.columnDesc ? ColumnDesc.toJSON(message.columnDesc) : undefined);
    message.isHidden !== undefined && (obj.isHidden = message.isHidden);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnCatalog>, I>>(object: I): ColumnCatalog {
    const message = createBaseColumnCatalog();
    message.columnDesc = (object.columnDesc !== undefined && object.columnDesc !== null)
      ? ColumnDesc.fromPartial(object.columnDesc)
      : undefined;
    message.isHidden = object.isHidden ?? false;
    return message;
  },
};

function createBaseStorageTableDesc(): StorageTableDesc {
  return { tableId: 0, columns: [], orderKey: [], distKeyIndices: [], retentionSeconds: 0 };
}

export const StorageTableDesc = {
  encode(message: StorageTableDesc, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableId !== 0) {
      writer.uint32(8).uint32(message.tableId);
    }
    for (const v of message.columns) {
      ColumnDesc.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.orderKey) {
      ColumnOrder.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    writer.uint32(34).fork();
    for (const v of message.distKeyIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    if (message.retentionSeconds !== 0) {
      writer.uint32(40).uint32(message.retentionSeconds);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StorageTableDesc {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStorageTableDesc();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableId = reader.uint32();
          break;
        case 2:
          message.columns.push(ColumnDesc.decode(reader, reader.uint32()));
          break;
        case 3:
          message.orderKey.push(ColumnOrder.decode(reader, reader.uint32()));
          break;
        case 4:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.distKeyIndices.push(reader.uint32());
            }
          } else {
            message.distKeyIndices.push(reader.uint32());
          }
          break;
        case 5:
          message.retentionSeconds = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StorageTableDesc {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnDesc.fromJSON(e)) : [],
      orderKey: Array.isArray(object?.orderKey) ? object.orderKey.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      distKeyIndices: Array.isArray(object?.distKeyIndices) ? object.distKeyIndices.map((e: any) => Number(e)) : [],
      retentionSeconds: isSet(object.retentionSeconds) ? Number(object.retentionSeconds) : 0,
    };
  },

  toJSON(message: StorageTableDesc): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.orderKey) {
      obj.orderKey = message.orderKey.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.orderKey = [];
    }
    if (message.distKeyIndices) {
      obj.distKeyIndices = message.distKeyIndices.map((e) => Math.round(e));
    } else {
      obj.distKeyIndices = [];
    }
    message.retentionSeconds !== undefined && (obj.retentionSeconds = Math.round(message.retentionSeconds));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StorageTableDesc>, I>>(object: I): StorageTableDesc {
    const message = createBaseStorageTableDesc();
    message.tableId = object.tableId ?? 0;
    message.columns = object.columns?.map((e) => ColumnDesc.fromPartial(e)) || [];
    message.orderKey = object.orderKey?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.distKeyIndices = object.distKeyIndices?.map((e) => e) || [];
    message.retentionSeconds = object.retentionSeconds ?? 0;
    return message;
  },
};

function createBaseColumnOrder(): ColumnOrder {
  return { orderType: 0, index: 0 };
}

export const ColumnOrder = {
  encode(message: ColumnOrder, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.orderType !== 0) {
      writer.uint32(8).int32(message.orderType);
    }
    if (message.index !== 0) {
      writer.uint32(16).uint32(message.index);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ColumnOrder {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseColumnOrder();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.orderType = reader.int32() as any;
          break;
        case 2:
          message.index = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ColumnOrder {
    return {
      orderType: isSet(object.orderType) ? orderTypeFromJSON(object.orderType) : 0,
      index: isSet(object.index) ? Number(object.index) : 0,
    };
  },

  toJSON(message: ColumnOrder): unknown {
    const obj: any = {};
    message.orderType !== undefined && (obj.orderType = orderTypeToJSON(message.orderType));
    message.index !== undefined && (obj.index = Math.round(message.index));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnOrder>, I>>(object: I): ColumnOrder {
    const message = createBaseColumnOrder();
    message.orderType = object.orderType ?? 0;
    message.index = object.index ?? 0;
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
