/* eslint-disable */
import { DataType } from "./data";

export const protobufPackage = "plan_common";

export const JoinType = {
  /**
   * UNSPECIFIED - Note that it comes from Calcite's JoinRelType.
   * DO NOT HAVE direction for SEMI and ANTI now.
   */
  UNSPECIFIED: "UNSPECIFIED",
  INNER: "INNER",
  LEFT_OUTER: "LEFT_OUTER",
  RIGHT_OUTER: "RIGHT_OUTER",
  FULL_OUTER: "FULL_OUTER",
  LEFT_SEMI: "LEFT_SEMI",
  LEFT_ANTI: "LEFT_ANTI",
  RIGHT_SEMI: "RIGHT_SEMI",
  RIGHT_ANTI: "RIGHT_ANTI",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type JoinType = typeof JoinType[keyof typeof JoinType];

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

export const OrderType = {
  ORDER_UNSPECIFIED: "ORDER_UNSPECIFIED",
  ASCENDING: "ASCENDING",
  DESCENDING: "DESCENDING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type OrderType = typeof OrderType[keyof typeof OrderType];

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

export const RowFormatType = {
  ROW_UNSPECIFIED: "ROW_UNSPECIFIED",
  JSON: "JSON",
  PROTOBUF: "PROTOBUF",
  DEBEZIUM_JSON: "DEBEZIUM_JSON",
  AVRO: "AVRO",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type RowFormatType = typeof RowFormatType[keyof typeof RowFormatType];

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
  pk: ColumnOrder[];
  distKeyIndices: number[];
  retentionSeconds: number;
  valueIndices: number[];
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
  return { tableId: 0, columns: [], pk: [], distKeyIndices: [], retentionSeconds: 0, valueIndices: [] };
}

export const StorageTableDesc = {
  fromJSON(object: any): StorageTableDesc {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnDesc.fromJSON(e)) : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      distKeyIndices: Array.isArray(object?.distKeyIndices) ? object.distKeyIndices.map((e: any) => Number(e)) : [],
      retentionSeconds: isSet(object.retentionSeconds) ? Number(object.retentionSeconds) : 0,
      valueIndices: Array.isArray(object?.valueIndices) ? object.valueIndices.map((e: any) => Number(e)) : [],
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
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.distKeyIndices) {
      obj.distKeyIndices = message.distKeyIndices.map((e) => Math.round(e));
    } else {
      obj.distKeyIndices = [];
    }
    message.retentionSeconds !== undefined && (obj.retentionSeconds = Math.round(message.retentionSeconds));
    if (message.valueIndices) {
      obj.valueIndices = message.valueIndices.map((e) => Math.round(e));
    } else {
      obj.valueIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StorageTableDesc>, I>>(object: I): StorageTableDesc {
    const message = createBaseStorageTableDesc();
    message.tableId = object.tableId ?? 0;
    message.columns = object.columns?.map((e) => ColumnDesc.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.distKeyIndices = object.distKeyIndices?.map((e) => e) || [];
    message.retentionSeconds = object.retentionSeconds ?? 0;
    message.valueIndices = object.valueIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseColumnOrder(): ColumnOrder {
  return { orderType: OrderType.ORDER_UNSPECIFIED, index: 0 };
}

export const ColumnOrder = {
  fromJSON(object: any): ColumnOrder {
    return {
      orderType: isSet(object.orderType) ? orderTypeFromJSON(object.orderType) : OrderType.ORDER_UNSPECIFIED,
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
    message.orderType = object.orderType ?? OrderType.ORDER_UNSPECIFIED;
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
