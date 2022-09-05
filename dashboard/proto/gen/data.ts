/* eslint-disable */
import { Buffer } from "./common";

export const protobufPackage = "data";

export const RwArrayType = {
  UNSPECIFIED: "UNSPECIFIED",
  INT16: "INT16",
  INT32: "INT32",
  INT64: "INT64",
  FLOAT32: "FLOAT32",
  FLOAT64: "FLOAT64",
  UTF8: "UTF8",
  BOOL: "BOOL",
  DECIMAL: "DECIMAL",
  DATE: "DATE",
  TIME: "TIME",
  TIMESTAMP: "TIMESTAMP",
  INTERVAL: "INTERVAL",
  STRUCT: "STRUCT",
  LIST: "LIST",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type RwArrayType = typeof RwArrayType[keyof typeof RwArrayType];

export function rwArrayTypeFromJSON(object: any): RwArrayType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return RwArrayType.UNSPECIFIED;
    case 1:
    case "INT16":
      return RwArrayType.INT16;
    case 2:
    case "INT32":
      return RwArrayType.INT32;
    case 3:
    case "INT64":
      return RwArrayType.INT64;
    case 4:
    case "FLOAT32":
      return RwArrayType.FLOAT32;
    case 5:
    case "FLOAT64":
      return RwArrayType.FLOAT64;
    case 6:
    case "UTF8":
      return RwArrayType.UTF8;
    case 7:
    case "BOOL":
      return RwArrayType.BOOL;
    case 8:
    case "DECIMAL":
      return RwArrayType.DECIMAL;
    case 9:
    case "DATE":
      return RwArrayType.DATE;
    case 10:
    case "TIME":
      return RwArrayType.TIME;
    case 11:
    case "TIMESTAMP":
      return RwArrayType.TIMESTAMP;
    case 12:
    case "INTERVAL":
      return RwArrayType.INTERVAL;
    case 13:
    case "STRUCT":
      return RwArrayType.STRUCT;
    case 14:
    case "LIST":
      return RwArrayType.LIST;
    case -1:
    case "UNRECOGNIZED":
    default:
      return RwArrayType.UNRECOGNIZED;
  }
}

export function rwArrayTypeToJSON(object: RwArrayType): string {
  switch (object) {
    case RwArrayType.UNSPECIFIED:
      return "UNSPECIFIED";
    case RwArrayType.INT16:
      return "INT16";
    case RwArrayType.INT32:
      return "INT32";
    case RwArrayType.INT64:
      return "INT64";
    case RwArrayType.FLOAT32:
      return "FLOAT32";
    case RwArrayType.FLOAT64:
      return "FLOAT64";
    case RwArrayType.UTF8:
      return "UTF8";
    case RwArrayType.BOOL:
      return "BOOL";
    case RwArrayType.DECIMAL:
      return "DECIMAL";
    case RwArrayType.DATE:
      return "DATE";
    case RwArrayType.TIME:
      return "TIME";
    case RwArrayType.TIMESTAMP:
      return "TIMESTAMP";
    case RwArrayType.INTERVAL:
      return "INTERVAL";
    case RwArrayType.STRUCT:
      return "STRUCT";
    case RwArrayType.LIST:
      return "LIST";
    case RwArrayType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const Op = {
  OP_UNSPECIFIED: "OP_UNSPECIFIED",
  INSERT: "INSERT",
  DELETE: "DELETE",
  UPDATE_INSERT: "UPDATE_INSERT",
  UPDATE_DELETE: "UPDATE_DELETE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Op = typeof Op[keyof typeof Op];

export function opFromJSON(object: any): Op {
  switch (object) {
    case 0:
    case "OP_UNSPECIFIED":
      return Op.OP_UNSPECIFIED;
    case 1:
    case "INSERT":
      return Op.INSERT;
    case 2:
    case "DELETE":
      return Op.DELETE;
    case 3:
    case "UPDATE_INSERT":
      return Op.UPDATE_INSERT;
    case 4:
    case "UPDATE_DELETE":
      return Op.UPDATE_DELETE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Op.UNRECOGNIZED;
  }
}

export function opToJSON(object: Op): string {
  switch (object) {
    case Op.OP_UNSPECIFIED:
      return "OP_UNSPECIFIED";
    case Op.INSERT:
      return "INSERT";
    case Op.DELETE:
      return "DELETE";
    case Op.UPDATE_INSERT:
      return "UPDATE_INSERT";
    case Op.UPDATE_DELETE:
      return "UPDATE_DELETE";
    case Op.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface IntervalUnit {
  months: number;
  days: number;
  ms: number;
}

export interface DataType {
  typeName: DataType_TypeName;
  /**
   * Data length for char.
   * Max data length for varchar.
   * Precision for time, decimal.
   */
  precision: number;
  /** Scale for decimal. */
  scale: number;
  isNullable: boolean;
  intervalType: DataType_IntervalType;
  /**
   * For struct type, it represents all the fields in the struct.
   * For list type it only contains 1 element which is the inner item type of the List.
   * For example, `ARRAY<INTEGER>` will be represented as `vec![DataType::Int32]`.
   */
  fieldType: DataType[];
  /** Name of the fields if it is a struct type. For other types it will be empty. */
  fieldNames: string[];
}

export const DataType_IntervalType = {
  UNSPECIFIED: "UNSPECIFIED",
  YEAR: "YEAR",
  MONTH: "MONTH",
  DAY: "DAY",
  HOUR: "HOUR",
  MINUTE: "MINUTE",
  SECOND: "SECOND",
  YEAR_TO_MONTH: "YEAR_TO_MONTH",
  DAY_TO_HOUR: "DAY_TO_HOUR",
  DAY_TO_MINUTE: "DAY_TO_MINUTE",
  DAY_TO_SECOND: "DAY_TO_SECOND",
  HOUR_TO_MINUTE: "HOUR_TO_MINUTE",
  HOUR_TO_SECOND: "HOUR_TO_SECOND",
  MINUTE_TO_SECOND: "MINUTE_TO_SECOND",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type DataType_IntervalType = typeof DataType_IntervalType[keyof typeof DataType_IntervalType];

export function dataType_IntervalTypeFromJSON(object: any): DataType_IntervalType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return DataType_IntervalType.UNSPECIFIED;
    case 1:
    case "YEAR":
      return DataType_IntervalType.YEAR;
    case 2:
    case "MONTH":
      return DataType_IntervalType.MONTH;
    case 3:
    case "DAY":
      return DataType_IntervalType.DAY;
    case 4:
    case "HOUR":
      return DataType_IntervalType.HOUR;
    case 5:
    case "MINUTE":
      return DataType_IntervalType.MINUTE;
    case 6:
    case "SECOND":
      return DataType_IntervalType.SECOND;
    case 7:
    case "YEAR_TO_MONTH":
      return DataType_IntervalType.YEAR_TO_MONTH;
    case 8:
    case "DAY_TO_HOUR":
      return DataType_IntervalType.DAY_TO_HOUR;
    case 9:
    case "DAY_TO_MINUTE":
      return DataType_IntervalType.DAY_TO_MINUTE;
    case 10:
    case "DAY_TO_SECOND":
      return DataType_IntervalType.DAY_TO_SECOND;
    case 11:
    case "HOUR_TO_MINUTE":
      return DataType_IntervalType.HOUR_TO_MINUTE;
    case 12:
    case "HOUR_TO_SECOND":
      return DataType_IntervalType.HOUR_TO_SECOND;
    case 13:
    case "MINUTE_TO_SECOND":
      return DataType_IntervalType.MINUTE_TO_SECOND;
    case -1:
    case "UNRECOGNIZED":
    default:
      return DataType_IntervalType.UNRECOGNIZED;
  }
}

export function dataType_IntervalTypeToJSON(object: DataType_IntervalType): string {
  switch (object) {
    case DataType_IntervalType.UNSPECIFIED:
      return "UNSPECIFIED";
    case DataType_IntervalType.YEAR:
      return "YEAR";
    case DataType_IntervalType.MONTH:
      return "MONTH";
    case DataType_IntervalType.DAY:
      return "DAY";
    case DataType_IntervalType.HOUR:
      return "HOUR";
    case DataType_IntervalType.MINUTE:
      return "MINUTE";
    case DataType_IntervalType.SECOND:
      return "SECOND";
    case DataType_IntervalType.YEAR_TO_MONTH:
      return "YEAR_TO_MONTH";
    case DataType_IntervalType.DAY_TO_HOUR:
      return "DAY_TO_HOUR";
    case DataType_IntervalType.DAY_TO_MINUTE:
      return "DAY_TO_MINUTE";
    case DataType_IntervalType.DAY_TO_SECOND:
      return "DAY_TO_SECOND";
    case DataType_IntervalType.HOUR_TO_MINUTE:
      return "HOUR_TO_MINUTE";
    case DataType_IntervalType.HOUR_TO_SECOND:
      return "HOUR_TO_SECOND";
    case DataType_IntervalType.MINUTE_TO_SECOND:
      return "MINUTE_TO_SECOND";
    case DataType_IntervalType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const DataType_TypeName = {
  TYPE_UNSPECIFIED: "TYPE_UNSPECIFIED",
  INT16: "INT16",
  INT32: "INT32",
  INT64: "INT64",
  FLOAT: "FLOAT",
  DOUBLE: "DOUBLE",
  BOOLEAN: "BOOLEAN",
  VARCHAR: "VARCHAR",
  DECIMAL: "DECIMAL",
  TIME: "TIME",
  TIMESTAMP: "TIMESTAMP",
  INTERVAL: "INTERVAL",
  DATE: "DATE",
  /** TIMESTAMPZ - Timestamp type with timezone */
  TIMESTAMPZ: "TIMESTAMPZ",
  STRUCT: "STRUCT",
  LIST: "LIST",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type DataType_TypeName = typeof DataType_TypeName[keyof typeof DataType_TypeName];

export function dataType_TypeNameFromJSON(object: any): DataType_TypeName {
  switch (object) {
    case 0:
    case "TYPE_UNSPECIFIED":
      return DataType_TypeName.TYPE_UNSPECIFIED;
    case 1:
    case "INT16":
      return DataType_TypeName.INT16;
    case 2:
    case "INT32":
      return DataType_TypeName.INT32;
    case 3:
    case "INT64":
      return DataType_TypeName.INT64;
    case 4:
    case "FLOAT":
      return DataType_TypeName.FLOAT;
    case 5:
    case "DOUBLE":
      return DataType_TypeName.DOUBLE;
    case 6:
    case "BOOLEAN":
      return DataType_TypeName.BOOLEAN;
    case 7:
    case "VARCHAR":
      return DataType_TypeName.VARCHAR;
    case 8:
    case "DECIMAL":
      return DataType_TypeName.DECIMAL;
    case 9:
    case "TIME":
      return DataType_TypeName.TIME;
    case 10:
    case "TIMESTAMP":
      return DataType_TypeName.TIMESTAMP;
    case 11:
    case "INTERVAL":
      return DataType_TypeName.INTERVAL;
    case 12:
    case "DATE":
      return DataType_TypeName.DATE;
    case 13:
    case "TIMESTAMPZ":
      return DataType_TypeName.TIMESTAMPZ;
    case 15:
    case "STRUCT":
      return DataType_TypeName.STRUCT;
    case 16:
    case "LIST":
      return DataType_TypeName.LIST;
    case -1:
    case "UNRECOGNIZED":
    default:
      return DataType_TypeName.UNRECOGNIZED;
  }
}

export function dataType_TypeNameToJSON(object: DataType_TypeName): string {
  switch (object) {
    case DataType_TypeName.TYPE_UNSPECIFIED:
      return "TYPE_UNSPECIFIED";
    case DataType_TypeName.INT16:
      return "INT16";
    case DataType_TypeName.INT32:
      return "INT32";
    case DataType_TypeName.INT64:
      return "INT64";
    case DataType_TypeName.FLOAT:
      return "FLOAT";
    case DataType_TypeName.DOUBLE:
      return "DOUBLE";
    case DataType_TypeName.BOOLEAN:
      return "BOOLEAN";
    case DataType_TypeName.VARCHAR:
      return "VARCHAR";
    case DataType_TypeName.DECIMAL:
      return "DECIMAL";
    case DataType_TypeName.TIME:
      return "TIME";
    case DataType_TypeName.TIMESTAMP:
      return "TIMESTAMP";
    case DataType_TypeName.INTERVAL:
      return "INTERVAL";
    case DataType_TypeName.DATE:
      return "DATE";
    case DataType_TypeName.TIMESTAMPZ:
      return "TIMESTAMPZ";
    case DataType_TypeName.STRUCT:
      return "STRUCT";
    case DataType_TypeName.LIST:
      return "LIST";
    case DataType_TypeName.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface StructRwArrayData {
  childrenArray: RwArray[];
  childrenType: DataType[];
}

export interface ListRwArrayData {
  offsets: number[];
  value: RwArray | undefined;
  valueType: DataType | undefined;
}

export interface RwArray {
  arrayType: RwArrayType;
  nullBitmap: Buffer | undefined;
  values: Buffer[];
  structArrayData: StructRwArrayData | undefined;
  listArrayData: ListRwArrayData | undefined;
}

/**
 * New column proto def to replace fixed width column. This def
 * aims to include all column type. Currently it do not support struct/array
 * but capable of extending in future by add other fields.
 */
export interface Column {
  array: RwArray | undefined;
}

export interface DataChunk {
  cardinality: number;
  columns: Column[];
}

export interface StreamChunk {
  /** for Column::from_protobuf(), may not need later */
  cardinality: number;
  ops: Op[];
  columns: Column[];
}

export interface Epoch {
  curr: number;
  prev: number;
}

export interface Terminate {
}

function createBaseIntervalUnit(): IntervalUnit {
  return { months: 0, days: 0, ms: 0 };
}

export const IntervalUnit = {
  fromJSON(object: any): IntervalUnit {
    return {
      months: isSet(object.months) ? Number(object.months) : 0,
      days: isSet(object.days) ? Number(object.days) : 0,
      ms: isSet(object.ms) ? Number(object.ms) : 0,
    };
  },

  toJSON(message: IntervalUnit): unknown {
    const obj: any = {};
    message.months !== undefined && (obj.months = Math.round(message.months));
    message.days !== undefined && (obj.days = Math.round(message.days));
    message.ms !== undefined && (obj.ms = Math.round(message.ms));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<IntervalUnit>, I>>(object: I): IntervalUnit {
    const message = createBaseIntervalUnit();
    message.months = object.months ?? 0;
    message.days = object.days ?? 0;
    message.ms = object.ms ?? 0;
    return message;
  },
};

function createBaseDataType(): DataType {
  return {
    typeName: DataType_TypeName.TYPE_UNSPECIFIED,
    precision: 0,
    scale: 0,
    isNullable: false,
    intervalType: DataType_IntervalType.UNSPECIFIED,
    fieldType: [],
    fieldNames: [],
  };
}

export const DataType = {
  fromJSON(object: any): DataType {
    return {
      typeName: isSet(object.typeName)
        ? dataType_TypeNameFromJSON(object.typeName)
        : DataType_TypeName.TYPE_UNSPECIFIED,
      precision: isSet(object.precision) ? Number(object.precision) : 0,
      scale: isSet(object.scale) ? Number(object.scale) : 0,
      isNullable: isSet(object.isNullable) ? Boolean(object.isNullable) : false,
      intervalType: isSet(object.intervalType)
        ? dataType_IntervalTypeFromJSON(object.intervalType)
        : DataType_IntervalType.UNSPECIFIED,
      fieldType: Array.isArray(object?.fieldType) ? object.fieldType.map((e: any) => DataType.fromJSON(e)) : [],
      fieldNames: Array.isArray(object?.fieldNames) ? object.fieldNames.map((e: any) => String(e)) : [],
    };
  },

  toJSON(message: DataType): unknown {
    const obj: any = {};
    message.typeName !== undefined && (obj.typeName = dataType_TypeNameToJSON(message.typeName));
    message.precision !== undefined && (obj.precision = Math.round(message.precision));
    message.scale !== undefined && (obj.scale = Math.round(message.scale));
    message.isNullable !== undefined && (obj.isNullable = message.isNullable);
    message.intervalType !== undefined && (obj.intervalType = dataType_IntervalTypeToJSON(message.intervalType));
    if (message.fieldType) {
      obj.fieldType = message.fieldType.map((e) => e ? DataType.toJSON(e) : undefined);
    } else {
      obj.fieldType = [];
    }
    if (message.fieldNames) {
      obj.fieldNames = message.fieldNames.map((e) => e);
    } else {
      obj.fieldNames = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DataType>, I>>(object: I): DataType {
    const message = createBaseDataType();
    message.typeName = object.typeName ?? DataType_TypeName.TYPE_UNSPECIFIED;
    message.precision = object.precision ?? 0;
    message.scale = object.scale ?? 0;
    message.isNullable = object.isNullable ?? false;
    message.intervalType = object.intervalType ?? DataType_IntervalType.UNSPECIFIED;
    message.fieldType = object.fieldType?.map((e) => DataType.fromPartial(e)) || [];
    message.fieldNames = object.fieldNames?.map((e) => e) || [];
    return message;
  },
};

function createBaseStructRwArrayData(): StructRwArrayData {
  return { childrenArray: [], childrenType: [] };
}

export const StructRwArrayData = {
  fromJSON(object: any): StructRwArrayData {
    return {
      childrenArray: Array.isArray(object?.childrenArray)
        ? object.childrenArray.map((e: any) => RwArray.fromJSON(e))
        : [],
      childrenType: Array.isArray(object?.childrenType)
        ? object.childrenType.map((e: any) => DataType.fromJSON(e))
        : [],
    };
  },

  toJSON(message: StructRwArrayData): unknown {
    const obj: any = {};
    if (message.childrenArray) {
      obj.childrenArray = message.childrenArray.map((e) => e ? RwArray.toJSON(e) : undefined);
    } else {
      obj.childrenArray = [];
    }
    if (message.childrenType) {
      obj.childrenType = message.childrenType.map((e) => e ? DataType.toJSON(e) : undefined);
    } else {
      obj.childrenType = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StructRwArrayData>, I>>(object: I): StructRwArrayData {
    const message = createBaseStructRwArrayData();
    message.childrenArray = object.childrenArray?.map((e) => RwArray.fromPartial(e)) || [];
    message.childrenType = object.childrenType?.map((e) => DataType.fromPartial(e)) || [];
    return message;
  },
};

function createBaseListRwArrayData(): ListRwArrayData {
  return { offsets: [], value: undefined, valueType: undefined };
}

export const ListRwArrayData = {
  fromJSON(object: any): ListRwArrayData {
    return {
      offsets: Array.isArray(object?.offsets) ? object.offsets.map((e: any) => Number(e)) : [],
      value: isSet(object.value) ? RwArray.fromJSON(object.value) : undefined,
      valueType: isSet(object.valueType) ? DataType.fromJSON(object.valueType) : undefined,
    };
  },

  toJSON(message: ListRwArrayData): unknown {
    const obj: any = {};
    if (message.offsets) {
      obj.offsets = message.offsets.map((e) => Math.round(e));
    } else {
      obj.offsets = [];
    }
    message.value !== undefined && (obj.value = message.value ? RwArray.toJSON(message.value) : undefined);
    message.valueType !== undefined &&
      (obj.valueType = message.valueType ? DataType.toJSON(message.valueType) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListRwArrayData>, I>>(object: I): ListRwArrayData {
    const message = createBaseListRwArrayData();
    message.offsets = object.offsets?.map((e) => e) || [];
    message.value = (object.value !== undefined && object.value !== null)
      ? RwArray.fromPartial(object.value)
      : undefined;
    message.valueType = (object.valueType !== undefined && object.valueType !== null)
      ? DataType.fromPartial(object.valueType)
      : undefined;
    return message;
  },
};

function createBaseRwArray(): RwArray {
  return {
    arrayType: RwArrayType.UNSPECIFIED,
    nullBitmap: undefined,
    values: [],
    structArrayData: undefined,
    listArrayData: undefined,
  };
}

export const RwArray = {
  fromJSON(object: any): RwArray {
    return {
      arrayType: isSet(object.arrayType) ? rwArrayTypeFromJSON(object.arrayType) : RwArrayType.UNSPECIFIED,
      nullBitmap: isSet(object.nullBitmap) ? Buffer.fromJSON(object.nullBitmap) : undefined,
      values: Array.isArray(object?.values) ? object.values.map((e: any) => Buffer.fromJSON(e)) : [],
      structArrayData: isSet(object.structArrayData) ? StructRwArrayData.fromJSON(object.structArrayData) : undefined,
      listArrayData: isSet(object.listArrayData) ? ListRwArrayData.fromJSON(object.listArrayData) : undefined,
    };
  },

  toJSON(message: RwArray): unknown {
    const obj: any = {};
    message.arrayType !== undefined && (obj.arrayType = rwArrayTypeToJSON(message.arrayType));
    message.nullBitmap !== undefined &&
      (obj.nullBitmap = message.nullBitmap ? Buffer.toJSON(message.nullBitmap) : undefined);
    if (message.values) {
      obj.values = message.values.map((e) => e ? Buffer.toJSON(e) : undefined);
    } else {
      obj.values = [];
    }
    message.structArrayData !== undefined &&
      (obj.structArrayData = message.structArrayData ? StructRwArrayData.toJSON(message.structArrayData) : undefined);
    message.listArrayData !== undefined &&
      (obj.listArrayData = message.listArrayData ? ListRwArrayData.toJSON(message.listArrayData) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RwArray>, I>>(object: I): RwArray {
    const message = createBaseRwArray();
    message.arrayType = object.arrayType ?? RwArrayType.UNSPECIFIED;
    message.nullBitmap = (object.nullBitmap !== undefined && object.nullBitmap !== null)
      ? Buffer.fromPartial(object.nullBitmap)
      : undefined;
    message.values = object.values?.map((e) => Buffer.fromPartial(e)) || [];
    message.structArrayData = (object.structArrayData !== undefined && object.structArrayData !== null)
      ? StructRwArrayData.fromPartial(object.structArrayData)
      : undefined;
    message.listArrayData = (object.listArrayData !== undefined && object.listArrayData !== null)
      ? ListRwArrayData.fromPartial(object.listArrayData)
      : undefined;
    return message;
  },
};

function createBaseColumn(): Column {
  return { array: undefined };
}

export const Column = {
  fromJSON(object: any): Column {
    return { array: isSet(object.array) ? RwArray.fromJSON(object.array) : undefined };
  },

  toJSON(message: Column): unknown {
    const obj: any = {};
    message.array !== undefined && (obj.array = message.array ? RwArray.toJSON(message.array) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Column>, I>>(object: I): Column {
    const message = createBaseColumn();
    message.array = (object.array !== undefined && object.array !== null)
      ? RwArray.fromPartial(object.array)
      : undefined;
    return message;
  },
};

function createBaseDataChunk(): DataChunk {
  return { cardinality: 0, columns: [] };
}

export const DataChunk = {
  fromJSON(object: any): DataChunk {
    return {
      cardinality: isSet(object.cardinality) ? Number(object.cardinality) : 0,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => Column.fromJSON(e)) : [],
    };
  },

  toJSON(message: DataChunk): unknown {
    const obj: any = {};
    message.cardinality !== undefined && (obj.cardinality = Math.round(message.cardinality));
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? Column.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DataChunk>, I>>(object: I): DataChunk {
    const message = createBaseDataChunk();
    message.cardinality = object.cardinality ?? 0;
    message.columns = object.columns?.map((e) => Column.fromPartial(e)) || [];
    return message;
  },
};

function createBaseStreamChunk(): StreamChunk {
  return { cardinality: 0, ops: [], columns: [] };
}

export const StreamChunk = {
  fromJSON(object: any): StreamChunk {
    return {
      cardinality: isSet(object.cardinality) ? Number(object.cardinality) : 0,
      ops: Array.isArray(object?.ops) ? object.ops.map((e: any) => opFromJSON(e)) : [],
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => Column.fromJSON(e)) : [],
    };
  },

  toJSON(message: StreamChunk): unknown {
    const obj: any = {};
    message.cardinality !== undefined && (obj.cardinality = Math.round(message.cardinality));
    if (message.ops) {
      obj.ops = message.ops.map((e) => opToJSON(e));
    } else {
      obj.ops = [];
    }
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? Column.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamChunk>, I>>(object: I): StreamChunk {
    const message = createBaseStreamChunk();
    message.cardinality = object.cardinality ?? 0;
    message.ops = object.ops?.map((e) => e) || [];
    message.columns = object.columns?.map((e) => Column.fromPartial(e)) || [];
    return message;
  },
};

function createBaseEpoch(): Epoch {
  return { curr: 0, prev: 0 };
}

export const Epoch = {
  fromJSON(object: any): Epoch {
    return { curr: isSet(object.curr) ? Number(object.curr) : 0, prev: isSet(object.prev) ? Number(object.prev) : 0 };
  },

  toJSON(message: Epoch): unknown {
    const obj: any = {};
    message.curr !== undefined && (obj.curr = Math.round(message.curr));
    message.prev !== undefined && (obj.prev = Math.round(message.prev));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Epoch>, I>>(object: I): Epoch {
    const message = createBaseEpoch();
    message.curr = object.curr ?? 0;
    message.prev = object.prev ?? 0;
    return message;
  },
};

function createBaseTerminate(): Terminate {
  return {};
}

export const Terminate = {
  fromJSON(_: any): Terminate {
    return {};
  },

  toJSON(_: Terminate): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Terminate>, I>>(_: I): Terminate {
    const message = createBaseTerminate();
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
