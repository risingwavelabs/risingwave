/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { ExprNode } from "./expr";
import { ColumnCatalog, ColumnOrder, RowFormatType, rowFormatTypeFromJSON, rowFormatTypeToJSON } from "./plan_common";

export const protobufPackage = "catalog";

/**
 * The rust prost library always treats uint64 as required and message as optional.
 * In order to allow `row_id_index` as optional field in `StreamSourceInfo` and `TableSourceInfo`,
 * we wrap uint64 inside this message.
 */
export interface ColumnIndex {
  index: number;
}

export interface StreamSourceInfo {
  properties: { [key: string]: string };
  rowFormat: RowFormatType;
  rowSchemaLocation: string;
  rowIdIndex: ColumnIndex | undefined;
  columns: ColumnCatalog[];
  pkColumnIds: number[];
}

export interface StreamSourceInfo_PropertiesEntry {
  key: string;
  value: string;
}

export interface TableSourceInfo {
  rowIdIndex: ColumnIndex | undefined;
  columns: ColumnCatalog[];
  pkColumnIds: number[];
  properties: { [key: string]: string };
}

export interface TableSourceInfo_PropertiesEntry {
  key: string;
  value: string;
}

export interface Source {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  info?: { $case: "streamSource"; streamSource: StreamSourceInfo } | {
    $case: "tableSource";
    tableSource: TableSourceInfo;
  };
  owner: number;
}

export interface Sink {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  associatedTableId: number;
  properties: { [key: string]: string };
  owner: number;
  dependentRelations: number[];
}

export interface Sink_PropertiesEntry {
  key: string;
  value: string;
}

export interface Index {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  owner: number;
  indexTableId: number;
  primaryTableId: number;
  /**
   * Only `InputRef` type index is supported Now.
   * The index of `InputRef` is the column index of the primary table.
   */
  indexItem: ExprNode[];
}

/** See `TableCatalog` struct in frontend crate for more information. */
export interface Table {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  columns: ColumnCatalog[];
  orderKey: ColumnOrder[];
  dependentRelations: number[];
  optionalAssociatedSourceId?: { $case: "associatedSourceId"; associatedSourceId: number };
  isIndex: boolean;
  indexOnId: number;
  distributionKey: number[];
  /** pk_indices of the corresponding materialize operator's output. */
  streamKey: number[];
  appendonly: boolean;
  owner: number;
  properties: { [key: string]: string };
  fragmentId: number;
  /** an optional column index which is the vnode of each row computed by the table's consistent hash distribution */
  vnodeColIdx: ColumnIndex | undefined;
}

export interface Table_PropertiesEntry {
  key: string;
  value: string;
}

export interface Schema {
  id: number;
  databaseId: number;
  name: string;
  owner: number;
}

export interface Database {
  id: number;
  name: string;
  owner: number;
}

function createBaseColumnIndex(): ColumnIndex {
  return { index: 0 };
}

export const ColumnIndex = {
  encode(message: ColumnIndex, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.index !== 0) {
      writer.uint32(8).uint64(message.index);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ColumnIndex {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseColumnIndex();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.index = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ColumnIndex {
    return { index: isSet(object.index) ? Number(object.index) : 0 };
  },

  toJSON(message: ColumnIndex): unknown {
    const obj: any = {};
    message.index !== undefined && (obj.index = Math.round(message.index));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnIndex>, I>>(object: I): ColumnIndex {
    const message = createBaseColumnIndex();
    message.index = object.index ?? 0;
    return message;
  },
};

function createBaseStreamSourceInfo(): StreamSourceInfo {
  return { properties: {}, rowFormat: 0, rowSchemaLocation: "", rowIdIndex: undefined, columns: [], pkColumnIds: [] };
}

export const StreamSourceInfo = {
  encode(message: StreamSourceInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.properties).forEach(([key, value]) => {
      StreamSourceInfo_PropertiesEntry.encode({ key: key as any, value }, writer.uint32(10).fork()).ldelim();
    });
    if (message.rowFormat !== 0) {
      writer.uint32(16).int32(message.rowFormat);
    }
    if (message.rowSchemaLocation !== "") {
      writer.uint32(26).string(message.rowSchemaLocation);
    }
    if (message.rowIdIndex !== undefined) {
      ColumnIndex.encode(message.rowIdIndex, writer.uint32(34).fork()).ldelim();
    }
    for (const v of message.columns) {
      ColumnCatalog.encode(v!, writer.uint32(42).fork()).ldelim();
    }
    writer.uint32(50).fork();
    for (const v of message.pkColumnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StreamSourceInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStreamSourceInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          const entry1 = StreamSourceInfo_PropertiesEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.properties[entry1.key] = entry1.value;
          }
          break;
        case 2:
          message.rowFormat = reader.int32() as any;
          break;
        case 3:
          message.rowSchemaLocation = reader.string();
          break;
        case 4:
          message.rowIdIndex = ColumnIndex.decode(reader, reader.uint32());
          break;
        case 5:
          message.columns.push(ColumnCatalog.decode(reader, reader.uint32()));
          break;
        case 6:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.pkColumnIds.push(reader.int32());
            }
          } else {
            message.pkColumnIds.push(reader.int32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StreamSourceInfo {
    return {
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      rowFormat: isSet(object.rowFormat) ? rowFormatTypeFromJSON(object.rowFormat) : 0,
      rowSchemaLocation: isSet(object.rowSchemaLocation) ? String(object.rowSchemaLocation) : "",
      rowIdIndex: isSet(object.rowIdIndex) ? ColumnIndex.fromJSON(object.rowIdIndex) : undefined,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pkColumnIds: Array.isArray(object?.pkColumnIds) ? object.pkColumnIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: StreamSourceInfo): unknown {
    const obj: any = {};
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.rowFormat !== undefined && (obj.rowFormat = rowFormatTypeToJSON(message.rowFormat));
    message.rowSchemaLocation !== undefined && (obj.rowSchemaLocation = message.rowSchemaLocation);
    message.rowIdIndex !== undefined &&
      (obj.rowIdIndex = message.rowIdIndex ? ColumnIndex.toJSON(message.rowIdIndex) : undefined);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pkColumnIds) {
      obj.pkColumnIds = message.pkColumnIds.map((e) => Math.round(e));
    } else {
      obj.pkColumnIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSourceInfo>, I>>(object: I): StreamSourceInfo {
    const message = createBaseStreamSourceInfo();
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.rowFormat = object.rowFormat ?? 0;
    message.rowSchemaLocation = object.rowSchemaLocation ?? "";
    message.rowIdIndex = (object.rowIdIndex !== undefined && object.rowIdIndex !== null)
      ? ColumnIndex.fromPartial(object.rowIdIndex)
      : undefined;
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pkColumnIds = object.pkColumnIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseStreamSourceInfo_PropertiesEntry(): StreamSourceInfo_PropertiesEntry {
  return { key: "", value: "" };
}

export const StreamSourceInfo_PropertiesEntry = {
  encode(message: StreamSourceInfo_PropertiesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StreamSourceInfo_PropertiesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStreamSourceInfo_PropertiesEntry();
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

  fromJSON(object: any): StreamSourceInfo_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: StreamSourceInfo_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSourceInfo_PropertiesEntry>, I>>(
    object: I,
  ): StreamSourceInfo_PropertiesEntry {
    const message = createBaseStreamSourceInfo_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseTableSourceInfo(): TableSourceInfo {
  return { rowIdIndex: undefined, columns: [], pkColumnIds: [], properties: {} };
}

export const TableSourceInfo = {
  encode(message: TableSourceInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.rowIdIndex !== undefined) {
      ColumnIndex.encode(message.rowIdIndex, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.columns) {
      ColumnCatalog.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    writer.uint32(26).fork();
    for (const v of message.pkColumnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    Object.entries(message.properties).forEach(([key, value]) => {
      TableSourceInfo_PropertiesEntry.encode({ key: key as any, value }, writer.uint32(34).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableSourceInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableSourceInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.rowIdIndex = ColumnIndex.decode(reader, reader.uint32());
          break;
        case 2:
          message.columns.push(ColumnCatalog.decode(reader, reader.uint32()));
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.pkColumnIds.push(reader.int32());
            }
          } else {
            message.pkColumnIds.push(reader.int32());
          }
          break;
        case 4:
          const entry4 = TableSourceInfo_PropertiesEntry.decode(reader, reader.uint32());
          if (entry4.value !== undefined) {
            message.properties[entry4.key] = entry4.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableSourceInfo {
    return {
      rowIdIndex: isSet(object.rowIdIndex) ? ColumnIndex.fromJSON(object.rowIdIndex) : undefined,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pkColumnIds: Array.isArray(object?.pkColumnIds) ? object.pkColumnIds.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: TableSourceInfo): unknown {
    const obj: any = {};
    message.rowIdIndex !== undefined &&
      (obj.rowIdIndex = message.rowIdIndex ? ColumnIndex.toJSON(message.rowIdIndex) : undefined);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pkColumnIds) {
      obj.pkColumnIds = message.pkColumnIds.map((e) => Math.round(e));
    } else {
      obj.pkColumnIds = [];
    }
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableSourceInfo>, I>>(object: I): TableSourceInfo {
    const message = createBaseTableSourceInfo();
    message.rowIdIndex = (object.rowIdIndex !== undefined && object.rowIdIndex !== null)
      ? ColumnIndex.fromPartial(object.rowIdIndex)
      : undefined;
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pkColumnIds = object.pkColumnIds?.map((e) => e) || [];
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
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

function createBaseTableSourceInfo_PropertiesEntry(): TableSourceInfo_PropertiesEntry {
  return { key: "", value: "" };
}

export const TableSourceInfo_PropertiesEntry = {
  encode(message: TableSourceInfo_PropertiesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableSourceInfo_PropertiesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableSourceInfo_PropertiesEntry();
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

  fromJSON(object: any): TableSourceInfo_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: TableSourceInfo_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableSourceInfo_PropertiesEntry>, I>>(
    object: I,
  ): TableSourceInfo_PropertiesEntry {
    const message = createBaseTableSourceInfo_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSource(): Source {
  return { id: 0, schemaId: 0, databaseId: 0, name: "", info: undefined, owner: 0 };
}

export const Source = {
  encode(message: Source, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.schemaId !== 0) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.databaseId !== 0) {
      writer.uint32(24).uint32(message.databaseId);
    }
    if (message.name !== "") {
      writer.uint32(34).string(message.name);
    }
    if (message.info?.$case === "streamSource") {
      StreamSourceInfo.encode(message.info.streamSource, writer.uint32(42).fork()).ldelim();
    }
    if (message.info?.$case === "tableSource") {
      TableSourceInfo.encode(message.info.tableSource, writer.uint32(50).fork()).ldelim();
    }
    if (message.owner !== 0) {
      writer.uint32(56).uint32(message.owner);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Source {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSource();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.databaseId = reader.uint32();
          break;
        case 4:
          message.name = reader.string();
          break;
        case 5:
          message.info = { $case: "streamSource", streamSource: StreamSourceInfo.decode(reader, reader.uint32()) };
          break;
        case 6:
          message.info = { $case: "tableSource", tableSource: TableSourceInfo.decode(reader, reader.uint32()) };
          break;
        case 7:
          message.owner = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Source {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      info: isSet(object.streamSource)
        ? { $case: "streamSource", streamSource: StreamSourceInfo.fromJSON(object.streamSource) }
        : isSet(object.tableSource)
        ? { $case: "tableSource", tableSource: TableSourceInfo.fromJSON(object.tableSource) }
        : undefined,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
    };
  },

  toJSON(message: Source): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.info?.$case === "streamSource" &&
      (obj.streamSource = message.info?.streamSource ? StreamSourceInfo.toJSON(message.info?.streamSource) : undefined);
    message.info?.$case === "tableSource" &&
      (obj.tableSource = message.info?.tableSource ? TableSourceInfo.toJSON(message.info?.tableSource) : undefined);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Source>, I>>(object: I): Source {
    const message = createBaseSource();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    if (
      object.info?.$case === "streamSource" &&
      object.info?.streamSource !== undefined &&
      object.info?.streamSource !== null
    ) {
      message.info = { $case: "streamSource", streamSource: StreamSourceInfo.fromPartial(object.info.streamSource) };
    }
    if (
      object.info?.$case === "tableSource" &&
      object.info?.tableSource !== undefined &&
      object.info?.tableSource !== null
    ) {
      message.info = { $case: "tableSource", tableSource: TableSourceInfo.fromPartial(object.info.tableSource) };
    }
    message.owner = object.owner ?? 0;
    return message;
  },
};

function createBaseSink(): Sink {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    associatedTableId: 0,
    properties: {},
    owner: 0,
    dependentRelations: [],
  };
}

export const Sink = {
  encode(message: Sink, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.schemaId !== 0) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.databaseId !== 0) {
      writer.uint32(24).uint32(message.databaseId);
    }
    if (message.name !== "") {
      writer.uint32(34).string(message.name);
    }
    if (message.associatedTableId !== 0) {
      writer.uint32(40).uint32(message.associatedTableId);
    }
    Object.entries(message.properties).forEach(([key, value]) => {
      Sink_PropertiesEntry.encode({ key: key as any, value }, writer.uint32(50).fork()).ldelim();
    });
    if (message.owner !== 0) {
      writer.uint32(56).uint32(message.owner);
    }
    writer.uint32(66).fork();
    for (const v of message.dependentRelations) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Sink {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSink();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.databaseId = reader.uint32();
          break;
        case 4:
          message.name = reader.string();
          break;
        case 5:
          message.associatedTableId = reader.uint32();
          break;
        case 6:
          const entry6 = Sink_PropertiesEntry.decode(reader, reader.uint32());
          if (entry6.value !== undefined) {
            message.properties[entry6.key] = entry6.value;
          }
          break;
        case 7:
          message.owner = reader.uint32();
          break;
        case 8:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.dependentRelations.push(reader.uint32());
            }
          } else {
            message.dependentRelations.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Sink {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      associatedTableId: isSet(object.associatedTableId) ? Number(object.associatedTableId) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: Sink): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.associatedTableId !== undefined && (obj.associatedTableId = Math.round(message.associatedTableId));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Sink>, I>>(object: I): Sink {
    const message = createBaseSink();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.associatedTableId = object.associatedTableId ?? 0;
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.owner = object.owner ?? 0;
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    return message;
  },
};

function createBaseSink_PropertiesEntry(): Sink_PropertiesEntry {
  return { key: "", value: "" };
}

export const Sink_PropertiesEntry = {
  encode(message: Sink_PropertiesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Sink_PropertiesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSink_PropertiesEntry();
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

  fromJSON(object: any): Sink_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Sink_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Sink_PropertiesEntry>, I>>(object: I): Sink_PropertiesEntry {
    const message = createBaseSink_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseIndex(): Index {
  return { id: 0, schemaId: 0, databaseId: 0, name: "", owner: 0, indexTableId: 0, primaryTableId: 0, indexItem: [] };
}

export const Index = {
  encode(message: Index, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.schemaId !== 0) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.databaseId !== 0) {
      writer.uint32(24).uint32(message.databaseId);
    }
    if (message.name !== "") {
      writer.uint32(34).string(message.name);
    }
    if (message.owner !== 0) {
      writer.uint32(40).uint32(message.owner);
    }
    if (message.indexTableId !== 0) {
      writer.uint32(48).uint32(message.indexTableId);
    }
    if (message.primaryTableId !== 0) {
      writer.uint32(56).uint32(message.primaryTableId);
    }
    for (const v of message.indexItem) {
      ExprNode.encode(v!, writer.uint32(66).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Index {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseIndex();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.databaseId = reader.uint32();
          break;
        case 4:
          message.name = reader.string();
          break;
        case 5:
          message.owner = reader.uint32();
          break;
        case 6:
          message.indexTableId = reader.uint32();
          break;
        case 7:
          message.primaryTableId = reader.uint32();
          break;
        case 8:
          message.indexItem.push(ExprNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Index {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      indexTableId: isSet(object.indexTableId) ? Number(object.indexTableId) : 0,
      primaryTableId: isSet(object.primaryTableId) ? Number(object.primaryTableId) : 0,
      indexItem: Array.isArray(object?.indexItem) ? object.indexItem.map((e: any) => ExprNode.fromJSON(e)) : [],
    };
  },

  toJSON(message: Index): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    message.indexTableId !== undefined && (obj.indexTableId = Math.round(message.indexTableId));
    message.primaryTableId !== undefined && (obj.primaryTableId = Math.round(message.primaryTableId));
    if (message.indexItem) {
      obj.indexItem = message.indexItem.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.indexItem = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Index>, I>>(object: I): Index {
    const message = createBaseIndex();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    message.indexTableId = object.indexTableId ?? 0;
    message.primaryTableId = object.primaryTableId ?? 0;
    message.indexItem = object.indexItem?.map((e) => ExprNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseTable(): Table {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    columns: [],
    orderKey: [],
    dependentRelations: [],
    optionalAssociatedSourceId: undefined,
    isIndex: false,
    indexOnId: 0,
    distributionKey: [],
    streamKey: [],
    appendonly: false,
    owner: 0,
    properties: {},
    fragmentId: 0,
    vnodeColIdx: undefined,
  };
}

export const Table = {
  encode(message: Table, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.schemaId !== 0) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.databaseId !== 0) {
      writer.uint32(24).uint32(message.databaseId);
    }
    if (message.name !== "") {
      writer.uint32(34).string(message.name);
    }
    for (const v of message.columns) {
      ColumnCatalog.encode(v!, writer.uint32(42).fork()).ldelim();
    }
    for (const v of message.orderKey) {
      ColumnOrder.encode(v!, writer.uint32(50).fork()).ldelim();
    }
    writer.uint32(66).fork();
    for (const v of message.dependentRelations) {
      writer.uint32(v);
    }
    writer.ldelim();
    if (message.optionalAssociatedSourceId?.$case === "associatedSourceId") {
      writer.uint32(72).uint32(message.optionalAssociatedSourceId.associatedSourceId);
    }
    if (message.isIndex === true) {
      writer.uint32(80).bool(message.isIndex);
    }
    if (message.indexOnId !== 0) {
      writer.uint32(88).uint32(message.indexOnId);
    }
    writer.uint32(98).fork();
    for (const v of message.distributionKey) {
      writer.int32(v);
    }
    writer.ldelim();
    writer.uint32(106).fork();
    for (const v of message.streamKey) {
      writer.int32(v);
    }
    writer.ldelim();
    if (message.appendonly === true) {
      writer.uint32(112).bool(message.appendonly);
    }
    if (message.owner !== 0) {
      writer.uint32(120).uint32(message.owner);
    }
    Object.entries(message.properties).forEach(([key, value]) => {
      Table_PropertiesEntry.encode({ key: key as any, value }, writer.uint32(130).fork()).ldelim();
    });
    if (message.fragmentId !== 0) {
      writer.uint32(136).uint32(message.fragmentId);
    }
    if (message.vnodeColIdx !== undefined) {
      ColumnIndex.encode(message.vnodeColIdx, writer.uint32(146).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Table {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTable();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.databaseId = reader.uint32();
          break;
        case 4:
          message.name = reader.string();
          break;
        case 5:
          message.columns.push(ColumnCatalog.decode(reader, reader.uint32()));
          break;
        case 6:
          message.orderKey.push(ColumnOrder.decode(reader, reader.uint32()));
          break;
        case 8:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.dependentRelations.push(reader.uint32());
            }
          } else {
            message.dependentRelations.push(reader.uint32());
          }
          break;
        case 9:
          message.optionalAssociatedSourceId = { $case: "associatedSourceId", associatedSourceId: reader.uint32() };
          break;
        case 10:
          message.isIndex = reader.bool();
          break;
        case 11:
          message.indexOnId = reader.uint32();
          break;
        case 12:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.distributionKey.push(reader.int32());
            }
          } else {
            message.distributionKey.push(reader.int32());
          }
          break;
        case 13:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.streamKey.push(reader.int32());
            }
          } else {
            message.streamKey.push(reader.int32());
          }
          break;
        case 14:
          message.appendonly = reader.bool();
          break;
        case 15:
          message.owner = reader.uint32();
          break;
        case 16:
          const entry16 = Table_PropertiesEntry.decode(reader, reader.uint32());
          if (entry16.value !== undefined) {
            message.properties[entry16.key] = entry16.value;
          }
          break;
        case 17:
          message.fragmentId = reader.uint32();
          break;
        case 18:
          message.vnodeColIdx = ColumnIndex.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Table {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      orderKey: Array.isArray(object?.orderKey) ? object.orderKey.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      optionalAssociatedSourceId: isSet(object.associatedSourceId)
        ? { $case: "associatedSourceId", associatedSourceId: Number(object.associatedSourceId) }
        : undefined,
      isIndex: isSet(object.isIndex) ? Boolean(object.isIndex) : false,
      indexOnId: isSet(object.indexOnId) ? Number(object.indexOnId) : 0,
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      appendonly: isSet(object.appendonly) ? Boolean(object.appendonly) : false,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      vnodeColIdx: isSet(object.vnodeColIdx) ? ColumnIndex.fromJSON(object.vnodeColIdx) : undefined,
    };
  },

  toJSON(message: Table): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.orderKey) {
      obj.orderKey = message.orderKey.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.orderKey = [];
    }
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    message.optionalAssociatedSourceId?.$case === "associatedSourceId" &&
      (obj.associatedSourceId = Math.round(message.optionalAssociatedSourceId?.associatedSourceId));
    message.isIndex !== undefined && (obj.isIndex = message.isIndex);
    message.indexOnId !== undefined && (obj.indexOnId = Math.round(message.indexOnId));
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
    } else {
      obj.streamKey = [];
    }
    message.appendonly !== undefined && (obj.appendonly = message.appendonly);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.vnodeColIdx !== undefined &&
      (obj.vnodeColIdx = message.vnodeColIdx ? ColumnIndex.toJSON(message.vnodeColIdx) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table>, I>>(object: I): Table {
    const message = createBaseTable();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.orderKey = object.orderKey?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    if (
      object.optionalAssociatedSourceId?.$case === "associatedSourceId" &&
      object.optionalAssociatedSourceId?.associatedSourceId !== undefined &&
      object.optionalAssociatedSourceId?.associatedSourceId !== null
    ) {
      message.optionalAssociatedSourceId = {
        $case: "associatedSourceId",
        associatedSourceId: object.optionalAssociatedSourceId.associatedSourceId,
      };
    }
    message.isIndex = object.isIndex ?? false;
    message.indexOnId = object.indexOnId ?? 0;
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.appendonly = object.appendonly ?? false;
    message.owner = object.owner ?? 0;
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.fragmentId = object.fragmentId ?? 0;
    message.vnodeColIdx = (object.vnodeColIdx !== undefined && object.vnodeColIdx !== null)
      ? ColumnIndex.fromPartial(object.vnodeColIdx)
      : undefined;
    return message;
  },
};

function createBaseTable_PropertiesEntry(): Table_PropertiesEntry {
  return { key: "", value: "" };
}

export const Table_PropertiesEntry = {
  encode(message: Table_PropertiesEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== "") {
      writer.uint32(10).string(message.key);
    }
    if (message.value !== "") {
      writer.uint32(18).string(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Table_PropertiesEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTable_PropertiesEntry();
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

  fromJSON(object: any): Table_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Table_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table_PropertiesEntry>, I>>(object: I): Table_PropertiesEntry {
    const message = createBaseTable_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSchema(): Schema {
  return { id: 0, databaseId: 0, name: "", owner: 0 };
}

export const Schema = {
  encode(message: Schema, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.databaseId !== 0) {
      writer.uint32(16).uint32(message.databaseId);
    }
    if (message.name !== "") {
      writer.uint32(26).string(message.name);
    }
    if (message.owner !== 0) {
      writer.uint32(32).uint32(message.owner);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Schema {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSchema();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.databaseId = reader.uint32();
          break;
        case 3:
          message.name = reader.string();
          break;
        case 4:
          message.owner = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Schema {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
    };
  },

  toJSON(message: Schema): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Schema>, I>>(object: I): Schema {
    const message = createBaseSchema();
    message.id = object.id ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    return message;
  },
};

function createBaseDatabase(): Database {
  return { id: 0, name: "", owner: 0 };
}

export const Database = {
  encode(message: Database, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.name !== "") {
      writer.uint32(18).string(message.name);
    }
    if (message.owner !== 0) {
      writer.uint32(24).uint32(message.owner);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Database {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDatabase();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.name = reader.string();
          break;
        case 3:
          message.owner = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Database {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
    };
  },

  toJSON(message: Database): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Database>, I>>(object: I): Database {
    const message = createBaseDatabase();
    message.id = object.id ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
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
