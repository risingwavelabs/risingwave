/* eslint-disable */
import { DataType } from "./data";
import { ExprNode } from "./expr";
import { PbColumnOrder } from "./order";
import { ColumnCatalog, Field, RowFormatType, rowFormatTypeFromJSON, rowFormatTypeToJSON } from "./plan_common";

export const protobufPackage = "catalog";

export const SinkType = {
  UNSPECIFIED: "UNSPECIFIED",
  APPEND_ONLY: "APPEND_ONLY",
  FORCE_APPEND_ONLY: "FORCE_APPEND_ONLY",
  UPSERT: "UPSERT",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type SinkType = typeof SinkType[keyof typeof SinkType];

export function sinkTypeFromJSON(object: any): SinkType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return SinkType.UNSPECIFIED;
    case 1:
    case "APPEND_ONLY":
      return SinkType.APPEND_ONLY;
    case 2:
    case "FORCE_APPEND_ONLY":
      return SinkType.FORCE_APPEND_ONLY;
    case 3:
    case "UPSERT":
      return SinkType.UPSERT;
    case -1:
    case "UNRECOGNIZED":
    default:
      return SinkType.UNRECOGNIZED;
  }
}

export function sinkTypeToJSON(object: SinkType): string {
  switch (object) {
    case SinkType.UNSPECIFIED:
      return "UNSPECIFIED";
    case SinkType.APPEND_ONLY:
      return "APPEND_ONLY";
    case SinkType.FORCE_APPEND_ONLY:
      return "FORCE_APPEND_ONLY";
    case SinkType.UPSERT:
      return "UPSERT";
    case SinkType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const HandleConflictBehavior = {
  NO_CHECK_UNSPECIFIED: "NO_CHECK_UNSPECIFIED",
  OVERWRITE: "OVERWRITE",
  IGNORE: "IGNORE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type HandleConflictBehavior = typeof HandleConflictBehavior[keyof typeof HandleConflictBehavior];

export function handleConflictBehaviorFromJSON(object: any): HandleConflictBehavior {
  switch (object) {
    case 0:
    case "NO_CHECK_UNSPECIFIED":
      return HandleConflictBehavior.NO_CHECK_UNSPECIFIED;
    case 1:
    case "OVERWRITE":
      return HandleConflictBehavior.OVERWRITE;
    case 2:
    case "IGNORE":
      return HandleConflictBehavior.IGNORE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return HandleConflictBehavior.UNRECOGNIZED;
  }
}

export function handleConflictBehaviorToJSON(object: HandleConflictBehavior): string {
  switch (object) {
    case HandleConflictBehavior.NO_CHECK_UNSPECIFIED:
      return "NO_CHECK_UNSPECIFIED";
    case HandleConflictBehavior.OVERWRITE:
      return "OVERWRITE";
    case HandleConflictBehavior.IGNORE:
      return "IGNORE";
    case HandleConflictBehavior.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

/** A mapping of column indices. */
export interface ColIndexMapping {
  /** The size of the target space. */
  targetSize: number;
  /**
   * Each subscript is mapped to the corresponding element.
   * For those not mapped, the value will be negative.
   */
  map: number[];
}

export interface WatermarkDesc {
  /** The column idx the watermark is on */
  watermarkIdx: number;
  /** The expression to calculate the watermark value. */
  expr: ExprNode | undefined;
}

export interface StreamSourceInfo {
  rowFormat: RowFormatType;
  rowSchemaLocation: string;
  useSchemaRegistry: boolean;
  protoMessageName: string;
  csvDelimiter: number;
  csvHasHeader: boolean;
  upsertAvroPrimaryKey: string;
}

export interface Source {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  /**
   * The column index of row ID. If the primary key is specified by the user,
   * this will be `None`.
   */
  rowIdIndex?:
    | number
    | undefined;
  /** Columns of the source. */
  columns: ColumnCatalog[];
  /**
   * Column id of the primary key specified by the user. If the user does not
   * specify a primary key, the vector will be empty.
   */
  pkColumnIds: number[];
  /** Properties specified by the user in WITH clause. */
  properties: { [key: string]: string };
  owner: number;
  info:
    | StreamSourceInfo
    | undefined;
  /**
   * Define watermarks on the source. The `repeated` is just for forward
   * compatibility, currently, only one watermark on the source
   */
  watermarkDescs: WatermarkDesc[];
}

export interface Source_PropertiesEntry {
  key: string;
  value: string;
}

export interface Sink {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  columns: ColumnCatalog[];
  pk: PbColumnOrder[];
  dependentRelations: number[];
  distributionKey: number[];
  /** pk_indices of the corresponding materialize operator's output. */
  streamKey: number[];
  sinkType: SinkType;
  owner: number;
  properties: { [key: string]: string };
  definition: string;
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
  originalColumns: number[];
}

export interface Function {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  owner: number;
  argTypes: DataType[];
  returnType: DataType | undefined;
  language: string;
  link: string;
  identifier: string;
}

/** See `TableCatalog` struct in frontend crate for more information. */
export interface Table {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  columns: ColumnCatalog[];
  pk: PbColumnOrder[];
  dependentRelations: number[];
  optionalAssociatedSourceId?: { $case: "associatedSourceId"; associatedSourceId: number };
  tableType: Table_TableType;
  distributionKey: number[];
  /** pk_indices of the corresponding materialize operator's output. */
  streamKey: number[];
  appendOnly: boolean;
  owner: number;
  properties: { [key: string]: string };
  fragmentId: number;
  /**
   * an optional column index which is the vnode of each row computed by the
   * table's consistent hash distribution
   */
  vnodeColIndex?:
    | number
    | undefined;
  /**
   * An optional column index of row id. If the primary key is specified by users,
   * this will be `None`.
   */
  rowIdIndex?:
    | number
    | undefined;
  /**
   * The column indices which are stored in the state store's value with
   * row-encoding. Currently is not supported yet and expected to be
   * `[0..columns.len()]`.
   */
  valueIndices: number[];
  definition: string;
  handlePkConflictBehavior: HandleConflictBehavior;
  readPrefixLenHint: number;
  watermarkIndices: number[];
  /**
   * Per-table catalog version, used by schema change. `None` for internal tables and tests.
   * Not to be confused with the global catalog version for notification service.
   */
  version: Table_TableVersion | undefined;
}

export const Table_TableType = {
  UNSPECIFIED: "UNSPECIFIED",
  TABLE: "TABLE",
  MATERIALIZED_VIEW: "MATERIALIZED_VIEW",
  INDEX: "INDEX",
  INTERNAL: "INTERNAL",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Table_TableType = typeof Table_TableType[keyof typeof Table_TableType];

export function table_TableTypeFromJSON(object: any): Table_TableType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return Table_TableType.UNSPECIFIED;
    case 1:
    case "TABLE":
      return Table_TableType.TABLE;
    case 2:
    case "MATERIALIZED_VIEW":
      return Table_TableType.MATERIALIZED_VIEW;
    case 3:
    case "INDEX":
      return Table_TableType.INDEX;
    case 4:
    case "INTERNAL":
      return Table_TableType.INTERNAL;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Table_TableType.UNRECOGNIZED;
  }
}

export function table_TableTypeToJSON(object: Table_TableType): string {
  switch (object) {
    case Table_TableType.UNSPECIFIED:
      return "UNSPECIFIED";
    case Table_TableType.TABLE:
      return "TABLE";
    case Table_TableType.MATERIALIZED_VIEW:
      return "MATERIALIZED_VIEW";
    case Table_TableType.INDEX:
      return "INDEX";
    case Table_TableType.INTERNAL:
      return "INTERNAL";
    case Table_TableType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface Table_TableVersion {
  /**
   * The version number, which will be 0 by default and be increased by 1 for
   * each schema change in the frontend.
   */
  version: number;
  /**
   * The ID of the next column to be added, which is used to make all columns
   * in the table have unique IDs, even if some columns have been dropped.
   */
  nextColumnId: number;
}

export interface Table_PropertiesEntry {
  key: string;
  value: string;
}

export interface View {
  id: number;
  schemaId: number;
  databaseId: number;
  name: string;
  owner: number;
  properties: { [key: string]: string };
  sql: string;
  dependentRelations: number[];
  /** User-specified column names. */
  columns: Field[];
}

export interface View_PropertiesEntry {
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

function createBaseColIndexMapping(): ColIndexMapping {
  return { targetSize: 0, map: [] };
}

export const ColIndexMapping = {
  fromJSON(object: any): ColIndexMapping {
    return {
      targetSize: isSet(object.targetSize) ? Number(object.targetSize) : 0,
      map: Array.isArray(object?.map) ? object.map.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ColIndexMapping): unknown {
    const obj: any = {};
    message.targetSize !== undefined && (obj.targetSize = Math.round(message.targetSize));
    if (message.map) {
      obj.map = message.map.map((e) => Math.round(e));
    } else {
      obj.map = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColIndexMapping>, I>>(object: I): ColIndexMapping {
    const message = createBaseColIndexMapping();
    message.targetSize = object.targetSize ?? 0;
    message.map = object.map?.map((e) => e) || [];
    return message;
  },
};

function createBaseWatermarkDesc(): WatermarkDesc {
  return { watermarkIdx: 0, expr: undefined };
}

export const WatermarkDesc = {
  fromJSON(object: any): WatermarkDesc {
    return {
      watermarkIdx: isSet(object.watermarkIdx) ? Number(object.watermarkIdx) : 0,
      expr: isSet(object.expr) ? ExprNode.fromJSON(object.expr) : undefined,
    };
  },

  toJSON(message: WatermarkDesc): unknown {
    const obj: any = {};
    message.watermarkIdx !== undefined && (obj.watermarkIdx = Math.round(message.watermarkIdx));
    message.expr !== undefined && (obj.expr = message.expr ? ExprNode.toJSON(message.expr) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<WatermarkDesc>, I>>(object: I): WatermarkDesc {
    const message = createBaseWatermarkDesc();
    message.watermarkIdx = object.watermarkIdx ?? 0;
    message.expr = (object.expr !== undefined && object.expr !== null) ? ExprNode.fromPartial(object.expr) : undefined;
    return message;
  },
};

function createBaseStreamSourceInfo(): StreamSourceInfo {
  return {
    rowFormat: RowFormatType.ROW_UNSPECIFIED,
    rowSchemaLocation: "",
    useSchemaRegistry: false,
    protoMessageName: "",
    csvDelimiter: 0,
    csvHasHeader: false,
    upsertAvroPrimaryKey: "",
  };
}

export const StreamSourceInfo = {
  fromJSON(object: any): StreamSourceInfo {
    return {
      rowFormat: isSet(object.rowFormat) ? rowFormatTypeFromJSON(object.rowFormat) : RowFormatType.ROW_UNSPECIFIED,
      rowSchemaLocation: isSet(object.rowSchemaLocation) ? String(object.rowSchemaLocation) : "",
      useSchemaRegistry: isSet(object.useSchemaRegistry) ? Boolean(object.useSchemaRegistry) : false,
      protoMessageName: isSet(object.protoMessageName) ? String(object.protoMessageName) : "",
      csvDelimiter: isSet(object.csvDelimiter) ? Number(object.csvDelimiter) : 0,
      csvHasHeader: isSet(object.csvHasHeader) ? Boolean(object.csvHasHeader) : false,
      upsertAvroPrimaryKey: isSet(object.upsertAvroPrimaryKey) ? String(object.upsertAvroPrimaryKey) : "",
    };
  },

  toJSON(message: StreamSourceInfo): unknown {
    const obj: any = {};
    message.rowFormat !== undefined && (obj.rowFormat = rowFormatTypeToJSON(message.rowFormat));
    message.rowSchemaLocation !== undefined && (obj.rowSchemaLocation = message.rowSchemaLocation);
    message.useSchemaRegistry !== undefined && (obj.useSchemaRegistry = message.useSchemaRegistry);
    message.protoMessageName !== undefined && (obj.protoMessageName = message.protoMessageName);
    message.csvDelimiter !== undefined && (obj.csvDelimiter = Math.round(message.csvDelimiter));
    message.csvHasHeader !== undefined && (obj.csvHasHeader = message.csvHasHeader);
    message.upsertAvroPrimaryKey !== undefined && (obj.upsertAvroPrimaryKey = message.upsertAvroPrimaryKey);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSourceInfo>, I>>(object: I): StreamSourceInfo {
    const message = createBaseStreamSourceInfo();
    message.rowFormat = object.rowFormat ?? RowFormatType.ROW_UNSPECIFIED;
    message.rowSchemaLocation = object.rowSchemaLocation ?? "";
    message.useSchemaRegistry = object.useSchemaRegistry ?? false;
    message.protoMessageName = object.protoMessageName ?? "";
    message.csvDelimiter = object.csvDelimiter ?? 0;
    message.csvHasHeader = object.csvHasHeader ?? false;
    message.upsertAvroPrimaryKey = object.upsertAvroPrimaryKey ?? "";
    return message;
  },
};

function createBaseSource(): Source {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    rowIdIndex: undefined,
    columns: [],
    pkColumnIds: [],
    properties: {},
    owner: 0,
    info: undefined,
    watermarkDescs: [],
  };
}

export const Source = {
  fromJSON(object: any): Source {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      rowIdIndex: isSet(object.rowIdIndex) ? Number(object.rowIdIndex) : undefined,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pkColumnIds: Array.isArray(object?.pkColumnIds) ? object.pkColumnIds.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      info: isSet(object.info) ? StreamSourceInfo.fromJSON(object.info) : undefined,
      watermarkDescs: Array.isArray(object?.watermarkDescs)
        ? object.watermarkDescs.map((e: any) => WatermarkDesc.fromJSON(e))
        : [],
    };
  },

  toJSON(message: Source): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.rowIdIndex !== undefined && (obj.rowIdIndex = Math.round(message.rowIdIndex));
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
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    message.info !== undefined && (obj.info = message.info ? StreamSourceInfo.toJSON(message.info) : undefined);
    if (message.watermarkDescs) {
      obj.watermarkDescs = message.watermarkDescs.map((e) => e ? WatermarkDesc.toJSON(e) : undefined);
    } else {
      obj.watermarkDescs = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Source>, I>>(object: I): Source {
    const message = createBaseSource();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.rowIdIndex = object.rowIdIndex ?? undefined;
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
    message.owner = object.owner ?? 0;
    message.info = (object.info !== undefined && object.info !== null)
      ? StreamSourceInfo.fromPartial(object.info)
      : undefined;
    message.watermarkDescs = object.watermarkDescs?.map((e) => WatermarkDesc.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSource_PropertiesEntry(): Source_PropertiesEntry {
  return { key: "", value: "" };
}

export const Source_PropertiesEntry = {
  fromJSON(object: any): Source_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: Source_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Source_PropertiesEntry>, I>>(object: I): Source_PropertiesEntry {
    const message = createBaseSource_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSink(): Sink {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    columns: [],
    pk: [],
    dependentRelations: [],
    distributionKey: [],
    streamKey: [],
    sinkType: SinkType.UNSPECIFIED,
    owner: 0,
    properties: {},
    definition: "",
  };
}

export const Sink = {
  fromJSON(object: any): Sink {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => PbColumnOrder.fromJSON(e)) : [],
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      distributionKey: Array.isArray(object?.distributionKey)
        ? object.distributionKey.map((e: any) => Number(e))
        : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      sinkType: isSet(object.sinkType) ? sinkTypeFromJSON(object.sinkType) : SinkType.UNSPECIFIED,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      definition: isSet(object.definition) ? String(object.definition) : "",
    };
  },

  toJSON(message: Sink): unknown {
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
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? PbColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
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
    message.sinkType !== undefined && (obj.sinkType = sinkTypeToJSON(message.sinkType));
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.definition !== undefined && (obj.definition = message.definition);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Sink>, I>>(object: I): Sink {
    const message = createBaseSink();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => PbColumnOrder.fromPartial(e)) || [];
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.sinkType = object.sinkType ?? SinkType.UNSPECIFIED;
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
    message.definition = object.definition ?? "";
    return message;
  },
};

function createBaseSink_PropertiesEntry(): Sink_PropertiesEntry {
  return { key: "", value: "" };
}

export const Sink_PropertiesEntry = {
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
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    owner: 0,
    indexTableId: 0,
    primaryTableId: 0,
    indexItem: [],
    originalColumns: [],
  };
}

export const Index = {
  fromJSON(object: any): Index {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      indexTableId: isSet(object.indexTableId) ? Number(object.indexTableId) : 0,
      primaryTableId: isSet(object.primaryTableId) ? Number(object.primaryTableId) : 0,
      indexItem: Array.isArray(object?.indexItem)
        ? object.indexItem.map((e: any) => ExprNode.fromJSON(e))
        : [],
      originalColumns: Array.isArray(object?.originalColumns) ? object.originalColumns.map((e: any) => Number(e)) : [],
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
    if (message.originalColumns) {
      obj.originalColumns = message.originalColumns.map((e) => Math.round(e));
    } else {
      obj.originalColumns = [];
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
    message.originalColumns = object.originalColumns?.map((e) => e) || [];
    return message;
  },
};

function createBaseFunction(): Function {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    owner: 0,
    argTypes: [],
    returnType: undefined,
    language: "",
    link: "",
    identifier: "",
  };
}

export const Function = {
  fromJSON(object: any): Function {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      argTypes: Array.isArray(object?.argTypes)
        ? object.argTypes.map((e: any) => DataType.fromJSON(e))
        : [],
      returnType: isSet(object.returnType) ? DataType.fromJSON(object.returnType) : undefined,
      language: isSet(object.language) ? String(object.language) : "",
      link: isSet(object.link) ? String(object.link) : "",
      identifier: isSet(object.identifier) ? String(object.identifier) : "",
    };
  },

  toJSON(message: Function): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    if (message.argTypes) {
      obj.argTypes = message.argTypes.map((e) => e ? DataType.toJSON(e) : undefined);
    } else {
      obj.argTypes = [];
    }
    message.returnType !== undefined &&
      (obj.returnType = message.returnType ? DataType.toJSON(message.returnType) : undefined);
    message.language !== undefined && (obj.language = message.language);
    message.link !== undefined && (obj.link = message.link);
    message.identifier !== undefined && (obj.identifier = message.identifier);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Function>, I>>(object: I): Function {
    const message = createBaseFunction();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.owner = object.owner ?? 0;
    message.argTypes = object.argTypes?.map((e) => DataType.fromPartial(e)) || [];
    message.returnType = (object.returnType !== undefined && object.returnType !== null)
      ? DataType.fromPartial(object.returnType)
      : undefined;
    message.language = object.language ?? "";
    message.link = object.link ?? "";
    message.identifier = object.identifier ?? "";
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
    pk: [],
    dependentRelations: [],
    optionalAssociatedSourceId: undefined,
    tableType: Table_TableType.UNSPECIFIED,
    distributionKey: [],
    streamKey: [],
    appendOnly: false,
    owner: 0,
    properties: {},
    fragmentId: 0,
    vnodeColIndex: undefined,
    rowIdIndex: undefined,
    valueIndices: [],
    definition: "",
    handlePkConflictBehavior: HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
    readPrefixLenHint: 0,
    watermarkIndices: [],
    version: undefined,
  };
}

export const Table = {
  fromJSON(object: any): Table {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => PbColumnOrder.fromJSON(e)) : [],
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      optionalAssociatedSourceId: isSet(object.associatedSourceId)
        ? { $case: "associatedSourceId", associatedSourceId: Number(object.associatedSourceId) }
        : undefined,
      tableType: isSet(object.tableType) ? table_TableTypeFromJSON(object.tableType) : Table_TableType.UNSPECIFIED,
      distributionKey: Array.isArray(object?.distributionKey)
        ? object.distributionKey.map((e: any) => Number(e))
        : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      appendOnly: isSet(object.appendOnly) ? Boolean(object.appendOnly) : false,
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      vnodeColIndex: isSet(object.vnodeColIndex) ? Number(object.vnodeColIndex) : undefined,
      rowIdIndex: isSet(object.rowIdIndex) ? Number(object.rowIdIndex) : undefined,
      valueIndices: Array.isArray(object?.valueIndices)
        ? object.valueIndices.map((e: any) => Number(e))
        : [],
      definition: isSet(object.definition) ? String(object.definition) : "",
      handlePkConflictBehavior: isSet(object.handlePkConflictBehavior)
        ? handleConflictBehaviorFromJSON(object.handlePkConflictBehavior)
        : HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
      readPrefixLenHint: isSet(object.readPrefixLenHint) ? Number(object.readPrefixLenHint) : 0,
      watermarkIndices: Array.isArray(object?.watermarkIndices)
        ? object.watermarkIndices.map((e: any) => Number(e))
        : [],
      version: isSet(object.version) ? Table_TableVersion.fromJSON(object.version) : undefined,
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
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? PbColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    message.optionalAssociatedSourceId?.$case === "associatedSourceId" &&
      (obj.associatedSourceId = Math.round(message.optionalAssociatedSourceId?.associatedSourceId));
    message.tableType !== undefined && (obj.tableType = table_TableTypeToJSON(message.tableType));
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
    message.appendOnly !== undefined && (obj.appendOnly = message.appendOnly);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.vnodeColIndex !== undefined && (obj.vnodeColIndex = Math.round(message.vnodeColIndex));
    message.rowIdIndex !== undefined && (obj.rowIdIndex = Math.round(message.rowIdIndex));
    if (message.valueIndices) {
      obj.valueIndices = message.valueIndices.map((e) => Math.round(e));
    } else {
      obj.valueIndices = [];
    }
    message.definition !== undefined && (obj.definition = message.definition);
    message.handlePkConflictBehavior !== undefined &&
      (obj.handlePkConflictBehavior = handleConflictBehaviorToJSON(message.handlePkConflictBehavior));
    message.readPrefixLenHint !== undefined && (obj.readPrefixLenHint = Math.round(message.readPrefixLenHint));
    if (message.watermarkIndices) {
      obj.watermarkIndices = message.watermarkIndices.map((e) => Math.round(e));
    } else {
      obj.watermarkIndices = [];
    }
    message.version !== undefined &&
      (obj.version = message.version ? Table_TableVersion.toJSON(message.version) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table>, I>>(object: I): Table {
    const message = createBaseTable();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => PbColumnOrder.fromPartial(e)) || [];
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
    message.tableType = object.tableType ?? Table_TableType.UNSPECIFIED;
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.appendOnly = object.appendOnly ?? false;
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
    message.vnodeColIndex = object.vnodeColIndex ?? undefined;
    message.rowIdIndex = object.rowIdIndex ?? undefined;
    message.valueIndices = object.valueIndices?.map((e) => e) || [];
    message.definition = object.definition ?? "";
    message.handlePkConflictBehavior = object.handlePkConflictBehavior ?? HandleConflictBehavior.NO_CHECK_UNSPECIFIED;
    message.readPrefixLenHint = object.readPrefixLenHint ?? 0;
    message.watermarkIndices = object.watermarkIndices?.map((e) => e) || [];
    message.version = (object.version !== undefined && object.version !== null)
      ? Table_TableVersion.fromPartial(object.version)
      : undefined;
    return message;
  },
};

function createBaseTable_TableVersion(): Table_TableVersion {
  return { version: 0, nextColumnId: 0 };
}

export const Table_TableVersion = {
  fromJSON(object: any): Table_TableVersion {
    return {
      version: isSet(object.version) ? Number(object.version) : 0,
      nextColumnId: isSet(object.nextColumnId) ? Number(object.nextColumnId) : 0,
    };
  },

  toJSON(message: Table_TableVersion): unknown {
    const obj: any = {};
    message.version !== undefined && (obj.version = Math.round(message.version));
    message.nextColumnId !== undefined && (obj.nextColumnId = Math.round(message.nextColumnId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Table_TableVersion>, I>>(object: I): Table_TableVersion {
    const message = createBaseTable_TableVersion();
    message.version = object.version ?? 0;
    message.nextColumnId = object.nextColumnId ?? 0;
    return message;
  },
};

function createBaseTable_PropertiesEntry(): Table_PropertiesEntry {
  return { key: "", value: "" };
}

export const Table_PropertiesEntry = {
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

function createBaseView(): View {
  return {
    id: 0,
    schemaId: 0,
    databaseId: 0,
    name: "",
    owner: 0,
    properties: {},
    sql: "",
    dependentRelations: [],
    columns: [],
  };
}

export const View = {
  fromJSON(object: any): View {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      owner: isSet(object.owner) ? Number(object.owner) : 0,
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      sql: isSet(object.sql) ? String(object.sql) : "",
      dependentRelations: Array.isArray(object?.dependentRelations)
        ? object.dependentRelations.map((e: any) => Number(e))
        : [],
      columns: Array.isArray(object?.columns)
        ? object.columns.map((e: any) => Field.fromJSON(e))
        : [],
    };
  },

  toJSON(message: View): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.name !== undefined && (obj.name = message.name);
    message.owner !== undefined && (obj.owner = Math.round(message.owner));
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.sql !== undefined && (obj.sql = message.sql);
    if (message.dependentRelations) {
      obj.dependentRelations = message.dependentRelations.map((e) => Math.round(e));
    } else {
      obj.dependentRelations = [];
    }
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<View>, I>>(object: I): View {
    const message = createBaseView();
    message.id = object.id ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.databaseId = object.databaseId ?? 0;
    message.name = object.name ?? "";
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
    message.sql = object.sql ?? "";
    message.dependentRelations = object.dependentRelations?.map((e) => e) || [];
    message.columns = object.columns?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseView_PropertiesEntry(): View_PropertiesEntry {
  return { key: "", value: "" };
}

export const View_PropertiesEntry = {
  fromJSON(object: any): View_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: View_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<View_PropertiesEntry>, I>>(object: I): View_PropertiesEntry {
    const message = createBaseView_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSchema(): Schema {
  return { id: 0, databaseId: 0, name: "", owner: 0 };
}

export const Schema = {
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

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

export type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends { $case: string } ? { [K in keyof Omit<T, "$case">]?: DeepPartial<T[K]> } & { $case: T["$case"] }
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
export type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
