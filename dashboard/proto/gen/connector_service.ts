/* eslint-disable */
import {
  DataType_TypeName,
  dataType_TypeNameFromJSON,
  dataType_TypeNameToJSON,
  Op,
  opFromJSON,
  opToJSON,
} from "./data";

export const protobufPackage = "connector_service";

export interface SinkConfig {
  sinkType: string;
  properties: { [key: string]: string };
  tableSchema: SinkConfig_TableSchema | undefined;
}

export interface SinkConfig_TableSchema {
  columns: SinkConfig_TableSchema_Column[];
  pkIndices: number[];
}

export interface SinkConfig_TableSchema_Column {
  name: string;
  dataType: DataType_TypeName;
}

export interface SinkConfig_PropertiesEntry {
  key: string;
  value: string;
}

export interface SinkStreamRequest {
  request?:
    | { $case: "start"; start: SinkStreamRequest_StartSink }
    | { $case: "startEpoch"; startEpoch: SinkStreamRequest_StartEpoch }
    | { $case: "write"; write: SinkStreamRequest_WriteBatch }
    | { $case: "sync"; sync: SinkStreamRequest_SyncBatch };
}

export interface SinkStreamRequest_StartSink {
  sinkConfig: SinkConfig | undefined;
}

export interface SinkStreamRequest_WriteBatch {
  payload?: { $case: "jsonPayload"; jsonPayload: SinkStreamRequest_WriteBatch_JsonPayload };
  batchId: number;
  epoch: number;
}

export interface SinkStreamRequest_WriteBatch_JsonPayload {
  rowOps: SinkStreamRequest_WriteBatch_JsonPayload_RowOp[];
}

export interface SinkStreamRequest_WriteBatch_JsonPayload_RowOp {
  opType: Op;
  line: string;
}

export interface SinkStreamRequest_StartEpoch {
  epoch: number;
}

export interface SinkStreamRequest_SyncBatch {
  epoch: number;
}

export interface SinkResponse {
  response?:
    | { $case: "sync"; sync: SinkResponse_SyncResponse }
    | { $case: "startEpoch"; startEpoch: SinkResponse_StartEpochResponse }
    | { $case: "write"; write: SinkResponse_WriteResponse }
    | { $case: "start"; start: SinkResponse_StartResponse };
}

export interface SinkResponse_SyncResponse {
  epoch: number;
}

export interface SinkResponse_StartEpochResponse {
  epoch: number;
}

export interface SinkResponse_WriteResponse {
  epoch: number;
  batchId: number;
}

export interface SinkResponse_StartResponse {
}

export interface DbConnectorProperties {
  databaseHost: string;
  databasePort: string;
  databaseUser: string;
  databasePassword: string;
  databaseName: string;
  tableName: string;
  partition: string;
  startOffset: string;
  includeSchemaEvents: boolean;
}

export interface CdcMessage {
  payload: string;
  partition: string;
  offset: string;
}

export interface GetEventStreamRequest {
  sourceId: number;
  properties: DbConnectorProperties | undefined;
}

export interface GetEventStreamResponse {
  sourceId: number;
  events: CdcMessage[];
}

function createBaseSinkConfig(): SinkConfig {
  return { sinkType: "", properties: {}, tableSchema: undefined };
}

export const SinkConfig = {
  fromJSON(object: any): SinkConfig {
    return {
      sinkType: isSet(object.sinkType) ? String(object.sinkType) : "",
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      tableSchema: isSet(object.tableSchema) ? SinkConfig_TableSchema.fromJSON(object.tableSchema) : undefined,
    };
  },

  toJSON(message: SinkConfig): unknown {
    const obj: any = {};
    message.sinkType !== undefined && (obj.sinkType = message.sinkType);
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.tableSchema !== undefined &&
      (obj.tableSchema = message.tableSchema ? SinkConfig_TableSchema.toJSON(message.tableSchema) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkConfig>, I>>(object: I): SinkConfig {
    const message = createBaseSinkConfig();
    message.sinkType = object.sinkType ?? "";
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.tableSchema = (object.tableSchema !== undefined && object.tableSchema !== null)
      ? SinkConfig_TableSchema.fromPartial(object.tableSchema)
      : undefined;
    return message;
  },
};

function createBaseSinkConfig_TableSchema(): SinkConfig_TableSchema {
  return { columns: [], pkIndices: [] };
}

export const SinkConfig_TableSchema = {
  fromJSON(object: any): SinkConfig_TableSchema {
    return {
      columns: Array.isArray(object?.columns)
        ? object.columns.map((e: any) => SinkConfig_TableSchema_Column.fromJSON(e))
        : [],
      pkIndices: Array.isArray(object?.pkIndices)
        ? object.pkIndices.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: SinkConfig_TableSchema): unknown {
    const obj: any = {};
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? SinkConfig_TableSchema_Column.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pkIndices) {
      obj.pkIndices = message.pkIndices.map((e) => Math.round(e));
    } else {
      obj.pkIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkConfig_TableSchema>, I>>(object: I): SinkConfig_TableSchema {
    const message = createBaseSinkConfig_TableSchema();
    message.columns = object.columns?.map((e) => SinkConfig_TableSchema_Column.fromPartial(e)) || [];
    message.pkIndices = object.pkIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseSinkConfig_TableSchema_Column(): SinkConfig_TableSchema_Column {
  return { name: "", dataType: DataType_TypeName.TYPE_UNSPECIFIED };
}

export const SinkConfig_TableSchema_Column = {
  fromJSON(object: any): SinkConfig_TableSchema_Column {
    return {
      name: isSet(object.name) ? String(object.name) : "",
      dataType: isSet(object.dataType)
        ? dataType_TypeNameFromJSON(object.dataType)
        : DataType_TypeName.TYPE_UNSPECIFIED,
    };
  },

  toJSON(message: SinkConfig_TableSchema_Column): unknown {
    const obj: any = {};
    message.name !== undefined && (obj.name = message.name);
    message.dataType !== undefined && (obj.dataType = dataType_TypeNameToJSON(message.dataType));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkConfig_TableSchema_Column>, I>>(
    object: I,
  ): SinkConfig_TableSchema_Column {
    const message = createBaseSinkConfig_TableSchema_Column();
    message.name = object.name ?? "";
    message.dataType = object.dataType ?? DataType_TypeName.TYPE_UNSPECIFIED;
    return message;
  },
};

function createBaseSinkConfig_PropertiesEntry(): SinkConfig_PropertiesEntry {
  return { key: "", value: "" };
}

export const SinkConfig_PropertiesEntry = {
  fromJSON(object: any): SinkConfig_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: SinkConfig_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkConfig_PropertiesEntry>, I>>(object: I): SinkConfig_PropertiesEntry {
    const message = createBaseSinkConfig_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSinkStreamRequest(): SinkStreamRequest {
  return { request: undefined };
}

export const SinkStreamRequest = {
  fromJSON(object: any): SinkStreamRequest {
    return {
      request: isSet(object.start)
        ? { $case: "start", start: SinkStreamRequest_StartSink.fromJSON(object.start) }
        : isSet(object.startEpoch)
        ? { $case: "startEpoch", startEpoch: SinkStreamRequest_StartEpoch.fromJSON(object.startEpoch) }
        : isSet(object.write)
        ? { $case: "write", write: SinkStreamRequest_WriteBatch.fromJSON(object.write) }
        : isSet(object.sync)
        ? { $case: "sync", sync: SinkStreamRequest_SyncBatch.fromJSON(object.sync) }
        : undefined,
    };
  },

  toJSON(message: SinkStreamRequest): unknown {
    const obj: any = {};
    message.request?.$case === "start" &&
      (obj.start = message.request?.start ? SinkStreamRequest_StartSink.toJSON(message.request?.start) : undefined);
    message.request?.$case === "startEpoch" && (obj.startEpoch = message.request?.startEpoch
      ? SinkStreamRequest_StartEpoch.toJSON(message.request?.startEpoch)
      : undefined);
    message.request?.$case === "write" &&
      (obj.write = message.request?.write ? SinkStreamRequest_WriteBatch.toJSON(message.request?.write) : undefined);
    message.request?.$case === "sync" &&
      (obj.sync = message.request?.sync ? SinkStreamRequest_SyncBatch.toJSON(message.request?.sync) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest>, I>>(object: I): SinkStreamRequest {
    const message = createBaseSinkStreamRequest();
    if (object.request?.$case === "start" && object.request?.start !== undefined && object.request?.start !== null) {
      message.request = { $case: "start", start: SinkStreamRequest_StartSink.fromPartial(object.request.start) };
    }
    if (
      object.request?.$case === "startEpoch" &&
      object.request?.startEpoch !== undefined &&
      object.request?.startEpoch !== null
    ) {
      message.request = {
        $case: "startEpoch",
        startEpoch: SinkStreamRequest_StartEpoch.fromPartial(object.request.startEpoch),
      };
    }
    if (object.request?.$case === "write" && object.request?.write !== undefined && object.request?.write !== null) {
      message.request = { $case: "write", write: SinkStreamRequest_WriteBatch.fromPartial(object.request.write) };
    }
    if (object.request?.$case === "sync" && object.request?.sync !== undefined && object.request?.sync !== null) {
      message.request = { $case: "sync", sync: SinkStreamRequest_SyncBatch.fromPartial(object.request.sync) };
    }
    return message;
  },
};

function createBaseSinkStreamRequest_StartSink(): SinkStreamRequest_StartSink {
  return { sinkConfig: undefined };
}

export const SinkStreamRequest_StartSink = {
  fromJSON(object: any): SinkStreamRequest_StartSink {
    return { sinkConfig: isSet(object.sinkConfig) ? SinkConfig.fromJSON(object.sinkConfig) : undefined };
  },

  toJSON(message: SinkStreamRequest_StartSink): unknown {
    const obj: any = {};
    message.sinkConfig !== undefined &&
      (obj.sinkConfig = message.sinkConfig ? SinkConfig.toJSON(message.sinkConfig) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_StartSink>, I>>(object: I): SinkStreamRequest_StartSink {
    const message = createBaseSinkStreamRequest_StartSink();
    message.sinkConfig = (object.sinkConfig !== undefined && object.sinkConfig !== null)
      ? SinkConfig.fromPartial(object.sinkConfig)
      : undefined;
    return message;
  },
};

function createBaseSinkStreamRequest_WriteBatch(): SinkStreamRequest_WriteBatch {
  return { payload: undefined, batchId: 0, epoch: 0 };
}

export const SinkStreamRequest_WriteBatch = {
  fromJSON(object: any): SinkStreamRequest_WriteBatch {
    return {
      payload: isSet(object.jsonPayload)
        ? { $case: "jsonPayload", jsonPayload: SinkStreamRequest_WriteBatch_JsonPayload.fromJSON(object.jsonPayload) }
        : undefined,
      batchId: isSet(object.batchId) ? Number(object.batchId) : 0,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: SinkStreamRequest_WriteBatch): unknown {
    const obj: any = {};
    message.payload?.$case === "jsonPayload" && (obj.jsonPayload = message.payload?.jsonPayload
      ? SinkStreamRequest_WriteBatch_JsonPayload.toJSON(message.payload?.jsonPayload)
      : undefined);
    message.batchId !== undefined && (obj.batchId = Math.round(message.batchId));
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_WriteBatch>, I>>(object: I): SinkStreamRequest_WriteBatch {
    const message = createBaseSinkStreamRequest_WriteBatch();
    if (
      object.payload?.$case === "jsonPayload" &&
      object.payload?.jsonPayload !== undefined &&
      object.payload?.jsonPayload !== null
    ) {
      message.payload = {
        $case: "jsonPayload",
        jsonPayload: SinkStreamRequest_WriteBatch_JsonPayload.fromPartial(object.payload.jsonPayload),
      };
    }
    message.batchId = object.batchId ?? 0;
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseSinkStreamRequest_WriteBatch_JsonPayload(): SinkStreamRequest_WriteBatch_JsonPayload {
  return { rowOps: [] };
}

export const SinkStreamRequest_WriteBatch_JsonPayload = {
  fromJSON(object: any): SinkStreamRequest_WriteBatch_JsonPayload {
    return {
      rowOps: Array.isArray(object?.rowOps)
        ? object.rowOps.map((e: any) => SinkStreamRequest_WriteBatch_JsonPayload_RowOp.fromJSON(e))
        : [],
    };
  },

  toJSON(message: SinkStreamRequest_WriteBatch_JsonPayload): unknown {
    const obj: any = {};
    if (message.rowOps) {
      obj.rowOps = message.rowOps.map((e) => e ? SinkStreamRequest_WriteBatch_JsonPayload_RowOp.toJSON(e) : undefined);
    } else {
      obj.rowOps = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_WriteBatch_JsonPayload>, I>>(
    object: I,
  ): SinkStreamRequest_WriteBatch_JsonPayload {
    const message = createBaseSinkStreamRequest_WriteBatch_JsonPayload();
    message.rowOps = object.rowOps?.map((e) => SinkStreamRequest_WriteBatch_JsonPayload_RowOp.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSinkStreamRequest_WriteBatch_JsonPayload_RowOp(): SinkStreamRequest_WriteBatch_JsonPayload_RowOp {
  return { opType: Op.OP_UNSPECIFIED, line: "" };
}

export const SinkStreamRequest_WriteBatch_JsonPayload_RowOp = {
  fromJSON(object: any): SinkStreamRequest_WriteBatch_JsonPayload_RowOp {
    return {
      opType: isSet(object.opType) ? opFromJSON(object.opType) : Op.OP_UNSPECIFIED,
      line: isSet(object.line) ? String(object.line) : "",
    };
  },

  toJSON(message: SinkStreamRequest_WriteBatch_JsonPayload_RowOp): unknown {
    const obj: any = {};
    message.opType !== undefined && (obj.opType = opToJSON(message.opType));
    message.line !== undefined && (obj.line = message.line);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_WriteBatch_JsonPayload_RowOp>, I>>(
    object: I,
  ): SinkStreamRequest_WriteBatch_JsonPayload_RowOp {
    const message = createBaseSinkStreamRequest_WriteBatch_JsonPayload_RowOp();
    message.opType = object.opType ?? Op.OP_UNSPECIFIED;
    message.line = object.line ?? "";
    return message;
  },
};

function createBaseSinkStreamRequest_StartEpoch(): SinkStreamRequest_StartEpoch {
  return { epoch: 0 };
}

export const SinkStreamRequest_StartEpoch = {
  fromJSON(object: any): SinkStreamRequest_StartEpoch {
    return { epoch: isSet(object.epoch) ? Number(object.epoch) : 0 };
  },

  toJSON(message: SinkStreamRequest_StartEpoch): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_StartEpoch>, I>>(object: I): SinkStreamRequest_StartEpoch {
    const message = createBaseSinkStreamRequest_StartEpoch();
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseSinkStreamRequest_SyncBatch(): SinkStreamRequest_SyncBatch {
  return { epoch: 0 };
}

export const SinkStreamRequest_SyncBatch = {
  fromJSON(object: any): SinkStreamRequest_SyncBatch {
    return { epoch: isSet(object.epoch) ? Number(object.epoch) : 0 };
  },

  toJSON(message: SinkStreamRequest_SyncBatch): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkStreamRequest_SyncBatch>, I>>(object: I): SinkStreamRequest_SyncBatch {
    const message = createBaseSinkStreamRequest_SyncBatch();
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseSinkResponse(): SinkResponse {
  return { response: undefined };
}

export const SinkResponse = {
  fromJSON(object: any): SinkResponse {
    return {
      response: isSet(object.sync)
        ? { $case: "sync", sync: SinkResponse_SyncResponse.fromJSON(object.sync) }
        : isSet(object.startEpoch)
        ? { $case: "startEpoch", startEpoch: SinkResponse_StartEpochResponse.fromJSON(object.startEpoch) }
        : isSet(object.write)
        ? { $case: "write", write: SinkResponse_WriteResponse.fromJSON(object.write) }
        : isSet(object.start)
        ? { $case: "start", start: SinkResponse_StartResponse.fromJSON(object.start) }
        : undefined,
    };
  },

  toJSON(message: SinkResponse): unknown {
    const obj: any = {};
    message.response?.$case === "sync" &&
      (obj.sync = message.response?.sync ? SinkResponse_SyncResponse.toJSON(message.response?.sync) : undefined);
    message.response?.$case === "startEpoch" && (obj.startEpoch = message.response?.startEpoch
      ? SinkResponse_StartEpochResponse.toJSON(message.response?.startEpoch)
      : undefined);
    message.response?.$case === "write" &&
      (obj.write = message.response?.write ? SinkResponse_WriteResponse.toJSON(message.response?.write) : undefined);
    message.response?.$case === "start" &&
      (obj.start = message.response?.start ? SinkResponse_StartResponse.toJSON(message.response?.start) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkResponse>, I>>(object: I): SinkResponse {
    const message = createBaseSinkResponse();
    if (object.response?.$case === "sync" && object.response?.sync !== undefined && object.response?.sync !== null) {
      message.response = { $case: "sync", sync: SinkResponse_SyncResponse.fromPartial(object.response.sync) };
    }
    if (
      object.response?.$case === "startEpoch" &&
      object.response?.startEpoch !== undefined &&
      object.response?.startEpoch !== null
    ) {
      message.response = {
        $case: "startEpoch",
        startEpoch: SinkResponse_StartEpochResponse.fromPartial(object.response.startEpoch),
      };
    }
    if (object.response?.$case === "write" && object.response?.write !== undefined && object.response?.write !== null) {
      message.response = { $case: "write", write: SinkResponse_WriteResponse.fromPartial(object.response.write) };
    }
    if (object.response?.$case === "start" && object.response?.start !== undefined && object.response?.start !== null) {
      message.response = { $case: "start", start: SinkResponse_StartResponse.fromPartial(object.response.start) };
    }
    return message;
  },
};

function createBaseSinkResponse_SyncResponse(): SinkResponse_SyncResponse {
  return { epoch: 0 };
}

export const SinkResponse_SyncResponse = {
  fromJSON(object: any): SinkResponse_SyncResponse {
    return { epoch: isSet(object.epoch) ? Number(object.epoch) : 0 };
  },

  toJSON(message: SinkResponse_SyncResponse): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkResponse_SyncResponse>, I>>(object: I): SinkResponse_SyncResponse {
    const message = createBaseSinkResponse_SyncResponse();
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseSinkResponse_StartEpochResponse(): SinkResponse_StartEpochResponse {
  return { epoch: 0 };
}

export const SinkResponse_StartEpochResponse = {
  fromJSON(object: any): SinkResponse_StartEpochResponse {
    return { epoch: isSet(object.epoch) ? Number(object.epoch) : 0 };
  },

  toJSON(message: SinkResponse_StartEpochResponse): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkResponse_StartEpochResponse>, I>>(
    object: I,
  ): SinkResponse_StartEpochResponse {
    const message = createBaseSinkResponse_StartEpochResponse();
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseSinkResponse_WriteResponse(): SinkResponse_WriteResponse {
  return { epoch: 0, batchId: 0 };
}

export const SinkResponse_WriteResponse = {
  fromJSON(object: any): SinkResponse_WriteResponse {
    return {
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
      batchId: isSet(object.batchId) ? Number(object.batchId) : 0,
    };
  },

  toJSON(message: SinkResponse_WriteResponse): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    message.batchId !== undefined && (obj.batchId = Math.round(message.batchId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkResponse_WriteResponse>, I>>(object: I): SinkResponse_WriteResponse {
    const message = createBaseSinkResponse_WriteResponse();
    message.epoch = object.epoch ?? 0;
    message.batchId = object.batchId ?? 0;
    return message;
  },
};

function createBaseSinkResponse_StartResponse(): SinkResponse_StartResponse {
  return {};
}

export const SinkResponse_StartResponse = {
  fromJSON(_: any): SinkResponse_StartResponse {
    return {};
  },

  toJSON(_: SinkResponse_StartResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkResponse_StartResponse>, I>>(_: I): SinkResponse_StartResponse {
    const message = createBaseSinkResponse_StartResponse();
    return message;
  },
};

function createBaseDbConnectorProperties(): DbConnectorProperties {
  return {
    databaseHost: "",
    databasePort: "",
    databaseUser: "",
    databasePassword: "",
    databaseName: "",
    tableName: "",
    partition: "",
    startOffset: "",
    includeSchemaEvents: false,
  };
}

export const DbConnectorProperties = {
  fromJSON(object: any): DbConnectorProperties {
    return {
      databaseHost: isSet(object.databaseHost) ? String(object.databaseHost) : "",
      databasePort: isSet(object.databasePort) ? String(object.databasePort) : "",
      databaseUser: isSet(object.databaseUser) ? String(object.databaseUser) : "",
      databasePassword: isSet(object.databasePassword) ? String(object.databasePassword) : "",
      databaseName: isSet(object.databaseName) ? String(object.databaseName) : "",
      tableName: isSet(object.tableName) ? String(object.tableName) : "",
      partition: isSet(object.partition) ? String(object.partition) : "",
      startOffset: isSet(object.startOffset) ? String(object.startOffset) : "",
      includeSchemaEvents: isSet(object.includeSchemaEvents) ? Boolean(object.includeSchemaEvents) : false,
    };
  },

  toJSON(message: DbConnectorProperties): unknown {
    const obj: any = {};
    message.databaseHost !== undefined && (obj.databaseHost = message.databaseHost);
    message.databasePort !== undefined && (obj.databasePort = message.databasePort);
    message.databaseUser !== undefined && (obj.databaseUser = message.databaseUser);
    message.databasePassword !== undefined && (obj.databasePassword = message.databasePassword);
    message.databaseName !== undefined && (obj.databaseName = message.databaseName);
    message.tableName !== undefined && (obj.tableName = message.tableName);
    message.partition !== undefined && (obj.partition = message.partition);
    message.startOffset !== undefined && (obj.startOffset = message.startOffset);
    message.includeSchemaEvents !== undefined && (obj.includeSchemaEvents = message.includeSchemaEvents);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DbConnectorProperties>, I>>(object: I): DbConnectorProperties {
    const message = createBaseDbConnectorProperties();
    message.databaseHost = object.databaseHost ?? "";
    message.databasePort = object.databasePort ?? "";
    message.databaseUser = object.databaseUser ?? "";
    message.databasePassword = object.databasePassword ?? "";
    message.databaseName = object.databaseName ?? "";
    message.tableName = object.tableName ?? "";
    message.partition = object.partition ?? "";
    message.startOffset = object.startOffset ?? "";
    message.includeSchemaEvents = object.includeSchemaEvents ?? false;
    return message;
  },
};

function createBaseCdcMessage(): CdcMessage {
  return { payload: "", partition: "", offset: "" };
}

export const CdcMessage = {
  fromJSON(object: any): CdcMessage {
    return {
      payload: isSet(object.payload) ? String(object.payload) : "",
      partition: isSet(object.partition) ? String(object.partition) : "",
      offset: isSet(object.offset) ? String(object.offset) : "",
    };
  },

  toJSON(message: CdcMessage): unknown {
    const obj: any = {};
    message.payload !== undefined && (obj.payload = message.payload);
    message.partition !== undefined && (obj.partition = message.partition);
    message.offset !== undefined && (obj.offset = message.offset);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CdcMessage>, I>>(object: I): CdcMessage {
    const message = createBaseCdcMessage();
    message.payload = object.payload ?? "";
    message.partition = object.partition ?? "";
    message.offset = object.offset ?? "";
    return message;
  },
};

function createBaseGetEventStreamRequest(): GetEventStreamRequest {
  return { sourceId: 0, properties: undefined };
}

export const GetEventStreamRequest = {
  fromJSON(object: any): GetEventStreamRequest {
    return {
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      properties: isSet(object.properties) ? DbConnectorProperties.fromJSON(object.properties) : undefined,
    };
  },

  toJSON(message: GetEventStreamRequest): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.properties !== undefined &&
      (obj.properties = message.properties ? DbConnectorProperties.toJSON(message.properties) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetEventStreamRequest>, I>>(object: I): GetEventStreamRequest {
    const message = createBaseGetEventStreamRequest();
    message.sourceId = object.sourceId ?? 0;
    message.properties = (object.properties !== undefined && object.properties !== null)
      ? DbConnectorProperties.fromPartial(object.properties)
      : undefined;
    return message;
  },
};

function createBaseGetEventStreamResponse(): GetEventStreamResponse {
  return { sourceId: 0, events: [] };
}

export const GetEventStreamResponse = {
  fromJSON(object: any): GetEventStreamResponse {
    return {
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      events: Array.isArray(object?.events) ? object.events.map((e: any) => CdcMessage.fromJSON(e)) : [],
    };
  },

  toJSON(message: GetEventStreamResponse): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    if (message.events) {
      obj.events = message.events.map((e) => e ? CdcMessage.toJSON(e) : undefined);
    } else {
      obj.events = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetEventStreamResponse>, I>>(object: I): GetEventStreamResponse {
    const message = createBaseGetEventStreamResponse();
    message.sourceId = object.sourceId ?? 0;
    message.events = object.events?.map((e) => CdcMessage.fromPartial(e)) || [];
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
