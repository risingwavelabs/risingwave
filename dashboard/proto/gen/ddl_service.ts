/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Database, Index, Schema, Sink, Source, Table } from "./catalog";
import { Status } from "./common";
import { StreamFragmentGraph } from "./stream_plan";

export const protobufPackage = "ddl_service";

export interface CreateDatabaseRequest {
  db: Database | undefined;
}

export interface CreateDatabaseResponse {
  status: Status | undefined;
  databaseId: number;
  version: number;
}

export interface DropDatabaseRequest {
  databaseId: number;
}

export interface DropDatabaseResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateSchemaRequest {
  schema: Schema | undefined;
}

export interface CreateSchemaResponse {
  status: Status | undefined;
  schemaId: number;
  version: number;
}

export interface DropSchemaRequest {
  schemaId: number;
}

export interface DropSchemaResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateSourceRequest {
  source: Source | undefined;
}

export interface CreateSourceResponse {
  status: Status | undefined;
  sourceId: number;
  version: number;
}

export interface DropSourceRequest {
  sourceId: number;
}

export interface DropSourceResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateSinkRequest {
  sink: Sink | undefined;
  fragmentGraph: StreamFragmentGraph | undefined;
}

export interface CreateSinkResponse {
  status: Status | undefined;
  sinkId: number;
  version: number;
}

export interface DropSinkRequest {
  sinkId: number;
}

export interface DropSinkResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateMaterializedViewRequest {
  materializedView: Table | undefined;
  fragmentGraph: StreamFragmentGraph | undefined;
}

export interface CreateMaterializedViewResponse {
  status: Status | undefined;
  tableId: number;
  version: number;
}

export interface DropMaterializedViewRequest {
  tableId: number;
}

export interface DropMaterializedViewResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateMaterializedSourceRequest {
  source: Source | undefined;
  materializedView: Table | undefined;
  fragmentGraph: StreamFragmentGraph | undefined;
}

export interface CreateMaterializedSourceResponse {
  status: Status | undefined;
  sourceId: number;
  tableId: number;
  version: number;
}

export interface DropMaterializedSourceRequest {
  sourceId: number;
  tableId: number;
}

export interface DropMaterializedSourceResponse {
  status: Status | undefined;
  version: number;
}

/** Used by risectl (and in the future, dashboard) */
export interface RisectlListStateTablesRequest {
}

/** Used by risectl (and in the future, dashboard) */
export interface RisectlListStateTablesResponse {
  tables: Table[];
}

export interface CreateIndexRequest {
  index: Index | undefined;
  indexTable: Table | undefined;
  fragmentGraph: StreamFragmentGraph | undefined;
}

export interface CreateIndexResponse {
  status: Status | undefined;
  indexId: number;
  version: number;
}

export interface DropIndexRequest {
  indexId: number;
}

export interface DropIndexResponse {
  status: Status | undefined;
  version: number;
}

function createBaseCreateDatabaseRequest(): CreateDatabaseRequest {
  return { db: undefined };
}

export const CreateDatabaseRequest = {
  encode(message: CreateDatabaseRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.db !== undefined) {
      Database.encode(message.db, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateDatabaseRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateDatabaseRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.db = Database.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateDatabaseRequest {
    return { db: isSet(object.db) ? Database.fromJSON(object.db) : undefined };
  },

  toJSON(message: CreateDatabaseRequest): unknown {
    const obj: any = {};
    message.db !== undefined && (obj.db = message.db ? Database.toJSON(message.db) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateDatabaseRequest>, I>>(object: I): CreateDatabaseRequest {
    const message = createBaseCreateDatabaseRequest();
    message.db = (object.db !== undefined && object.db !== null) ? Database.fromPartial(object.db) : undefined;
    return message;
  },
};

function createBaseCreateDatabaseResponse(): CreateDatabaseResponse {
  return { status: undefined, databaseId: 0, version: 0 };
}

export const CreateDatabaseResponse = {
  encode(message: CreateDatabaseResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.databaseId !== 0) {
      writer.uint32(16).uint32(message.databaseId);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateDatabaseResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateDatabaseResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.databaseId = reader.uint32();
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateDatabaseResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateDatabaseResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateDatabaseResponse>, I>>(object: I): CreateDatabaseResponse {
    const message = createBaseCreateDatabaseResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.databaseId = object.databaseId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropDatabaseRequest(): DropDatabaseRequest {
  return { databaseId: 0 };
}

export const DropDatabaseRequest = {
  encode(message: DropDatabaseRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.databaseId !== 0) {
      writer.uint32(8).uint32(message.databaseId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropDatabaseRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropDatabaseRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.databaseId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropDatabaseRequest {
    return { databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0 };
  },

  toJSON(message: DropDatabaseRequest): unknown {
    const obj: any = {};
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropDatabaseRequest>, I>>(object: I): DropDatabaseRequest {
    const message = createBaseDropDatabaseRequest();
    message.databaseId = object.databaseId ?? 0;
    return message;
  },
};

function createBaseDropDatabaseResponse(): DropDatabaseResponse {
  return { status: undefined, version: 0 };
}

export const DropDatabaseResponse = {
  encode(message: DropDatabaseResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropDatabaseResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropDatabaseResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropDatabaseResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropDatabaseResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropDatabaseResponse>, I>>(object: I): DropDatabaseResponse {
    const message = createBaseDropDatabaseResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateSchemaRequest(): CreateSchemaRequest {
  return { schema: undefined };
}

export const CreateSchemaRequest = {
  encode(message: CreateSchemaRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.schema !== undefined) {
      Schema.encode(message.schema, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSchemaRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSchemaRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.schema = Schema.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSchemaRequest {
    return { schema: isSet(object.schema) ? Schema.fromJSON(object.schema) : undefined };
  },

  toJSON(message: CreateSchemaRequest): unknown {
    const obj: any = {};
    message.schema !== undefined && (obj.schema = message.schema ? Schema.toJSON(message.schema) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSchemaRequest>, I>>(object: I): CreateSchemaRequest {
    const message = createBaseCreateSchemaRequest();
    message.schema = (object.schema !== undefined && object.schema !== null)
      ? Schema.fromPartial(object.schema)
      : undefined;
    return message;
  },
};

function createBaseCreateSchemaResponse(): CreateSchemaResponse {
  return { status: undefined, schemaId: 0, version: 0 };
}

export const CreateSchemaResponse = {
  encode(message: CreateSchemaResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.schemaId !== 0) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSchemaResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSchemaResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSchemaResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateSchemaResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSchemaResponse>, I>>(object: I): CreateSchemaResponse {
    const message = createBaseCreateSchemaResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.schemaId = object.schemaId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropSchemaRequest(): DropSchemaRequest {
  return { schemaId: 0 };
}

export const DropSchemaRequest = {
  encode(message: DropSchemaRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.schemaId !== 0) {
      writer.uint32(8).uint32(message.schemaId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSchemaRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSchemaRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.schemaId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSchemaRequest {
    return { schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0 };
  },

  toJSON(message: DropSchemaRequest): unknown {
    const obj: any = {};
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSchemaRequest>, I>>(object: I): DropSchemaRequest {
    const message = createBaseDropSchemaRequest();
    message.schemaId = object.schemaId ?? 0;
    return message;
  },
};

function createBaseDropSchemaResponse(): DropSchemaResponse {
  return { status: undefined, version: 0 };
}

export const DropSchemaResponse = {
  encode(message: DropSchemaResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSchemaResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSchemaResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSchemaResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropSchemaResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSchemaResponse>, I>>(object: I): DropSchemaResponse {
    const message = createBaseDropSchemaResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateSourceRequest(): CreateSourceRequest {
  return { source: undefined };
}

export const CreateSourceRequest = {
  encode(message: CreateSourceRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.source !== undefined) {
      Source.encode(message.source, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSourceRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSourceRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.source = Source.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSourceRequest {
    return { source: isSet(object.source) ? Source.fromJSON(object.source) : undefined };
  },

  toJSON(message: CreateSourceRequest): unknown {
    const obj: any = {};
    message.source !== undefined && (obj.source = message.source ? Source.toJSON(message.source) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSourceRequest>, I>>(object: I): CreateSourceRequest {
    const message = createBaseCreateSourceRequest();
    message.source = (object.source !== undefined && object.source !== null)
      ? Source.fromPartial(object.source)
      : undefined;
    return message;
  },
};

function createBaseCreateSourceResponse(): CreateSourceResponse {
  return { status: undefined, sourceId: 0, version: 0 };
}

export const CreateSourceResponse = {
  encode(message: CreateSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.sourceId !== 0) {
      writer.uint32(16).uint32(message.sourceId);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSourceResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSourceResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.sourceId = reader.uint32();
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSourceResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSourceResponse>, I>>(object: I): CreateSourceResponse {
    const message = createBaseCreateSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.sourceId = object.sourceId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropSourceRequest(): DropSourceRequest {
  return { sourceId: 0 };
}

export const DropSourceRequest = {
  encode(message: DropSourceRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sourceId !== 0) {
      writer.uint32(8).uint32(message.sourceId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSourceRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSourceRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sourceId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSourceRequest {
    return { sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0 };
  },

  toJSON(message: DropSourceRequest): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSourceRequest>, I>>(object: I): DropSourceRequest {
    const message = createBaseDropSourceRequest();
    message.sourceId = object.sourceId ?? 0;
    return message;
  },
};

function createBaseDropSourceResponse(): DropSourceResponse {
  return { status: undefined, version: 0 };
}

export const DropSourceResponse = {
  encode(message: DropSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSourceResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSourceResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSourceResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSourceResponse>, I>>(object: I): DropSourceResponse {
    const message = createBaseDropSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateSinkRequest(): CreateSinkRequest {
  return { sink: undefined, fragmentGraph: undefined };
}

export const CreateSinkRequest = {
  encode(message: CreateSinkRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sink !== undefined) {
      Sink.encode(message.sink, writer.uint32(10).fork()).ldelim();
    }
    if (message.fragmentGraph !== undefined) {
      StreamFragmentGraph.encode(message.fragmentGraph, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSinkRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSinkRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sink = Sink.decode(reader, reader.uint32());
          break;
        case 2:
          message.fragmentGraph = StreamFragmentGraph.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSinkRequest {
    return {
      sink: isSet(object.sink) ? Sink.fromJSON(object.sink) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
    };
  },

  toJSON(message: CreateSinkRequest): unknown {
    const obj: any = {};
    message.sink !== undefined && (obj.sink = message.sink ? Sink.toJSON(message.sink) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSinkRequest>, I>>(object: I): CreateSinkRequest {
    const message = createBaseCreateSinkRequest();
    message.sink = (object.sink !== undefined && object.sink !== null) ? Sink.fromPartial(object.sink) : undefined;
    message.fragmentGraph = (object.fragmentGraph !== undefined && object.fragmentGraph !== null)
      ? StreamFragmentGraph.fromPartial(object.fragmentGraph)
      : undefined;
    return message;
  },
};

function createBaseCreateSinkResponse(): CreateSinkResponse {
  return { status: undefined, sinkId: 0, version: 0 };
}

export const CreateSinkResponse = {
  encode(message: CreateSinkResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.sinkId !== 0) {
      writer.uint32(16).uint32(message.sinkId);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateSinkResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateSinkResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.sinkId = reader.uint32();
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSinkResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      sinkId: isSet(object.sinkId) ? Number(object.sinkId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateSinkResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.sinkId !== undefined && (obj.sinkId = Math.round(message.sinkId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSinkResponse>, I>>(object: I): CreateSinkResponse {
    const message = createBaseCreateSinkResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.sinkId = object.sinkId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropSinkRequest(): DropSinkRequest {
  return { sinkId: 0 };
}

export const DropSinkRequest = {
  encode(message: DropSinkRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sinkId !== 0) {
      writer.uint32(8).uint32(message.sinkId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSinkRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSinkRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sinkId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSinkRequest {
    return { sinkId: isSet(object.sinkId) ? Number(object.sinkId) : 0 };
  },

  toJSON(message: DropSinkRequest): unknown {
    const obj: any = {};
    message.sinkId !== undefined && (obj.sinkId = Math.round(message.sinkId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSinkRequest>, I>>(object: I): DropSinkRequest {
    const message = createBaseDropSinkRequest();
    message.sinkId = object.sinkId ?? 0;
    return message;
  },
};

function createBaseDropSinkResponse(): DropSinkResponse {
  return { status: undefined, version: 0 };
}

export const DropSinkResponse = {
  encode(message: DropSinkResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropSinkResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropSinkResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSinkResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropSinkResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSinkResponse>, I>>(object: I): DropSinkResponse {
    const message = createBaseDropSinkResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateMaterializedViewRequest(): CreateMaterializedViewRequest {
  return { materializedView: undefined, fragmentGraph: undefined };
}

export const CreateMaterializedViewRequest = {
  encode(message: CreateMaterializedViewRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.materializedView !== undefined) {
      Table.encode(message.materializedView, writer.uint32(10).fork()).ldelim();
    }
    if (message.fragmentGraph !== undefined) {
      StreamFragmentGraph.encode(message.fragmentGraph, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateMaterializedViewRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateMaterializedViewRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.materializedView = Table.decode(reader, reader.uint32());
          break;
        case 2:
          message.fragmentGraph = StreamFragmentGraph.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateMaterializedViewRequest {
    return {
      materializedView: isSet(object.materializedView) ? Table.fromJSON(object.materializedView) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
    };
  },

  toJSON(message: CreateMaterializedViewRequest): unknown {
    const obj: any = {};
    message.materializedView !== undefined &&
      (obj.materializedView = message.materializedView ? Table.toJSON(message.materializedView) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateMaterializedViewRequest>, I>>(
    object: I,
  ): CreateMaterializedViewRequest {
    const message = createBaseCreateMaterializedViewRequest();
    message.materializedView = (object.materializedView !== undefined && object.materializedView !== null)
      ? Table.fromPartial(object.materializedView)
      : undefined;
    message.fragmentGraph = (object.fragmentGraph !== undefined && object.fragmentGraph !== null)
      ? StreamFragmentGraph.fromPartial(object.fragmentGraph)
      : undefined;
    return message;
  },
};

function createBaseCreateMaterializedViewResponse(): CreateMaterializedViewResponse {
  return { status: undefined, tableId: 0, version: 0 };
}

export const CreateMaterializedViewResponse = {
  encode(message: CreateMaterializedViewResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.tableId !== 0) {
      writer.uint32(16).uint32(message.tableId);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateMaterializedViewResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateMaterializedViewResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.tableId = reader.uint32();
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateMaterializedViewResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateMaterializedViewResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateMaterializedViewResponse>, I>>(
    object: I,
  ): CreateMaterializedViewResponse {
    const message = createBaseCreateMaterializedViewResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.tableId = object.tableId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropMaterializedViewRequest(): DropMaterializedViewRequest {
  return { tableId: 0 };
}

export const DropMaterializedViewRequest = {
  encode(message: DropMaterializedViewRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableId !== 0) {
      writer.uint32(8).uint32(message.tableId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropMaterializedViewRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropMaterializedViewRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropMaterializedViewRequest {
    return { tableId: isSet(object.tableId) ? Number(object.tableId) : 0 };
  },

  toJSON(message: DropMaterializedViewRequest): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropMaterializedViewRequest>, I>>(object: I): DropMaterializedViewRequest {
    const message = createBaseDropMaterializedViewRequest();
    message.tableId = object.tableId ?? 0;
    return message;
  },
};

function createBaseDropMaterializedViewResponse(): DropMaterializedViewResponse {
  return { status: undefined, version: 0 };
}

export const DropMaterializedViewResponse = {
  encode(message: DropMaterializedViewResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropMaterializedViewResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropMaterializedViewResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropMaterializedViewResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropMaterializedViewResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropMaterializedViewResponse>, I>>(object: I): DropMaterializedViewResponse {
    const message = createBaseDropMaterializedViewResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateMaterializedSourceRequest(): CreateMaterializedSourceRequest {
  return { source: undefined, materializedView: undefined, fragmentGraph: undefined };
}

export const CreateMaterializedSourceRequest = {
  encode(message: CreateMaterializedSourceRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.source !== undefined) {
      Source.encode(message.source, writer.uint32(10).fork()).ldelim();
    }
    if (message.materializedView !== undefined) {
      Table.encode(message.materializedView, writer.uint32(18).fork()).ldelim();
    }
    if (message.fragmentGraph !== undefined) {
      StreamFragmentGraph.encode(message.fragmentGraph, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateMaterializedSourceRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateMaterializedSourceRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.source = Source.decode(reader, reader.uint32());
          break;
        case 2:
          message.materializedView = Table.decode(reader, reader.uint32());
          break;
        case 3:
          message.fragmentGraph = StreamFragmentGraph.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateMaterializedSourceRequest {
    return {
      source: isSet(object.source) ? Source.fromJSON(object.source) : undefined,
      materializedView: isSet(object.materializedView) ? Table.fromJSON(object.materializedView) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
    };
  },

  toJSON(message: CreateMaterializedSourceRequest): unknown {
    const obj: any = {};
    message.source !== undefined && (obj.source = message.source ? Source.toJSON(message.source) : undefined);
    message.materializedView !== undefined &&
      (obj.materializedView = message.materializedView ? Table.toJSON(message.materializedView) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateMaterializedSourceRequest>, I>>(
    object: I,
  ): CreateMaterializedSourceRequest {
    const message = createBaseCreateMaterializedSourceRequest();
    message.source = (object.source !== undefined && object.source !== null)
      ? Source.fromPartial(object.source)
      : undefined;
    message.materializedView = (object.materializedView !== undefined && object.materializedView !== null)
      ? Table.fromPartial(object.materializedView)
      : undefined;
    message.fragmentGraph = (object.fragmentGraph !== undefined && object.fragmentGraph !== null)
      ? StreamFragmentGraph.fromPartial(object.fragmentGraph)
      : undefined;
    return message;
  },
};

function createBaseCreateMaterializedSourceResponse(): CreateMaterializedSourceResponse {
  return { status: undefined, sourceId: 0, tableId: 0, version: 0 };
}

export const CreateMaterializedSourceResponse = {
  encode(message: CreateMaterializedSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.sourceId !== 0) {
      writer.uint32(16).uint32(message.sourceId);
    }
    if (message.tableId !== 0) {
      writer.uint32(24).uint32(message.tableId);
    }
    if (message.version !== 0) {
      writer.uint32(32).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateMaterializedSourceResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateMaterializedSourceResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.sourceId = reader.uint32();
          break;
        case 3:
          message.tableId = reader.uint32();
          break;
        case 4:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateMaterializedSourceResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateMaterializedSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateMaterializedSourceResponse>, I>>(
    object: I,
  ): CreateMaterializedSourceResponse {
    const message = createBaseCreateMaterializedSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.sourceId = object.sourceId ?? 0;
    message.tableId = object.tableId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropMaterializedSourceRequest(): DropMaterializedSourceRequest {
  return { sourceId: 0, tableId: 0 };
}

export const DropMaterializedSourceRequest = {
  encode(message: DropMaterializedSourceRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sourceId !== 0) {
      writer.uint32(8).uint32(message.sourceId);
    }
    if (message.tableId !== 0) {
      writer.uint32(16).uint32(message.tableId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropMaterializedSourceRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropMaterializedSourceRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sourceId = reader.uint32();
          break;
        case 2:
          message.tableId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropMaterializedSourceRequest {
    return {
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
    };
  },

  toJSON(message: DropMaterializedSourceRequest): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropMaterializedSourceRequest>, I>>(
    object: I,
  ): DropMaterializedSourceRequest {
    const message = createBaseDropMaterializedSourceRequest();
    message.sourceId = object.sourceId ?? 0;
    message.tableId = object.tableId ?? 0;
    return message;
  },
};

function createBaseDropMaterializedSourceResponse(): DropMaterializedSourceResponse {
  return { status: undefined, version: 0 };
}

export const DropMaterializedSourceResponse = {
  encode(message: DropMaterializedSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropMaterializedSourceResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropMaterializedSourceResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropMaterializedSourceResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropMaterializedSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropMaterializedSourceResponse>, I>>(
    object: I,
  ): DropMaterializedSourceResponse {
    const message = createBaseDropMaterializedSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseRisectlListStateTablesRequest(): RisectlListStateTablesRequest {
  return {};
}

export const RisectlListStateTablesRequest = {
  encode(_: RisectlListStateTablesRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RisectlListStateTablesRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRisectlListStateTablesRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(_: any): RisectlListStateTablesRequest {
    return {};
  },

  toJSON(_: RisectlListStateTablesRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RisectlListStateTablesRequest>, I>>(_: I): RisectlListStateTablesRequest {
    const message = createBaseRisectlListStateTablesRequest();
    return message;
  },
};

function createBaseRisectlListStateTablesResponse(): RisectlListStateTablesResponse {
  return { tables: [] };
}

export const RisectlListStateTablesResponse = {
  encode(message: RisectlListStateTablesResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.tables) {
      Table.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RisectlListStateTablesResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRisectlListStateTablesResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tables.push(Table.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): RisectlListStateTablesResponse {
    return { tables: Array.isArray(object?.tables) ? object.tables.map((e: any) => Table.fromJSON(e)) : [] };
  },

  toJSON(message: RisectlListStateTablesResponse): unknown {
    const obj: any = {};
    if (message.tables) {
      obj.tables = message.tables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.tables = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RisectlListStateTablesResponse>, I>>(
    object: I,
  ): RisectlListStateTablesResponse {
    const message = createBaseRisectlListStateTablesResponse();
    message.tables = object.tables?.map((e) => Table.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCreateIndexRequest(): CreateIndexRequest {
  return { index: undefined, indexTable: undefined, fragmentGraph: undefined };
}

export const CreateIndexRequest = {
  encode(message: CreateIndexRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.index !== undefined) {
      Index.encode(message.index, writer.uint32(10).fork()).ldelim();
    }
    if (message.indexTable !== undefined) {
      Table.encode(message.indexTable, writer.uint32(18).fork()).ldelim();
    }
    if (message.fragmentGraph !== undefined) {
      StreamFragmentGraph.encode(message.fragmentGraph, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateIndexRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateIndexRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.index = Index.decode(reader, reader.uint32());
          break;
        case 2:
          message.indexTable = Table.decode(reader, reader.uint32());
          break;
        case 3:
          message.fragmentGraph = StreamFragmentGraph.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateIndexRequest {
    return {
      index: isSet(object.index) ? Index.fromJSON(object.index) : undefined,
      indexTable: isSet(object.indexTable) ? Table.fromJSON(object.indexTable) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
    };
  },

  toJSON(message: CreateIndexRequest): unknown {
    const obj: any = {};
    message.index !== undefined && (obj.index = message.index ? Index.toJSON(message.index) : undefined);
    message.indexTable !== undefined &&
      (obj.indexTable = message.indexTable ? Table.toJSON(message.indexTable) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateIndexRequest>, I>>(object: I): CreateIndexRequest {
    const message = createBaseCreateIndexRequest();
    message.index = (object.index !== undefined && object.index !== null) ? Index.fromPartial(object.index) : undefined;
    message.indexTable = (object.indexTable !== undefined && object.indexTable !== null)
      ? Table.fromPartial(object.indexTable)
      : undefined;
    message.fragmentGraph = (object.fragmentGraph !== undefined && object.fragmentGraph !== null)
      ? StreamFragmentGraph.fromPartial(object.fragmentGraph)
      : undefined;
    return message;
  },
};

function createBaseCreateIndexResponse(): CreateIndexResponse {
  return { status: undefined, indexId: 0, version: 0 };
}

export const CreateIndexResponse = {
  encode(message: CreateIndexResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.indexId !== 0) {
      writer.uint32(16).uint32(message.indexId);
    }
    if (message.version !== 0) {
      writer.uint32(32).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateIndexResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateIndexResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.indexId = reader.uint32();
          break;
        case 4:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateIndexResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      indexId: isSet(object.indexId) ? Number(object.indexId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateIndexResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.indexId !== undefined && (obj.indexId = Math.round(message.indexId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateIndexResponse>, I>>(object: I): CreateIndexResponse {
    const message = createBaseCreateIndexResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.indexId = object.indexId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropIndexRequest(): DropIndexRequest {
  return { indexId: 0 };
}

export const DropIndexRequest = {
  encode(message: DropIndexRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.indexId !== 0) {
      writer.uint32(8).uint32(message.indexId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropIndexRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropIndexRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.indexId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropIndexRequest {
    return { indexId: isSet(object.indexId) ? Number(object.indexId) : 0 };
  },

  toJSON(message: DropIndexRequest): unknown {
    const obj: any = {};
    message.indexId !== undefined && (obj.indexId = Math.round(message.indexId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropIndexRequest>, I>>(object: I): DropIndexRequest {
    const message = createBaseDropIndexRequest();
    message.indexId = object.indexId ?? 0;
    return message;
  },
};

function createBaseDropIndexResponse(): DropIndexResponse {
  return { status: undefined, version: 0 };
}

export const DropIndexResponse = {
  encode(message: DropIndexResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropIndexResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropIndexResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropIndexResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropIndexResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropIndexResponse>, I>>(object: I): DropIndexResponse {
    const message = createBaseDropIndexResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
