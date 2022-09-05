/* eslint-disable */
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
