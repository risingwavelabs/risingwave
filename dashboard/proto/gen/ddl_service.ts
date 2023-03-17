/* eslint-disable */
import { ColIndexMapping, Connection, Database, Function, Index, Schema, Sink, Source, Table, View } from "./catalog";
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

export interface CreateViewRequest {
  view: View | undefined;
}

export interface CreateViewResponse {
  status: Status | undefined;
  viewId: number;
  version: number;
}

export interface DropViewRequest {
  viewId: number;
}

export interface DropViewResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateTableRequest {
  /**
   * An optional field and will be `Some` for tables with an external connector. If so, the table
   * will subscribe to the changes of the external connector and materialize the data.
   */
  source: Source | undefined;
  materializedView: Table | undefined;
  fragmentGraph: StreamFragmentGraph | undefined;
}

export interface CreateTableResponse {
  status: Status | undefined;
  tableId: number;
  version: number;
}

export interface AlterRelationNameRequest {
  relation?: { $case: "tableId"; tableId: number } | { $case: "viewId"; viewId: number } | {
    $case: "indexId";
    indexId: number;
  } | { $case: "sinkId"; sinkId: number };
  newName: string;
}

export interface AlterRelationNameResponse {
  status: Status | undefined;
  version: number;
}

export interface CreateFunctionRequest {
  function: Function | undefined;
}

export interface CreateFunctionResponse {
  status: Status | undefined;
  functionId: number;
  version: number;
}

export interface DropFunctionRequest {
  functionId: number;
}

export interface DropFunctionResponse {
  status: Status | undefined;
  version: number;
}

export interface DropTableRequest {
  sourceId?: { $case: "id"; id: number };
  tableId: number;
}

export interface DropTableResponse {
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

export interface ReplaceTablePlanRequest {
  /**
   * The new table catalog, with the correct table ID and a new version.
   * If the new version does not match the subsequent version in the meta service's
   * catalog, this request will be rejected.
   */
  table:
    | Table
    | undefined;
  /** The new materialization plan, where all schema are updated. */
  fragmentGraph:
    | StreamFragmentGraph
    | undefined;
  /** The mapping from the old columns to the new columns of the table. */
  tableColIndexMapping: ColIndexMapping | undefined;
}

export interface ReplaceTablePlanResponse {
  status:
    | Status
    | undefined;
  /** The new global catalog version. */
  version: number;
}

export interface GetTableRequest {
  databaseName: string;
  tableName: string;
}

export interface GetTableResponse {
  table: Table | undefined;
}

export interface GetDdlProgressRequest {
}

export interface DdlProgress {
  id: number;
  statement: string;
  progress: string;
}

export interface GetDdlProgressResponse {
  ddlProgress: DdlProgress[];
}

export interface CreateConnectionRequest {
  payload?: { $case: "privateLink"; privateLink: CreateConnectionRequest_PrivateLink };
}

export interface CreateConnectionRequest_PrivateLink {
  provider: string;
  serviceName: string;
  availabilityZones: string[];
}

export interface CreateConnectionResponse {
  connectionId: number;
  /** global catalog version */
  version: number;
}

export interface ListConnectionsRequest {
}

export interface ListConnectionsResponse {
  connections: Connection[];
}

export interface DropConnectionRequest {
  connectionName: string;
}

export interface DropConnectionResponse {
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

function createBaseCreateViewRequest(): CreateViewRequest {
  return { view: undefined };
}

export const CreateViewRequest = {
  fromJSON(object: any): CreateViewRequest {
    return { view: isSet(object.view) ? View.fromJSON(object.view) : undefined };
  },

  toJSON(message: CreateViewRequest): unknown {
    const obj: any = {};
    message.view !== undefined && (obj.view = message.view ? View.toJSON(message.view) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateViewRequest>, I>>(object: I): CreateViewRequest {
    const message = createBaseCreateViewRequest();
    message.view = (object.view !== undefined && object.view !== null) ? View.fromPartial(object.view) : undefined;
    return message;
  },
};

function createBaseCreateViewResponse(): CreateViewResponse {
  return { status: undefined, viewId: 0, version: 0 };
}

export const CreateViewResponse = {
  fromJSON(object: any): CreateViewResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      viewId: isSet(object.viewId) ? Number(object.viewId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateViewResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.viewId !== undefined && (obj.viewId = Math.round(message.viewId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateViewResponse>, I>>(object: I): CreateViewResponse {
    const message = createBaseCreateViewResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.viewId = object.viewId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropViewRequest(): DropViewRequest {
  return { viewId: 0 };
}

export const DropViewRequest = {
  fromJSON(object: any): DropViewRequest {
    return { viewId: isSet(object.viewId) ? Number(object.viewId) : 0 };
  },

  toJSON(message: DropViewRequest): unknown {
    const obj: any = {};
    message.viewId !== undefined && (obj.viewId = Math.round(message.viewId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropViewRequest>, I>>(object: I): DropViewRequest {
    const message = createBaseDropViewRequest();
    message.viewId = object.viewId ?? 0;
    return message;
  },
};

function createBaseDropViewResponse(): DropViewResponse {
  return { status: undefined, version: 0 };
}

export const DropViewResponse = {
  fromJSON(object: any): DropViewResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropViewResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropViewResponse>, I>>(object: I): DropViewResponse {
    const message = createBaseDropViewResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateTableRequest(): CreateTableRequest {
  return { source: undefined, materializedView: undefined, fragmentGraph: undefined };
}

export const CreateTableRequest = {
  fromJSON(object: any): CreateTableRequest {
    return {
      source: isSet(object.source) ? Source.fromJSON(object.source) : undefined,
      materializedView: isSet(object.materializedView) ? Table.fromJSON(object.materializedView) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
    };
  },

  toJSON(message: CreateTableRequest): unknown {
    const obj: any = {};
    message.source !== undefined && (obj.source = message.source ? Source.toJSON(message.source) : undefined);
    message.materializedView !== undefined &&
      (obj.materializedView = message.materializedView ? Table.toJSON(message.materializedView) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateTableRequest>, I>>(object: I): CreateTableRequest {
    const message = createBaseCreateTableRequest();
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

function createBaseCreateTableResponse(): CreateTableResponse {
  return { status: undefined, tableId: 0, version: 0 };
}

export const CreateTableResponse = {
  fromJSON(object: any): CreateTableResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateTableResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateTableResponse>, I>>(object: I): CreateTableResponse {
    const message = createBaseCreateTableResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.tableId = object.tableId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseAlterRelationNameRequest(): AlterRelationNameRequest {
  return { relation: undefined, newName: "" };
}

export const AlterRelationNameRequest = {
  fromJSON(object: any): AlterRelationNameRequest {
    return {
      relation: isSet(object.tableId)
        ? { $case: "tableId", tableId: Number(object.tableId) }
        : isSet(object.viewId)
        ? { $case: "viewId", viewId: Number(object.viewId) }
        : isSet(object.indexId)
        ? { $case: "indexId", indexId: Number(object.indexId) }
        : isSet(object.sinkId)
        ? { $case: "sinkId", sinkId: Number(object.sinkId) }
        : undefined,
      newName: isSet(object.newName) ? String(object.newName) : "",
    };
  },

  toJSON(message: AlterRelationNameRequest): unknown {
    const obj: any = {};
    message.relation?.$case === "tableId" && (obj.tableId = Math.round(message.relation?.tableId));
    message.relation?.$case === "viewId" && (obj.viewId = Math.round(message.relation?.viewId));
    message.relation?.$case === "indexId" && (obj.indexId = Math.round(message.relation?.indexId));
    message.relation?.$case === "sinkId" && (obj.sinkId = Math.round(message.relation?.sinkId));
    message.newName !== undefined && (obj.newName = message.newName);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AlterRelationNameRequest>, I>>(object: I): AlterRelationNameRequest {
    const message = createBaseAlterRelationNameRequest();
    if (
      object.relation?.$case === "tableId" &&
      object.relation?.tableId !== undefined &&
      object.relation?.tableId !== null
    ) {
      message.relation = { $case: "tableId", tableId: object.relation.tableId };
    }
    if (
      object.relation?.$case === "viewId" && object.relation?.viewId !== undefined && object.relation?.viewId !== null
    ) {
      message.relation = { $case: "viewId", viewId: object.relation.viewId };
    }
    if (
      object.relation?.$case === "indexId" &&
      object.relation?.indexId !== undefined &&
      object.relation?.indexId !== null
    ) {
      message.relation = { $case: "indexId", indexId: object.relation.indexId };
    }
    if (
      object.relation?.$case === "sinkId" && object.relation?.sinkId !== undefined && object.relation?.sinkId !== null
    ) {
      message.relation = { $case: "sinkId", sinkId: object.relation.sinkId };
    }
    message.newName = object.newName ?? "";
    return message;
  },
};

function createBaseAlterRelationNameResponse(): AlterRelationNameResponse {
  return { status: undefined, version: 0 };
}

export const AlterRelationNameResponse = {
  fromJSON(object: any): AlterRelationNameResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: AlterRelationNameResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AlterRelationNameResponse>, I>>(object: I): AlterRelationNameResponse {
    const message = createBaseAlterRelationNameResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseCreateFunctionRequest(): CreateFunctionRequest {
  return { function: undefined };
}

export const CreateFunctionRequest = {
  fromJSON(object: any): CreateFunctionRequest {
    return { function: isSet(object.function) ? Function.fromJSON(object.function) : undefined };
  },

  toJSON(message: CreateFunctionRequest): unknown {
    const obj: any = {};
    message.function !== undefined && (obj.function = message.function ? Function.toJSON(message.function) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateFunctionRequest>, I>>(object: I): CreateFunctionRequest {
    const message = createBaseCreateFunctionRequest();
    message.function = (object.function !== undefined && object.function !== null)
      ? Function.fromPartial(object.function)
      : undefined;
    return message;
  },
};

function createBaseCreateFunctionResponse(): CreateFunctionResponse {
  return { status: undefined, functionId: 0, version: 0 };
}

export const CreateFunctionResponse = {
  fromJSON(object: any): CreateFunctionResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      functionId: isSet(object.functionId) ? Number(object.functionId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateFunctionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.functionId !== undefined && (obj.functionId = Math.round(message.functionId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateFunctionResponse>, I>>(object: I): CreateFunctionResponse {
    const message = createBaseCreateFunctionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.functionId = object.functionId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropFunctionRequest(): DropFunctionRequest {
  return { functionId: 0 };
}

export const DropFunctionRequest = {
  fromJSON(object: any): DropFunctionRequest {
    return { functionId: isSet(object.functionId) ? Number(object.functionId) : 0 };
  },

  toJSON(message: DropFunctionRequest): unknown {
    const obj: any = {};
    message.functionId !== undefined && (obj.functionId = Math.round(message.functionId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropFunctionRequest>, I>>(object: I): DropFunctionRequest {
    const message = createBaseDropFunctionRequest();
    message.functionId = object.functionId ?? 0;
    return message;
  },
};

function createBaseDropFunctionResponse(): DropFunctionResponse {
  return { status: undefined, version: 0 };
}

export const DropFunctionResponse = {
  fromJSON(object: any): DropFunctionResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropFunctionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropFunctionResponse>, I>>(object: I): DropFunctionResponse {
    const message = createBaseDropFunctionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropTableRequest(): DropTableRequest {
  return { sourceId: undefined, tableId: 0 };
}

export const DropTableRequest = {
  fromJSON(object: any): DropTableRequest {
    return {
      sourceId: isSet(object.id) ? { $case: "id", id: Number(object.id) } : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
    };
  },

  toJSON(message: DropTableRequest): unknown {
    const obj: any = {};
    message.sourceId?.$case === "id" && (obj.id = Math.round(message.sourceId?.id));
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropTableRequest>, I>>(object: I): DropTableRequest {
    const message = createBaseDropTableRequest();
    if (object.sourceId?.$case === "id" && object.sourceId?.id !== undefined && object.sourceId?.id !== null) {
      message.sourceId = { $case: "id", id: object.sourceId.id };
    }
    message.tableId = object.tableId ?? 0;
    return message;
  },
};

function createBaseDropTableResponse(): DropTableResponse {
  return { status: undefined, version: 0 };
}

export const DropTableResponse = {
  fromJSON(object: any): DropTableResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropTableResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropTableResponse>, I>>(object: I): DropTableResponse {
    const message = createBaseDropTableResponse();
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

function createBaseReplaceTablePlanRequest(): ReplaceTablePlanRequest {
  return { table: undefined, fragmentGraph: undefined, tableColIndexMapping: undefined };
}

export const ReplaceTablePlanRequest = {
  fromJSON(object: any): ReplaceTablePlanRequest {
    return {
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      fragmentGraph: isSet(object.fragmentGraph) ? StreamFragmentGraph.fromJSON(object.fragmentGraph) : undefined,
      tableColIndexMapping: isSet(object.tableColIndexMapping)
        ? ColIndexMapping.fromJSON(object.tableColIndexMapping)
        : undefined,
    };
  },

  toJSON(message: ReplaceTablePlanRequest): unknown {
    const obj: any = {};
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    message.fragmentGraph !== undefined &&
      (obj.fragmentGraph = message.fragmentGraph ? StreamFragmentGraph.toJSON(message.fragmentGraph) : undefined);
    message.tableColIndexMapping !== undefined && (obj.tableColIndexMapping = message.tableColIndexMapping
      ? ColIndexMapping.toJSON(message.tableColIndexMapping)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReplaceTablePlanRequest>, I>>(object: I): ReplaceTablePlanRequest {
    const message = createBaseReplaceTablePlanRequest();
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.fragmentGraph = (object.fragmentGraph !== undefined && object.fragmentGraph !== null)
      ? StreamFragmentGraph.fromPartial(object.fragmentGraph)
      : undefined;
    message.tableColIndexMapping = (object.tableColIndexMapping !== undefined && object.tableColIndexMapping !== null)
      ? ColIndexMapping.fromPartial(object.tableColIndexMapping)
      : undefined;
    return message;
  },
};

function createBaseReplaceTablePlanResponse(): ReplaceTablePlanResponse {
  return { status: undefined, version: 0 };
}

export const ReplaceTablePlanResponse = {
  fromJSON(object: any): ReplaceTablePlanResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: ReplaceTablePlanResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReplaceTablePlanResponse>, I>>(object: I): ReplaceTablePlanResponse {
    const message = createBaseReplaceTablePlanResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseGetTableRequest(): GetTableRequest {
  return { databaseName: "", tableName: "" };
}

export const GetTableRequest = {
  fromJSON(object: any): GetTableRequest {
    return {
      databaseName: isSet(object.databaseName) ? String(object.databaseName) : "",
      tableName: isSet(object.tableName) ? String(object.tableName) : "",
    };
  },

  toJSON(message: GetTableRequest): unknown {
    const obj: any = {};
    message.databaseName !== undefined && (obj.databaseName = message.databaseName);
    message.tableName !== undefined && (obj.tableName = message.tableName);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetTableRequest>, I>>(object: I): GetTableRequest {
    const message = createBaseGetTableRequest();
    message.databaseName = object.databaseName ?? "";
    message.tableName = object.tableName ?? "";
    return message;
  },
};

function createBaseGetTableResponse(): GetTableResponse {
  return { table: undefined };
}

export const GetTableResponse = {
  fromJSON(object: any): GetTableResponse {
    return { table: isSet(object.table) ? Table.fromJSON(object.table) : undefined };
  },

  toJSON(message: GetTableResponse): unknown {
    const obj: any = {};
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetTableResponse>, I>>(object: I): GetTableResponse {
    const message = createBaseGetTableResponse();
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    return message;
  },
};

function createBaseGetDdlProgressRequest(): GetDdlProgressRequest {
  return {};
}

export const GetDdlProgressRequest = {
  fromJSON(_: any): GetDdlProgressRequest {
    return {};
  },

  toJSON(_: GetDdlProgressRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetDdlProgressRequest>, I>>(_: I): GetDdlProgressRequest {
    const message = createBaseGetDdlProgressRequest();
    return message;
  },
};

function createBaseDdlProgress(): DdlProgress {
  return { id: 0, statement: "", progress: "" };
}

export const DdlProgress = {
  fromJSON(object: any): DdlProgress {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      statement: isSet(object.statement) ? String(object.statement) : "",
      progress: isSet(object.progress) ? String(object.progress) : "",
    };
  },

  toJSON(message: DdlProgress): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.statement !== undefined && (obj.statement = message.statement);
    message.progress !== undefined && (obj.progress = message.progress);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DdlProgress>, I>>(object: I): DdlProgress {
    const message = createBaseDdlProgress();
    message.id = object.id ?? 0;
    message.statement = object.statement ?? "";
    message.progress = object.progress ?? "";
    return message;
  },
};

function createBaseGetDdlProgressResponse(): GetDdlProgressResponse {
  return { ddlProgress: [] };
}

export const GetDdlProgressResponse = {
  fromJSON(object: any): GetDdlProgressResponse {
    return {
      ddlProgress: Array.isArray(object?.ddlProgress)
        ? object.ddlProgress.map((e: any) => DdlProgress.fromJSON(e))
        : [],
    };
  },

  toJSON(message: GetDdlProgressResponse): unknown {
    const obj: any = {};
    if (message.ddlProgress) {
      obj.ddlProgress = message.ddlProgress.map((e) => e ? DdlProgress.toJSON(e) : undefined);
    } else {
      obj.ddlProgress = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetDdlProgressResponse>, I>>(object: I): GetDdlProgressResponse {
    const message = createBaseGetDdlProgressResponse();
    message.ddlProgress = object.ddlProgress?.map((e) => DdlProgress.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCreateConnectionRequest(): CreateConnectionRequest {
  return { payload: undefined };
}

export const CreateConnectionRequest = {
  fromJSON(object: any): CreateConnectionRequest {
    return {
      payload: isSet(object.privateLink)
        ? { $case: "privateLink", privateLink: CreateConnectionRequest_PrivateLink.fromJSON(object.privateLink) }
        : undefined,
    };
  },

  toJSON(message: CreateConnectionRequest): unknown {
    const obj: any = {};
    message.payload?.$case === "privateLink" && (obj.privateLink = message.payload?.privateLink
      ? CreateConnectionRequest_PrivateLink.toJSON(message.payload?.privateLink)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateConnectionRequest>, I>>(object: I): CreateConnectionRequest {
    const message = createBaseCreateConnectionRequest();
    if (
      object.payload?.$case === "privateLink" &&
      object.payload?.privateLink !== undefined &&
      object.payload?.privateLink !== null
    ) {
      message.payload = {
        $case: "privateLink",
        privateLink: CreateConnectionRequest_PrivateLink.fromPartial(object.payload.privateLink),
      };
    }
    return message;
  },
};

function createBaseCreateConnectionRequest_PrivateLink(): CreateConnectionRequest_PrivateLink {
  return { provider: "", serviceName: "", availabilityZones: [] };
}

export const CreateConnectionRequest_PrivateLink = {
  fromJSON(object: any): CreateConnectionRequest_PrivateLink {
    return {
      provider: isSet(object.provider) ? String(object.provider) : "",
      serviceName: isSet(object.serviceName) ? String(object.serviceName) : "",
      availabilityZones: Array.isArray(object?.availabilityZones)
        ? object.availabilityZones.map((e: any) => String(e))
        : [],
    };
  },

  toJSON(message: CreateConnectionRequest_PrivateLink): unknown {
    const obj: any = {};
    message.provider !== undefined && (obj.provider = message.provider);
    message.serviceName !== undefined && (obj.serviceName = message.serviceName);
    if (message.availabilityZones) {
      obj.availabilityZones = message.availabilityZones.map((e) => e);
    } else {
      obj.availabilityZones = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateConnectionRequest_PrivateLink>, I>>(
    object: I,
  ): CreateConnectionRequest_PrivateLink {
    const message = createBaseCreateConnectionRequest_PrivateLink();
    message.provider = object.provider ?? "";
    message.serviceName = object.serviceName ?? "";
    message.availabilityZones = object.availabilityZones?.map((e) => e) || [];
    return message;
  },
};

function createBaseCreateConnectionResponse(): CreateConnectionResponse {
  return { connectionId: 0, version: 0 };
}

export const CreateConnectionResponse = {
  fromJSON(object: any): CreateConnectionResponse {
    return {
      connectionId: isSet(object.connectionId) ? Number(object.connectionId) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateConnectionResponse): unknown {
    const obj: any = {};
    message.connectionId !== undefined && (obj.connectionId = Math.round(message.connectionId));
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateConnectionResponse>, I>>(object: I): CreateConnectionResponse {
    const message = createBaseCreateConnectionResponse();
    message.connectionId = object.connectionId ?? 0;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseListConnectionsRequest(): ListConnectionsRequest {
  return {};
}

export const ListConnectionsRequest = {
  fromJSON(_: any): ListConnectionsRequest {
    return {};
  },

  toJSON(_: ListConnectionsRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListConnectionsRequest>, I>>(_: I): ListConnectionsRequest {
    const message = createBaseListConnectionsRequest();
    return message;
  },
};

function createBaseListConnectionsResponse(): ListConnectionsResponse {
  return { connections: [] };
}

export const ListConnectionsResponse = {
  fromJSON(object: any): ListConnectionsResponse {
    return {
      connections: Array.isArray(object?.connections) ? object.connections.map((e: any) => Connection.fromJSON(e)) : [],
    };
  },

  toJSON(message: ListConnectionsResponse): unknown {
    const obj: any = {};
    if (message.connections) {
      obj.connections = message.connections.map((e) => e ? Connection.toJSON(e) : undefined);
    } else {
      obj.connections = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListConnectionsResponse>, I>>(object: I): ListConnectionsResponse {
    const message = createBaseListConnectionsResponse();
    message.connections = object.connections?.map((e) => Connection.fromPartial(e)) || [];
    return message;
  },
};

function createBaseDropConnectionRequest(): DropConnectionRequest {
  return { connectionName: "" };
}

export const DropConnectionRequest = {
  fromJSON(object: any): DropConnectionRequest {
    return { connectionName: isSet(object.connectionName) ? String(object.connectionName) : "" };
  },

  toJSON(message: DropConnectionRequest): unknown {
    const obj: any = {};
    message.connectionName !== undefined && (obj.connectionName = message.connectionName);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropConnectionRequest>, I>>(object: I): DropConnectionRequest {
    const message = createBaseDropConnectionRequest();
    message.connectionName = object.connectionName ?? "";
    return message;
  },
};

function createBaseDropConnectionResponse(): DropConnectionResponse {
  return {};
}

export const DropConnectionResponse = {
  fromJSON(_: any): DropConnectionResponse {
    return {};
  },

  toJSON(_: DropConnectionResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropConnectionResponse>, I>>(_: I): DropConnectionResponse {
    const message = createBaseDropConnectionResponse();
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
