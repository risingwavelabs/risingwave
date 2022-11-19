/* eslint-disable */

export const protobufPackage = "cdc_service";

/** Notes: This proto needs to be self-contained */
export interface Status {
  code: Status_Code;
  message: string;
}

export const Status_Code = { UNSPECIFIED: "UNSPECIFIED", OK: "OK", UNRECOGNIZED: "UNRECOGNIZED" } as const;

export type Status_Code = typeof Status_Code[keyof typeof Status_Code];

export function status_CodeFromJSON(object: any): Status_Code {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return Status_Code.UNSPECIFIED;
    case 1:
    case "OK":
      return Status_Code.OK;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Status_Code.UNRECOGNIZED;
  }
}

export function status_CodeToJSON(object: Status_Code): string {
  switch (object) {
    case Status_Code.UNSPECIFIED:
      return "UNSPECIFIED";
    case Status_Code.OK:
      return "OK";
    case Status_Code.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
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

function createBaseStatus(): Status {
  return { code: Status_Code.UNSPECIFIED, message: "" };
}

export const Status = {
  fromJSON(object: any): Status {
    return {
      code: isSet(object.code) ? status_CodeFromJSON(object.code) : Status_Code.UNSPECIFIED,
      message: isSet(object.message) ? String(object.message) : "",
    };
  },

  toJSON(message: Status): unknown {
    const obj: any = {};
    message.code !== undefined && (obj.code = status_CodeToJSON(message.code));
    message.message !== undefined && (obj.message = message.message);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Status>, I>>(object: I): Status {
    const message = createBaseStatus();
    message.code = object.code ?? Status_Code.UNSPECIFIED;
    message.message = object.message ?? "";
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
