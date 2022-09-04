/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Status } from "./common";

export const protobufPackage = "user";

/** AuthInfo is the information required to login to a server. */
export interface AuthInfo {
  encryptionType: AuthInfo_EncryptionType;
  encryptedValue: Uint8Array;
}

export enum AuthInfo_EncryptionType {
  UNSPECIFIED = 0,
  UNKNOWN = 1,
  PLAINTEXT = 2,
  SHA256 = 3,
  MD5 = 4,
  UNRECOGNIZED = -1,
}

export function authInfo_EncryptionTypeFromJSON(object: any): AuthInfo_EncryptionType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return AuthInfo_EncryptionType.UNSPECIFIED;
    case 1:
    case "UNKNOWN":
      return AuthInfo_EncryptionType.UNKNOWN;
    case 2:
    case "PLAINTEXT":
      return AuthInfo_EncryptionType.PLAINTEXT;
    case 3:
    case "SHA256":
      return AuthInfo_EncryptionType.SHA256;
    case 4:
    case "MD5":
      return AuthInfo_EncryptionType.MD5;
    case -1:
    case "UNRECOGNIZED":
    default:
      return AuthInfo_EncryptionType.UNRECOGNIZED;
  }
}

export function authInfo_EncryptionTypeToJSON(object: AuthInfo_EncryptionType): string {
  switch (object) {
    case AuthInfo_EncryptionType.UNSPECIFIED:
      return "UNSPECIFIED";
    case AuthInfo_EncryptionType.UNKNOWN:
      return "UNKNOWN";
    case AuthInfo_EncryptionType.PLAINTEXT:
      return "PLAINTEXT";
    case AuthInfo_EncryptionType.SHA256:
      return "SHA256";
    case AuthInfo_EncryptionType.MD5:
      return "MD5";
    case AuthInfo_EncryptionType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

/** User defines a user in the system. */
export interface UserInfo {
  id: number;
  name: string;
  isSuper: boolean;
  canCreateDb: boolean;
  canCreateUser: boolean;
  canLogin: boolean;
  authInfo:
    | AuthInfo
    | undefined;
  /** / Granted privileges will be only updated through the command of GRANT/REVOKE. */
  grantPrivileges: GrantPrivilege[];
}

/** GrantPrivilege defines a privilege granted to a user. */
export interface GrantPrivilege {
  databaseId: number | undefined;
  schemaId: number | undefined;
  tableId: number | undefined;
  sourceId: number | undefined;
  allTablesSchemaId: number | undefined;
  allSourcesSchemaId: number | undefined;
  actionWithOpts: GrantPrivilege_ActionWithGrantOption[];
}

export enum GrantPrivilege_Action {
  UNSPECIFIED = 0,
  UNKNOWN = 1,
  SELECT = 2,
  INSERT = 3,
  UPDATE = 4,
  DELETE = 5,
  CREATE = 6,
  CONNECT = 7,
  UNRECOGNIZED = -1,
}

export function grantPrivilege_ActionFromJSON(object: any): GrantPrivilege_Action {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return GrantPrivilege_Action.UNSPECIFIED;
    case 1:
    case "UNKNOWN":
      return GrantPrivilege_Action.UNKNOWN;
    case 2:
    case "SELECT":
      return GrantPrivilege_Action.SELECT;
    case 3:
    case "INSERT":
      return GrantPrivilege_Action.INSERT;
    case 4:
    case "UPDATE":
      return GrantPrivilege_Action.UPDATE;
    case 5:
    case "DELETE":
      return GrantPrivilege_Action.DELETE;
    case 6:
    case "CREATE":
      return GrantPrivilege_Action.CREATE;
    case 7:
    case "CONNECT":
      return GrantPrivilege_Action.CONNECT;
    case -1:
    case "UNRECOGNIZED":
    default:
      return GrantPrivilege_Action.UNRECOGNIZED;
  }
}

export function grantPrivilege_ActionToJSON(object: GrantPrivilege_Action): string {
  switch (object) {
    case GrantPrivilege_Action.UNSPECIFIED:
      return "UNSPECIFIED";
    case GrantPrivilege_Action.UNKNOWN:
      return "UNKNOWN";
    case GrantPrivilege_Action.SELECT:
      return "SELECT";
    case GrantPrivilege_Action.INSERT:
      return "INSERT";
    case GrantPrivilege_Action.UPDATE:
      return "UPDATE";
    case GrantPrivilege_Action.DELETE:
      return "DELETE";
    case GrantPrivilege_Action.CREATE:
      return "CREATE";
    case GrantPrivilege_Action.CONNECT:
      return "CONNECT";
    case GrantPrivilege_Action.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface GrantPrivilege_ActionWithGrantOption {
  action: GrantPrivilege_Action;
  withGrantOption: boolean;
  grantedBy: number;
}

export interface CreateUserRequest {
  user: UserInfo | undefined;
}

export interface CreateUserResponse {
  status: Status | undefined;
  version: number;
}

export interface DropUserRequest {
  userId: number;
}

export interface DropUserResponse {
  status: Status | undefined;
  version: number;
}

export interface UpdateUserRequest {
  user: UserInfo | undefined;
  updateFields: UpdateUserRequest_UpdateField[];
}

export enum UpdateUserRequest_UpdateField {
  UNKNOWN = 0,
  SUPER = 1,
  LOGIN = 2,
  CREATE_DB = 3,
  AUTH_INFO = 4,
  RENAME = 5,
  CREATE_USER = 6,
  UNRECOGNIZED = -1,
}

export function updateUserRequest_UpdateFieldFromJSON(object: any): UpdateUserRequest_UpdateField {
  switch (object) {
    case 0:
    case "UNKNOWN":
      return UpdateUserRequest_UpdateField.UNKNOWN;
    case 1:
    case "SUPER":
      return UpdateUserRequest_UpdateField.SUPER;
    case 2:
    case "LOGIN":
      return UpdateUserRequest_UpdateField.LOGIN;
    case 3:
    case "CREATE_DB":
      return UpdateUserRequest_UpdateField.CREATE_DB;
    case 4:
    case "AUTH_INFO":
      return UpdateUserRequest_UpdateField.AUTH_INFO;
    case 5:
    case "RENAME":
      return UpdateUserRequest_UpdateField.RENAME;
    case 6:
    case "CREATE_USER":
      return UpdateUserRequest_UpdateField.CREATE_USER;
    case -1:
    case "UNRECOGNIZED":
    default:
      return UpdateUserRequest_UpdateField.UNRECOGNIZED;
  }
}

export function updateUserRequest_UpdateFieldToJSON(object: UpdateUserRequest_UpdateField): string {
  switch (object) {
    case UpdateUserRequest_UpdateField.UNKNOWN:
      return "UNKNOWN";
    case UpdateUserRequest_UpdateField.SUPER:
      return "SUPER";
    case UpdateUserRequest_UpdateField.LOGIN:
      return "LOGIN";
    case UpdateUserRequest_UpdateField.CREATE_DB:
      return "CREATE_DB";
    case UpdateUserRequest_UpdateField.AUTH_INFO:
      return "AUTH_INFO";
    case UpdateUserRequest_UpdateField.RENAME:
      return "RENAME";
    case UpdateUserRequest_UpdateField.CREATE_USER:
      return "CREATE_USER";
    case UpdateUserRequest_UpdateField.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface UpdateUserResponse {
  status: Status | undefined;
  version: number;
}

export interface GrantPrivilegeRequest {
  userIds: number[];
  privileges: GrantPrivilege[];
  withGrantOption: boolean;
  grantedBy: number;
}

export interface GrantPrivilegeResponse {
  status: Status | undefined;
  version: number;
}

export interface RevokePrivilegeRequest {
  userIds: number[];
  privileges: GrantPrivilege[];
  grantedBy: number;
  revokeBy: number;
  revokeGrantOption: boolean;
  cascade: boolean;
}

export interface RevokePrivilegeResponse {
  status: Status | undefined;
  version: number;
}

function createBaseAuthInfo(): AuthInfo {
  return { encryptionType: 0, encryptedValue: new Uint8Array() };
}

export const AuthInfo = {
  encode(message: AuthInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.encryptionType !== 0) {
      writer.uint32(8).int32(message.encryptionType);
    }
    if (message.encryptedValue.length !== 0) {
      writer.uint32(18).bytes(message.encryptedValue);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AuthInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAuthInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.encryptionType = reader.int32() as any;
          break;
        case 2:
          message.encryptedValue = reader.bytes();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AuthInfo {
    return {
      encryptionType: isSet(object.encryptionType) ? authInfo_EncryptionTypeFromJSON(object.encryptionType) : 0,
      encryptedValue: isSet(object.encryptedValue) ? bytesFromBase64(object.encryptedValue) : new Uint8Array(),
    };
  },

  toJSON(message: AuthInfo): unknown {
    const obj: any = {};
    message.encryptionType !== undefined &&
      (obj.encryptionType = authInfo_EncryptionTypeToJSON(message.encryptionType));
    message.encryptedValue !== undefined &&
      (obj.encryptedValue = base64FromBytes(
        message.encryptedValue !== undefined ? message.encryptedValue : new Uint8Array(),
      ));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AuthInfo>, I>>(object: I): AuthInfo {
    const message = createBaseAuthInfo();
    message.encryptionType = object.encryptionType ?? 0;
    message.encryptedValue = object.encryptedValue ?? new Uint8Array();
    return message;
  },
};

function createBaseUserInfo(): UserInfo {
  return {
    id: 0,
    name: "",
    isSuper: false,
    canCreateDb: false,
    canCreateUser: false,
    canLogin: false,
    authInfo: undefined,
    grantPrivileges: [],
  };
}

export const UserInfo = {
  encode(message: UserInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.name !== "") {
      writer.uint32(18).string(message.name);
    }
    if (message.isSuper === true) {
      writer.uint32(24).bool(message.isSuper);
    }
    if (message.canCreateDb === true) {
      writer.uint32(32).bool(message.canCreateDb);
    }
    if (message.canCreateUser === true) {
      writer.uint32(40).bool(message.canCreateUser);
    }
    if (message.canLogin === true) {
      writer.uint32(48).bool(message.canLogin);
    }
    if (message.authInfo !== undefined) {
      AuthInfo.encode(message.authInfo, writer.uint32(58).fork()).ldelim();
    }
    for (const v of message.grantPrivileges) {
      GrantPrivilege.encode(v!, writer.uint32(66).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UserInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUserInfo();
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
          message.isSuper = reader.bool();
          break;
        case 4:
          message.canCreateDb = reader.bool();
          break;
        case 5:
          message.canCreateUser = reader.bool();
          break;
        case 6:
          message.canLogin = reader.bool();
          break;
        case 7:
          message.authInfo = AuthInfo.decode(reader, reader.uint32());
          break;
        case 8:
          message.grantPrivileges.push(GrantPrivilege.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UserInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      isSuper: isSet(object.isSuper) ? Boolean(object.isSuper) : false,
      canCreateDb: isSet(object.canCreateDb) ? Boolean(object.canCreateDb) : false,
      canCreateUser: isSet(object.canCreateUser) ? Boolean(object.canCreateUser) : false,
      canLogin: isSet(object.canLogin) ? Boolean(object.canLogin) : false,
      authInfo: isSet(object.authInfo) ? AuthInfo.fromJSON(object.authInfo) : undefined,
      grantPrivileges: Array.isArray(object?.grantPrivileges)
        ? object.grantPrivileges.map((e: any) => GrantPrivilege.fromJSON(e))
        : [],
    };
  },

  toJSON(message: UserInfo): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.name !== undefined && (obj.name = message.name);
    message.isSuper !== undefined && (obj.isSuper = message.isSuper);
    message.canCreateDb !== undefined && (obj.canCreateDb = message.canCreateDb);
    message.canCreateUser !== undefined && (obj.canCreateUser = message.canCreateUser);
    message.canLogin !== undefined && (obj.canLogin = message.canLogin);
    message.authInfo !== undefined && (obj.authInfo = message.authInfo ? AuthInfo.toJSON(message.authInfo) : undefined);
    if (message.grantPrivileges) {
      obj.grantPrivileges = message.grantPrivileges.map((e) => e ? GrantPrivilege.toJSON(e) : undefined);
    } else {
      obj.grantPrivileges = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UserInfo>, I>>(object: I): UserInfo {
    const message = createBaseUserInfo();
    message.id = object.id ?? 0;
    message.name = object.name ?? "";
    message.isSuper = object.isSuper ?? false;
    message.canCreateDb = object.canCreateDb ?? false;
    message.canCreateUser = object.canCreateUser ?? false;
    message.canLogin = object.canLogin ?? false;
    message.authInfo = (object.authInfo !== undefined && object.authInfo !== null)
      ? AuthInfo.fromPartial(object.authInfo)
      : undefined;
    message.grantPrivileges = object.grantPrivileges?.map((e) => GrantPrivilege.fromPartial(e)) || [];
    return message;
  },
};

function createBaseGrantPrivilege(): GrantPrivilege {
  return {
    databaseId: undefined,
    schemaId: undefined,
    tableId: undefined,
    sourceId: undefined,
    allTablesSchemaId: undefined,
    allSourcesSchemaId: undefined,
    actionWithOpts: [],
  };
}

export const GrantPrivilege = {
  encode(message: GrantPrivilege, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.databaseId !== undefined) {
      writer.uint32(8).uint32(message.databaseId);
    }
    if (message.schemaId !== undefined) {
      writer.uint32(16).uint32(message.schemaId);
    }
    if (message.tableId !== undefined) {
      writer.uint32(24).uint32(message.tableId);
    }
    if (message.sourceId !== undefined) {
      writer.uint32(32).uint32(message.sourceId);
    }
    if (message.allTablesSchemaId !== undefined) {
      writer.uint32(40).uint32(message.allTablesSchemaId);
    }
    if (message.allSourcesSchemaId !== undefined) {
      writer.uint32(48).uint32(message.allSourcesSchemaId);
    }
    for (const v of message.actionWithOpts) {
      GrantPrivilege_ActionWithGrantOption.encode(v!, writer.uint32(58).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GrantPrivilege {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGrantPrivilege();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.databaseId = reader.uint32();
          break;
        case 2:
          message.schemaId = reader.uint32();
          break;
        case 3:
          message.tableId = reader.uint32();
          break;
        case 4:
          message.sourceId = reader.uint32();
          break;
        case 5:
          message.allTablesSchemaId = reader.uint32();
          break;
        case 6:
          message.allSourcesSchemaId = reader.uint32();
          break;
        case 7:
          message.actionWithOpts.push(GrantPrivilege_ActionWithGrantOption.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GrantPrivilege {
    return {
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : undefined,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : undefined,
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : undefined,
      allTablesSchemaId: isSet(object.allTablesSchemaId) ? Number(object.allTablesSchemaId) : undefined,
      allSourcesSchemaId: isSet(object.allSourcesSchemaId) ? Number(object.allSourcesSchemaId) : undefined,
      actionWithOpts: Array.isArray(object?.actionWithOpts)
        ? object.actionWithOpts.map((e: any) => GrantPrivilege_ActionWithGrantOption.fromJSON(e))
        : [],
    };
  },

  toJSON(message: GrantPrivilege): unknown {
    const obj: any = {};
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.allTablesSchemaId !== undefined && (obj.allTablesSchemaId = Math.round(message.allTablesSchemaId));
    message.allSourcesSchemaId !== undefined && (obj.allSourcesSchemaId = Math.round(message.allSourcesSchemaId));
    if (message.actionWithOpts) {
      obj.actionWithOpts = message.actionWithOpts.map((e) =>
        e ? GrantPrivilege_ActionWithGrantOption.toJSON(e) : undefined
      );
    } else {
      obj.actionWithOpts = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GrantPrivilege>, I>>(object: I): GrantPrivilege {
    const message = createBaseGrantPrivilege();
    message.databaseId = object.databaseId ?? undefined;
    message.schemaId = object.schemaId ?? undefined;
    message.tableId = object.tableId ?? undefined;
    message.sourceId = object.sourceId ?? undefined;
    message.allTablesSchemaId = object.allTablesSchemaId ?? undefined;
    message.allSourcesSchemaId = object.allSourcesSchemaId ?? undefined;
    message.actionWithOpts = object.actionWithOpts?.map((e) => GrantPrivilege_ActionWithGrantOption.fromPartial(e)) ||
      [];
    return message;
  },
};

function createBaseGrantPrivilege_ActionWithGrantOption(): GrantPrivilege_ActionWithGrantOption {
  return { action: 0, withGrantOption: false, grantedBy: 0 };
}

export const GrantPrivilege_ActionWithGrantOption = {
  encode(message: GrantPrivilege_ActionWithGrantOption, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.action !== 0) {
      writer.uint32(8).int32(message.action);
    }
    if (message.withGrantOption === true) {
      writer.uint32(16).bool(message.withGrantOption);
    }
    if (message.grantedBy !== 0) {
      writer.uint32(24).uint32(message.grantedBy);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GrantPrivilege_ActionWithGrantOption {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGrantPrivilege_ActionWithGrantOption();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.action = reader.int32() as any;
          break;
        case 2:
          message.withGrantOption = reader.bool();
          break;
        case 3:
          message.grantedBy = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GrantPrivilege_ActionWithGrantOption {
    return {
      action: isSet(object.action) ? grantPrivilege_ActionFromJSON(object.action) : 0,
      withGrantOption: isSet(object.withGrantOption) ? Boolean(object.withGrantOption) : false,
      grantedBy: isSet(object.grantedBy) ? Number(object.grantedBy) : 0,
    };
  },

  toJSON(message: GrantPrivilege_ActionWithGrantOption): unknown {
    const obj: any = {};
    message.action !== undefined && (obj.action = grantPrivilege_ActionToJSON(message.action));
    message.withGrantOption !== undefined && (obj.withGrantOption = message.withGrantOption);
    message.grantedBy !== undefined && (obj.grantedBy = Math.round(message.grantedBy));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GrantPrivilege_ActionWithGrantOption>, I>>(
    object: I,
  ): GrantPrivilege_ActionWithGrantOption {
    const message = createBaseGrantPrivilege_ActionWithGrantOption();
    message.action = object.action ?? 0;
    message.withGrantOption = object.withGrantOption ?? false;
    message.grantedBy = object.grantedBy ?? 0;
    return message;
  },
};

function createBaseCreateUserRequest(): CreateUserRequest {
  return { user: undefined };
}

export const CreateUserRequest = {
  encode(message: CreateUserRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.user !== undefined) {
      UserInfo.encode(message.user, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateUserRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateUserRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.user = UserInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateUserRequest {
    return { user: isSet(object.user) ? UserInfo.fromJSON(object.user) : undefined };
  },

  toJSON(message: CreateUserRequest): unknown {
    const obj: any = {};
    message.user !== undefined && (obj.user = message.user ? UserInfo.toJSON(message.user) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateUserRequest>, I>>(object: I): CreateUserRequest {
    const message = createBaseCreateUserRequest();
    message.user = (object.user !== undefined && object.user !== null) ? UserInfo.fromPartial(object.user) : undefined;
    return message;
  },
};

function createBaseCreateUserResponse(): CreateUserResponse {
  return { status: undefined, version: 0 };
}

export const CreateUserResponse = {
  encode(message: CreateUserResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateUserResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateUserResponse();
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

  fromJSON(object: any): CreateUserResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: CreateUserResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateUserResponse>, I>>(object: I): CreateUserResponse {
    const message = createBaseCreateUserResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseDropUserRequest(): DropUserRequest {
  return { userId: 0 };
}

export const DropUserRequest = {
  encode(message: DropUserRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.userId !== 0) {
      writer.uint32(8).uint32(message.userId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropUserRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropUserRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.userId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropUserRequest {
    return { userId: isSet(object.userId) ? Number(object.userId) : 0 };
  },

  toJSON(message: DropUserRequest): unknown {
    const obj: any = {};
    message.userId !== undefined && (obj.userId = Math.round(message.userId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropUserRequest>, I>>(object: I): DropUserRequest {
    const message = createBaseDropUserRequest();
    message.userId = object.userId ?? 0;
    return message;
  },
};

function createBaseDropUserResponse(): DropUserResponse {
  return { status: undefined, version: 0 };
}

export const DropUserResponse = {
  encode(message: DropUserResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropUserResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropUserResponse();
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

  fromJSON(object: any): DropUserResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: DropUserResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropUserResponse>, I>>(object: I): DropUserResponse {
    const message = createBaseDropUserResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseUpdateUserRequest(): UpdateUserRequest {
  return { user: undefined, updateFields: [] };
}

export const UpdateUserRequest = {
  encode(message: UpdateUserRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.user !== undefined) {
      UserInfo.encode(message.user, writer.uint32(10).fork()).ldelim();
    }
    writer.uint32(18).fork();
    for (const v of message.updateFields) {
      writer.int32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UpdateUserRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUpdateUserRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.user = UserInfo.decode(reader, reader.uint32());
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.updateFields.push(reader.int32() as any);
            }
          } else {
            message.updateFields.push(reader.int32() as any);
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UpdateUserRequest {
    return {
      user: isSet(object.user) ? UserInfo.fromJSON(object.user) : undefined,
      updateFields: Array.isArray(object?.updateFields)
        ? object.updateFields.map((e: any) => updateUserRequest_UpdateFieldFromJSON(e))
        : [],
    };
  },

  toJSON(message: UpdateUserRequest): unknown {
    const obj: any = {};
    message.user !== undefined && (obj.user = message.user ? UserInfo.toJSON(message.user) : undefined);
    if (message.updateFields) {
      obj.updateFields = message.updateFields.map((e) => updateUserRequest_UpdateFieldToJSON(e));
    } else {
      obj.updateFields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateUserRequest>, I>>(object: I): UpdateUserRequest {
    const message = createBaseUpdateUserRequest();
    message.user = (object.user !== undefined && object.user !== null) ? UserInfo.fromPartial(object.user) : undefined;
    message.updateFields = object.updateFields?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateUserResponse(): UpdateUserResponse {
  return { status: undefined, version: 0 };
}

export const UpdateUserResponse = {
  encode(message: UpdateUserResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UpdateUserResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUpdateUserResponse();
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

  fromJSON(object: any): UpdateUserResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: UpdateUserResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateUserResponse>, I>>(object: I): UpdateUserResponse {
    const message = createBaseUpdateUserResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseGrantPrivilegeRequest(): GrantPrivilegeRequest {
  return { userIds: [], privileges: [], withGrantOption: false, grantedBy: 0 };
}

export const GrantPrivilegeRequest = {
  encode(message: GrantPrivilegeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.userIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    for (const v of message.privileges) {
      GrantPrivilege.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.withGrantOption === true) {
      writer.uint32(24).bool(message.withGrantOption);
    }
    if (message.grantedBy !== 0) {
      writer.uint32(32).uint32(message.grantedBy);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GrantPrivilegeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGrantPrivilegeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.userIds.push(reader.uint32());
            }
          } else {
            message.userIds.push(reader.uint32());
          }
          break;
        case 2:
          message.privileges.push(GrantPrivilege.decode(reader, reader.uint32()));
          break;
        case 3:
          message.withGrantOption = reader.bool();
          break;
        case 4:
          message.grantedBy = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GrantPrivilegeRequest {
    return {
      userIds: Array.isArray(object?.userIds) ? object.userIds.map((e: any) => Number(e)) : [],
      privileges: Array.isArray(object?.privileges)
        ? object.privileges.map((e: any) => GrantPrivilege.fromJSON(e))
        : [],
      withGrantOption: isSet(object.withGrantOption) ? Boolean(object.withGrantOption) : false,
      grantedBy: isSet(object.grantedBy) ? Number(object.grantedBy) : 0,
    };
  },

  toJSON(message: GrantPrivilegeRequest): unknown {
    const obj: any = {};
    if (message.userIds) {
      obj.userIds = message.userIds.map((e) => Math.round(e));
    } else {
      obj.userIds = [];
    }
    if (message.privileges) {
      obj.privileges = message.privileges.map((e) => e ? GrantPrivilege.toJSON(e) : undefined);
    } else {
      obj.privileges = [];
    }
    message.withGrantOption !== undefined && (obj.withGrantOption = message.withGrantOption);
    message.grantedBy !== undefined && (obj.grantedBy = Math.round(message.grantedBy));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GrantPrivilegeRequest>, I>>(object: I): GrantPrivilegeRequest {
    const message = createBaseGrantPrivilegeRequest();
    message.userIds = object.userIds?.map((e) => e) || [];
    message.privileges = object.privileges?.map((e) => GrantPrivilege.fromPartial(e)) || [];
    message.withGrantOption = object.withGrantOption ?? false;
    message.grantedBy = object.grantedBy ?? 0;
    return message;
  },
};

function createBaseGrantPrivilegeResponse(): GrantPrivilegeResponse {
  return { status: undefined, version: 0 };
}

export const GrantPrivilegeResponse = {
  encode(message: GrantPrivilegeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GrantPrivilegeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGrantPrivilegeResponse();
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

  fromJSON(object: any): GrantPrivilegeResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: GrantPrivilegeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GrantPrivilegeResponse>, I>>(object: I): GrantPrivilegeResponse {
    const message = createBaseGrantPrivilegeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.version = object.version ?? 0;
    return message;
  },
};

function createBaseRevokePrivilegeRequest(): RevokePrivilegeRequest {
  return { userIds: [], privileges: [], grantedBy: 0, revokeBy: 0, revokeGrantOption: false, cascade: false };
}

export const RevokePrivilegeRequest = {
  encode(message: RevokePrivilegeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.userIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    for (const v of message.privileges) {
      GrantPrivilege.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.grantedBy !== 0) {
      writer.uint32(24).uint32(message.grantedBy);
    }
    if (message.revokeBy !== 0) {
      writer.uint32(32).uint32(message.revokeBy);
    }
    if (message.revokeGrantOption === true) {
      writer.uint32(40).bool(message.revokeGrantOption);
    }
    if (message.cascade === true) {
      writer.uint32(48).bool(message.cascade);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RevokePrivilegeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRevokePrivilegeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.userIds.push(reader.uint32());
            }
          } else {
            message.userIds.push(reader.uint32());
          }
          break;
        case 2:
          message.privileges.push(GrantPrivilege.decode(reader, reader.uint32()));
          break;
        case 3:
          message.grantedBy = reader.uint32();
          break;
        case 4:
          message.revokeBy = reader.uint32();
          break;
        case 5:
          message.revokeGrantOption = reader.bool();
          break;
        case 6:
          message.cascade = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): RevokePrivilegeRequest {
    return {
      userIds: Array.isArray(object?.userIds) ? object.userIds.map((e: any) => Number(e)) : [],
      privileges: Array.isArray(object?.privileges)
        ? object.privileges.map((e: any) => GrantPrivilege.fromJSON(e))
        : [],
      grantedBy: isSet(object.grantedBy) ? Number(object.grantedBy) : 0,
      revokeBy: isSet(object.revokeBy) ? Number(object.revokeBy) : 0,
      revokeGrantOption: isSet(object.revokeGrantOption) ? Boolean(object.revokeGrantOption) : false,
      cascade: isSet(object.cascade) ? Boolean(object.cascade) : false,
    };
  },

  toJSON(message: RevokePrivilegeRequest): unknown {
    const obj: any = {};
    if (message.userIds) {
      obj.userIds = message.userIds.map((e) => Math.round(e));
    } else {
      obj.userIds = [];
    }
    if (message.privileges) {
      obj.privileges = message.privileges.map((e) => e ? GrantPrivilege.toJSON(e) : undefined);
    } else {
      obj.privileges = [];
    }
    message.grantedBy !== undefined && (obj.grantedBy = Math.round(message.grantedBy));
    message.revokeBy !== undefined && (obj.revokeBy = Math.round(message.revokeBy));
    message.revokeGrantOption !== undefined && (obj.revokeGrantOption = message.revokeGrantOption);
    message.cascade !== undefined && (obj.cascade = message.cascade);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RevokePrivilegeRequest>, I>>(object: I): RevokePrivilegeRequest {
    const message = createBaseRevokePrivilegeRequest();
    message.userIds = object.userIds?.map((e) => e) || [];
    message.privileges = object.privileges?.map((e) => GrantPrivilege.fromPartial(e)) || [];
    message.grantedBy = object.grantedBy ?? 0;
    message.revokeBy = object.revokeBy ?? 0;
    message.revokeGrantOption = object.revokeGrantOption ?? false;
    message.cascade = object.cascade ?? false;
    return message;
  },
};

function createBaseRevokePrivilegeResponse(): RevokePrivilegeResponse {
  return { status: undefined, version: 0 };
}

export const RevokePrivilegeResponse = {
  encode(message: RevokePrivilegeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.version !== 0) {
      writer.uint32(16).uint64(message.version);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RevokePrivilegeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRevokePrivilegeResponse();
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

  fromJSON(object: any): RevokePrivilegeResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      version: isSet(object.version) ? Number(object.version) : 0,
    };
  },

  toJSON(message: RevokePrivilegeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.version !== undefined && (obj.version = Math.round(message.version));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RevokePrivilegeResponse>, I>>(object: I): RevokePrivilegeResponse {
    const message = createBaseRevokePrivilegeResponse();
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

function bytesFromBase64(b64: string): Uint8Array {
  if (globalThis.Buffer) {
    return Uint8Array.from(globalThis.Buffer.from(b64, "base64"));
  } else {
    const bin = globalThis.atob(b64);
    const arr = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; ++i) {
      arr[i] = bin.charCodeAt(i);
    }
    return arr;
  }
}

function base64FromBytes(arr: Uint8Array): string {
  if (globalThis.Buffer) {
    return globalThis.Buffer.from(arr).toString("base64");
  } else {
    const bin: string[] = [];
    arr.forEach((byte) => {
      bin.push(String.fromCharCode(byte));
    });
    return globalThis.btoa(bin.join(""));
  }
}

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
