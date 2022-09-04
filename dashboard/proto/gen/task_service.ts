/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { PlanFragment, TaskId as TaskId1, TaskOutputId } from "./batch_plan";
import { Status } from "./common";
import { DataChunk } from "./data";
import { StreamMessage } from "./stream_plan";

export const protobufPackage = "task_service";

/** Task is a running instance of Stage. */
export interface TaskId {
  queryId: string;
  stageId: number;
  taskId: number;
}

export interface TaskInfo {
  taskId: TaskId1 | undefined;
  taskStatus: TaskInfo_TaskStatus;
}

export enum TaskInfo_TaskStatus {
  /** UNSPECIFIED - Note: Requirement of proto3: first enum must be 0. */
  UNSPECIFIED = 0,
  PENDING = 2,
  RUNNING = 3,
  FINISHED = 6,
  FAILED = 7,
  ABORTED = 8,
  UNRECOGNIZED = -1,
}

export function taskInfo_TaskStatusFromJSON(object: any): TaskInfo_TaskStatus {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TaskInfo_TaskStatus.UNSPECIFIED;
    case 2:
    case "PENDING":
      return TaskInfo_TaskStatus.PENDING;
    case 3:
    case "RUNNING":
      return TaskInfo_TaskStatus.RUNNING;
    case 6:
    case "FINISHED":
      return TaskInfo_TaskStatus.FINISHED;
    case 7:
    case "FAILED":
      return TaskInfo_TaskStatus.FAILED;
    case 8:
    case "ABORTED":
      return TaskInfo_TaskStatus.ABORTED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TaskInfo_TaskStatus.UNRECOGNIZED;
  }
}

export function taskInfo_TaskStatusToJSON(object: TaskInfo_TaskStatus): string {
  switch (object) {
    case TaskInfo_TaskStatus.UNSPECIFIED:
      return "UNSPECIFIED";
    case TaskInfo_TaskStatus.PENDING:
      return "PENDING";
    case TaskInfo_TaskStatus.RUNNING:
      return "RUNNING";
    case TaskInfo_TaskStatus.FINISHED:
      return "FINISHED";
    case TaskInfo_TaskStatus.FAILED:
      return "FAILED";
    case TaskInfo_TaskStatus.ABORTED:
      return "ABORTED";
    case TaskInfo_TaskStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface CreateTaskRequest {
  taskId: TaskId1 | undefined;
  plan: PlanFragment | undefined;
  epoch: number;
}

export interface AbortTaskRequest {
  taskId: TaskId1 | undefined;
}

export interface AbortTaskResponse {
  status: Status | undefined;
}

export interface GetTaskInfoRequest {
  taskId: TaskId1 | undefined;
}

export interface TaskInfoResponse {
  status: Status | undefined;
  taskInfo: TaskInfo | undefined;
}

export interface GetDataResponse {
  status: Status | undefined;
  recordBatch: DataChunk | undefined;
}

export interface GetStreamRequest {
  upActorId: number;
  downActorId: number;
  upFragmentId: number;
  downFragmentId: number;
}

export interface ExecuteRequest {
  taskId: TaskId1 | undefined;
  plan: PlanFragment | undefined;
  epoch: number;
}

export interface GetDataRequest {
  taskOutputId: TaskOutputId | undefined;
}

export interface GetStreamResponse {
  message: StreamMessage | undefined;
}

function createBaseTaskId(): TaskId {
  return { queryId: "", stageId: 0, taskId: 0 };
}

export const TaskId = {
  encode(message: TaskId, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.queryId !== "") {
      writer.uint32(10).string(message.queryId);
    }
    if (message.stageId !== 0) {
      writer.uint32(16).uint32(message.stageId);
    }
    if (message.taskId !== 0) {
      writer.uint32(24).uint32(message.taskId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TaskId {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTaskId();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.queryId = reader.string();
          break;
        case 2:
          message.stageId = reader.uint32();
          break;
        case 3:
          message.taskId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TaskId {
    return {
      queryId: isSet(object.queryId) ? String(object.queryId) : "",
      stageId: isSet(object.stageId) ? Number(object.stageId) : 0,
      taskId: isSet(object.taskId) ? Number(object.taskId) : 0,
    };
  },

  toJSON(message: TaskId): unknown {
    const obj: any = {};
    message.queryId !== undefined && (obj.queryId = message.queryId);
    message.stageId !== undefined && (obj.stageId = Math.round(message.stageId));
    message.taskId !== undefined && (obj.taskId = Math.round(message.taskId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TaskId>, I>>(object: I): TaskId {
    const message = createBaseTaskId();
    message.queryId = object.queryId ?? "";
    message.stageId = object.stageId ?? 0;
    message.taskId = object.taskId ?? 0;
    return message;
  },
};

function createBaseTaskInfo(): TaskInfo {
  return { taskId: undefined, taskStatus: 0 };
}

export const TaskInfo = {
  encode(message: TaskInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId1.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    if (message.taskStatus !== 0) {
      writer.uint32(16).int32(message.taskStatus);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TaskInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTaskInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId1.decode(reader, reader.uint32());
          break;
        case 2:
          message.taskStatus = reader.int32() as any;
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TaskInfo {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      taskStatus: isSet(object.taskStatus) ? taskInfo_TaskStatusFromJSON(object.taskStatus) : 0,
    };
  },

  toJSON(message: TaskInfo): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.taskStatus !== undefined && (obj.taskStatus = taskInfo_TaskStatusToJSON(message.taskStatus));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TaskInfo>, I>>(object: I): TaskInfo {
    const message = createBaseTaskInfo();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    message.taskStatus = object.taskStatus ?? 0;
    return message;
  },
};

function createBaseCreateTaskRequest(): CreateTaskRequest {
  return { taskId: undefined, plan: undefined, epoch: 0 };
}

export const CreateTaskRequest = {
  encode(message: CreateTaskRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId1.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    if (message.plan !== undefined) {
      PlanFragment.encode(message.plan, writer.uint32(18).fork()).ldelim();
    }
    if (message.epoch !== 0) {
      writer.uint32(24).uint64(message.epoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CreateTaskRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCreateTaskRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId1.decode(reader, reader.uint32());
          break;
        case 2:
          message.plan = PlanFragment.decode(reader, reader.uint32());
          break;
        case 3:
          message.epoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateTaskRequest {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      plan: isSet(object.plan) ? PlanFragment.fromJSON(object.plan) : undefined,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: CreateTaskRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.plan !== undefined && (obj.plan = message.plan ? PlanFragment.toJSON(message.plan) : undefined);
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateTaskRequest>, I>>(object: I): CreateTaskRequest {
    const message = createBaseCreateTaskRequest();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    message.plan = (object.plan !== undefined && object.plan !== null)
      ? PlanFragment.fromPartial(object.plan)
      : undefined;
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseAbortTaskRequest(): AbortTaskRequest {
  return { taskId: undefined };
}

export const AbortTaskRequest = {
  encode(message: AbortTaskRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId1.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AbortTaskRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAbortTaskRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId1.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AbortTaskRequest {
    return { taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined };
  },

  toJSON(message: AbortTaskRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AbortTaskRequest>, I>>(object: I): AbortTaskRequest {
    const message = createBaseAbortTaskRequest();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    return message;
  },
};

function createBaseAbortTaskResponse(): AbortTaskResponse {
  return { status: undefined };
}

export const AbortTaskResponse = {
  encode(message: AbortTaskResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AbortTaskResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAbortTaskResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AbortTaskResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: AbortTaskResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AbortTaskResponse>, I>>(object: I): AbortTaskResponse {
    const message = createBaseAbortTaskResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseGetTaskInfoRequest(): GetTaskInfoRequest {
  return { taskId: undefined };
}

export const GetTaskInfoRequest = {
  encode(message: GetTaskInfoRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId1.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetTaskInfoRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetTaskInfoRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId1.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetTaskInfoRequest {
    return { taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined };
  },

  toJSON(message: GetTaskInfoRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetTaskInfoRequest>, I>>(object: I): GetTaskInfoRequest {
    const message = createBaseGetTaskInfoRequest();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    return message;
  },
};

function createBaseTaskInfoResponse(): TaskInfoResponse {
  return { status: undefined, taskInfo: undefined };
}

export const TaskInfoResponse = {
  encode(message: TaskInfoResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.taskInfo !== undefined) {
      TaskInfo.encode(message.taskInfo, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TaskInfoResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTaskInfoResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.taskInfo = TaskInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TaskInfoResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      taskInfo: isSet(object.taskInfo) ? TaskInfo.fromJSON(object.taskInfo) : undefined,
    };
  },

  toJSON(message: TaskInfoResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.taskInfo !== undefined && (obj.taskInfo = message.taskInfo ? TaskInfo.toJSON(message.taskInfo) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TaskInfoResponse>, I>>(object: I): TaskInfoResponse {
    const message = createBaseTaskInfoResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.taskInfo = (object.taskInfo !== undefined && object.taskInfo !== null)
      ? TaskInfo.fromPartial(object.taskInfo)
      : undefined;
    return message;
  },
};

function createBaseGetDataResponse(): GetDataResponse {
  return { status: undefined, recordBatch: undefined };
}

export const GetDataResponse = {
  encode(message: GetDataResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.recordBatch !== undefined) {
      DataChunk.encode(message.recordBatch, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetDataResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetDataResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.recordBatch = DataChunk.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetDataResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      recordBatch: isSet(object.recordBatch) ? DataChunk.fromJSON(object.recordBatch) : undefined,
    };
  },

  toJSON(message: GetDataResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.recordBatch !== undefined &&
      (obj.recordBatch = message.recordBatch ? DataChunk.toJSON(message.recordBatch) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetDataResponse>, I>>(object: I): GetDataResponse {
    const message = createBaseGetDataResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.recordBatch = (object.recordBatch !== undefined && object.recordBatch !== null)
      ? DataChunk.fromPartial(object.recordBatch)
      : undefined;
    return message;
  },
};

function createBaseGetStreamRequest(): GetStreamRequest {
  return { upActorId: 0, downActorId: 0, upFragmentId: 0, downFragmentId: 0 };
}

export const GetStreamRequest = {
  encode(message: GetStreamRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.upActorId !== 0) {
      writer.uint32(8).uint32(message.upActorId);
    }
    if (message.downActorId !== 0) {
      writer.uint32(16).uint32(message.downActorId);
    }
    if (message.upFragmentId !== 0) {
      writer.uint32(24).uint32(message.upFragmentId);
    }
    if (message.downFragmentId !== 0) {
      writer.uint32(32).uint32(message.downFragmentId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetStreamRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetStreamRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.upActorId = reader.uint32();
          break;
        case 2:
          message.downActorId = reader.uint32();
          break;
        case 3:
          message.upFragmentId = reader.uint32();
          break;
        case 4:
          message.downFragmentId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetStreamRequest {
    return {
      upActorId: isSet(object.upActorId) ? Number(object.upActorId) : 0,
      downActorId: isSet(object.downActorId) ? Number(object.downActorId) : 0,
      upFragmentId: isSet(object.upFragmentId) ? Number(object.upFragmentId) : 0,
      downFragmentId: isSet(object.downFragmentId) ? Number(object.downFragmentId) : 0,
    };
  },

  toJSON(message: GetStreamRequest): unknown {
    const obj: any = {};
    message.upActorId !== undefined && (obj.upActorId = Math.round(message.upActorId));
    message.downActorId !== undefined && (obj.downActorId = Math.round(message.downActorId));
    message.upFragmentId !== undefined && (obj.upFragmentId = Math.round(message.upFragmentId));
    message.downFragmentId !== undefined && (obj.downFragmentId = Math.round(message.downFragmentId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamRequest>, I>>(object: I): GetStreamRequest {
    const message = createBaseGetStreamRequest();
    message.upActorId = object.upActorId ?? 0;
    message.downActorId = object.downActorId ?? 0;
    message.upFragmentId = object.upFragmentId ?? 0;
    message.downFragmentId = object.downFragmentId ?? 0;
    return message;
  },
};

function createBaseExecuteRequest(): ExecuteRequest {
  return { taskId: undefined, plan: undefined, epoch: 0 };
}

export const ExecuteRequest = {
  encode(message: ExecuteRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId1.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    if (message.plan !== undefined) {
      PlanFragment.encode(message.plan, writer.uint32(18).fork()).ldelim();
    }
    if (message.epoch !== 0) {
      writer.uint32(24).uint64(message.epoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExecuteRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExecuteRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId1.decode(reader, reader.uint32());
          break;
        case 2:
          message.plan = PlanFragment.decode(reader, reader.uint32());
          break;
        case 3:
          message.epoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExecuteRequest {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      plan: isSet(object.plan) ? PlanFragment.fromJSON(object.plan) : undefined,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: ExecuteRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.plan !== undefined && (obj.plan = message.plan ? PlanFragment.toJSON(message.plan) : undefined);
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExecuteRequest>, I>>(object: I): ExecuteRequest {
    const message = createBaseExecuteRequest();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    message.plan = (object.plan !== undefined && object.plan !== null)
      ? PlanFragment.fromPartial(object.plan)
      : undefined;
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseGetDataRequest(): GetDataRequest {
  return { taskOutputId: undefined };
}

export const GetDataRequest = {
  encode(message: GetDataRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskOutputId !== undefined) {
      TaskOutputId.encode(message.taskOutputId, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetDataRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetDataRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskOutputId = TaskOutputId.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetDataRequest {
    return { taskOutputId: isSet(object.taskOutputId) ? TaskOutputId.fromJSON(object.taskOutputId) : undefined };
  },

  toJSON(message: GetDataRequest): unknown {
    const obj: any = {};
    message.taskOutputId !== undefined &&
      (obj.taskOutputId = message.taskOutputId ? TaskOutputId.toJSON(message.taskOutputId) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetDataRequest>, I>>(object: I): GetDataRequest {
    const message = createBaseGetDataRequest();
    message.taskOutputId = (object.taskOutputId !== undefined && object.taskOutputId !== null)
      ? TaskOutputId.fromPartial(object.taskOutputId)
      : undefined;
    return message;
  },
};

function createBaseGetStreamResponse(): GetStreamResponse {
  return { message: undefined };
}

export const GetStreamResponse = {
  encode(message: GetStreamResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.message !== undefined) {
      StreamMessage.encode(message.message, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetStreamResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetStreamResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.message = StreamMessage.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetStreamResponse {
    return { message: isSet(object.message) ? StreamMessage.fromJSON(object.message) : undefined };
  },

  toJSON(message: GetStreamResponse): unknown {
    const obj: any = {};
    message.message !== undefined &&
      (obj.message = message.message ? StreamMessage.toJSON(message.message) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamResponse>, I>>(object: I): GetStreamResponse {
    const message = createBaseGetStreamResponse();
    message.message = (object.message !== undefined && object.message !== null)
      ? StreamMessage.fromPartial(object.message)
      : undefined;
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
