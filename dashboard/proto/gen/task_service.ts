/* eslint-disable */
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

export const TaskInfo_TaskStatus = {
  /** UNSPECIFIED - Note: Requirement of proto3: first enum must be 0. */
  UNSPECIFIED: "UNSPECIFIED",
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  FINISHED: "FINISHED",
  FAILED: "FAILED",
  ABORTED: "ABORTED",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TaskInfo_TaskStatus = typeof TaskInfo_TaskStatus[keyof typeof TaskInfo_TaskStatus];

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
  return { taskId: undefined, taskStatus: TaskInfo_TaskStatus.UNSPECIFIED };
}

export const TaskInfo = {
  fromJSON(object: any): TaskInfo {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      taskStatus: isSet(object.taskStatus)
        ? taskInfo_TaskStatusFromJSON(object.taskStatus)
        : TaskInfo_TaskStatus.UNSPECIFIED,
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
    message.taskStatus = object.taskStatus ?? TaskInfo_TaskStatus.UNSPECIFIED;
    return message;
  },
};

function createBaseCreateTaskRequest(): CreateTaskRequest {
  return { taskId: undefined, plan: undefined, epoch: 0 };
}

export const CreateTaskRequest = {
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
