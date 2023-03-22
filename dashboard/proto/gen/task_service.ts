/* eslint-disable */
import { PlanFragment, TaskId as TaskId1, TaskOutputId } from "./batch_plan";
import { BatchQueryEpoch, Status } from "./common";
import { DataChunk } from "./data";
import { StreamMessage } from "./stream_plan";

export const protobufPackage = "task_service";

/** Task is a running instance of Stage. */
export interface TaskId {
  queryId: string;
  stageId: number;
  taskId: number;
}

export interface TaskInfoResponse {
  taskId: TaskId1 | undefined;
  taskStatus: TaskInfoResponse_TaskStatus;
  /** Optional error message for failed task. */
  errorMessage: string;
}

export const TaskInfoResponse_TaskStatus = {
  /** UNSPECIFIED - Note: Requirement of proto3: first enum must be 0. */
  UNSPECIFIED: "UNSPECIFIED",
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  FINISHED: "FINISHED",
  FAILED: "FAILED",
  ABORTED: "ABORTED",
  CANCELLED: "CANCELLED",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TaskInfoResponse_TaskStatus = typeof TaskInfoResponse_TaskStatus[keyof typeof TaskInfoResponse_TaskStatus];

export function taskInfoResponse_TaskStatusFromJSON(object: any): TaskInfoResponse_TaskStatus {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TaskInfoResponse_TaskStatus.UNSPECIFIED;
    case 2:
    case "PENDING":
      return TaskInfoResponse_TaskStatus.PENDING;
    case 3:
    case "RUNNING":
      return TaskInfoResponse_TaskStatus.RUNNING;
    case 6:
    case "FINISHED":
      return TaskInfoResponse_TaskStatus.FINISHED;
    case 7:
    case "FAILED":
      return TaskInfoResponse_TaskStatus.FAILED;
    case 8:
    case "ABORTED":
      return TaskInfoResponse_TaskStatus.ABORTED;
    case 9:
    case "CANCELLED":
      return TaskInfoResponse_TaskStatus.CANCELLED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TaskInfoResponse_TaskStatus.UNRECOGNIZED;
  }
}

export function taskInfoResponse_TaskStatusToJSON(object: TaskInfoResponse_TaskStatus): string {
  switch (object) {
    case TaskInfoResponse_TaskStatus.UNSPECIFIED:
      return "UNSPECIFIED";
    case TaskInfoResponse_TaskStatus.PENDING:
      return "PENDING";
    case TaskInfoResponse_TaskStatus.RUNNING:
      return "RUNNING";
    case TaskInfoResponse_TaskStatus.FINISHED:
      return "FINISHED";
    case TaskInfoResponse_TaskStatus.FAILED:
      return "FAILED";
    case TaskInfoResponse_TaskStatus.ABORTED:
      return "ABORTED";
    case TaskInfoResponse_TaskStatus.CANCELLED:
      return "CANCELLED";
    case TaskInfoResponse_TaskStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface CreateTaskRequest {
  taskId: TaskId1 | undefined;
  plan: PlanFragment | undefined;
  epoch: BatchQueryEpoch | undefined;
}

export interface CancelTaskRequest {
  taskId: TaskId1 | undefined;
}

export interface CancelTaskResponse {
  status: Status | undefined;
}

export interface GetTaskInfoRequest {
  taskId: TaskId1 | undefined;
}

export interface GetDataResponse {
  recordBatch: DataChunk | undefined;
}

export interface ExecuteRequest {
  taskId: TaskId1 | undefined;
  plan: PlanFragment | undefined;
  epoch: BatchQueryEpoch | undefined;
}

export interface GetDataRequest {
  taskOutputId: TaskOutputId | undefined;
}

export interface GetStreamRequest {
  value?: { $case: "get"; get: GetStreamRequest_Get } | {
    $case: "addPermits";
    addPermits: GetStreamRequest_AddPermits;
  };
}

/** The first message, which tells the upstream which channel this exchange stream is for. */
export interface GetStreamRequest_Get {
  upActorId: number;
  downActorId: number;
  upFragmentId: number;
  downFragmentId: number;
}

/** The following messages, which adds the permits back to the upstream to achieve back-pressure. */
export interface GetStreamRequest_AddPermits {
  permits: number;
}

export interface GetStreamResponse {
  message:
    | StreamMessage
    | undefined;
  /** The number of permits acquired for this message, which should be sent back to the upstream with `AddPermits`. */
  permits: number;
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

function createBaseTaskInfoResponse(): TaskInfoResponse {
  return { taskId: undefined, taskStatus: TaskInfoResponse_TaskStatus.UNSPECIFIED, errorMessage: "" };
}

export const TaskInfoResponse = {
  fromJSON(object: any): TaskInfoResponse {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      taskStatus: isSet(object.taskStatus)
        ? taskInfoResponse_TaskStatusFromJSON(object.taskStatus)
        : TaskInfoResponse_TaskStatus.UNSPECIFIED,
      errorMessage: isSet(object.errorMessage) ? String(object.errorMessage) : "",
    };
  },

  toJSON(message: TaskInfoResponse): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.taskStatus !== undefined && (obj.taskStatus = taskInfoResponse_TaskStatusToJSON(message.taskStatus));
    message.errorMessage !== undefined && (obj.errorMessage = message.errorMessage);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TaskInfoResponse>, I>>(object: I): TaskInfoResponse {
    const message = createBaseTaskInfoResponse();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    message.taskStatus = object.taskStatus ?? TaskInfoResponse_TaskStatus.UNSPECIFIED;
    message.errorMessage = object.errorMessage ?? "";
    return message;
  },
};

function createBaseCreateTaskRequest(): CreateTaskRequest {
  return { taskId: undefined, plan: undefined, epoch: undefined };
}

export const CreateTaskRequest = {
  fromJSON(object: any): CreateTaskRequest {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      plan: isSet(object.plan) ? PlanFragment.fromJSON(object.plan) : undefined,
      epoch: isSet(object.epoch) ? BatchQueryEpoch.fromJSON(object.epoch) : undefined,
    };
  },

  toJSON(message: CreateTaskRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.plan !== undefined && (obj.plan = message.plan ? PlanFragment.toJSON(message.plan) : undefined);
    message.epoch !== undefined && (obj.epoch = message.epoch ? BatchQueryEpoch.toJSON(message.epoch) : undefined);
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
    message.epoch = (object.epoch !== undefined && object.epoch !== null)
      ? BatchQueryEpoch.fromPartial(object.epoch)
      : undefined;
    return message;
  },
};

function createBaseCancelTaskRequest(): CancelTaskRequest {
  return { taskId: undefined };
}

export const CancelTaskRequest = {
  fromJSON(object: any): CancelTaskRequest {
    return { taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined };
  },

  toJSON(message: CancelTaskRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CancelTaskRequest>, I>>(object: I): CancelTaskRequest {
    const message = createBaseCancelTaskRequest();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId1.fromPartial(object.taskId)
      : undefined;
    return message;
  },
};

function createBaseCancelTaskResponse(): CancelTaskResponse {
  return { status: undefined };
}

export const CancelTaskResponse = {
  fromJSON(object: any): CancelTaskResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: CancelTaskResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CancelTaskResponse>, I>>(object: I): CancelTaskResponse {
    const message = createBaseCancelTaskResponse();
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

function createBaseGetDataResponse(): GetDataResponse {
  return { recordBatch: undefined };
}

export const GetDataResponse = {
  fromJSON(object: any): GetDataResponse {
    return { recordBatch: isSet(object.recordBatch) ? DataChunk.fromJSON(object.recordBatch) : undefined };
  },

  toJSON(message: GetDataResponse): unknown {
    const obj: any = {};
    message.recordBatch !== undefined &&
      (obj.recordBatch = message.recordBatch ? DataChunk.toJSON(message.recordBatch) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetDataResponse>, I>>(object: I): GetDataResponse {
    const message = createBaseGetDataResponse();
    message.recordBatch = (object.recordBatch !== undefined && object.recordBatch !== null)
      ? DataChunk.fromPartial(object.recordBatch)
      : undefined;
    return message;
  },
};

function createBaseExecuteRequest(): ExecuteRequest {
  return { taskId: undefined, plan: undefined, epoch: undefined };
}

export const ExecuteRequest = {
  fromJSON(object: any): ExecuteRequest {
    return {
      taskId: isSet(object.taskId) ? TaskId1.fromJSON(object.taskId) : undefined,
      plan: isSet(object.plan) ? PlanFragment.fromJSON(object.plan) : undefined,
      epoch: isSet(object.epoch) ? BatchQueryEpoch.fromJSON(object.epoch) : undefined,
    };
  },

  toJSON(message: ExecuteRequest): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId1.toJSON(message.taskId) : undefined);
    message.plan !== undefined && (obj.plan = message.plan ? PlanFragment.toJSON(message.plan) : undefined);
    message.epoch !== undefined && (obj.epoch = message.epoch ? BatchQueryEpoch.toJSON(message.epoch) : undefined);
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
    message.epoch = (object.epoch !== undefined && object.epoch !== null)
      ? BatchQueryEpoch.fromPartial(object.epoch)
      : undefined;
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

function createBaseGetStreamRequest(): GetStreamRequest {
  return { value: undefined };
}

export const GetStreamRequest = {
  fromJSON(object: any): GetStreamRequest {
    return {
      value: isSet(object.get)
        ? { $case: "get", get: GetStreamRequest_Get.fromJSON(object.get) }
        : isSet(object.addPermits)
        ? { $case: "addPermits", addPermits: GetStreamRequest_AddPermits.fromJSON(object.addPermits) }
        : undefined,
    };
  },

  toJSON(message: GetStreamRequest): unknown {
    const obj: any = {};
    message.value?.$case === "get" &&
      (obj.get = message.value?.get ? GetStreamRequest_Get.toJSON(message.value?.get) : undefined);
    message.value?.$case === "addPermits" && (obj.addPermits = message.value?.addPermits
      ? GetStreamRequest_AddPermits.toJSON(message.value?.addPermits)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamRequest>, I>>(object: I): GetStreamRequest {
    const message = createBaseGetStreamRequest();
    if (object.value?.$case === "get" && object.value?.get !== undefined && object.value?.get !== null) {
      message.value = { $case: "get", get: GetStreamRequest_Get.fromPartial(object.value.get) };
    }
    if (
      object.value?.$case === "addPermits" &&
      object.value?.addPermits !== undefined &&
      object.value?.addPermits !== null
    ) {
      message.value = {
        $case: "addPermits",
        addPermits: GetStreamRequest_AddPermits.fromPartial(object.value.addPermits),
      };
    }
    return message;
  },
};

function createBaseGetStreamRequest_Get(): GetStreamRequest_Get {
  return { upActorId: 0, downActorId: 0, upFragmentId: 0, downFragmentId: 0 };
}

export const GetStreamRequest_Get = {
  fromJSON(object: any): GetStreamRequest_Get {
    return {
      upActorId: isSet(object.upActorId) ? Number(object.upActorId) : 0,
      downActorId: isSet(object.downActorId) ? Number(object.downActorId) : 0,
      upFragmentId: isSet(object.upFragmentId) ? Number(object.upFragmentId) : 0,
      downFragmentId: isSet(object.downFragmentId) ? Number(object.downFragmentId) : 0,
    };
  },

  toJSON(message: GetStreamRequest_Get): unknown {
    const obj: any = {};
    message.upActorId !== undefined && (obj.upActorId = Math.round(message.upActorId));
    message.downActorId !== undefined && (obj.downActorId = Math.round(message.downActorId));
    message.upFragmentId !== undefined && (obj.upFragmentId = Math.round(message.upFragmentId));
    message.downFragmentId !== undefined && (obj.downFragmentId = Math.round(message.downFragmentId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamRequest_Get>, I>>(object: I): GetStreamRequest_Get {
    const message = createBaseGetStreamRequest_Get();
    message.upActorId = object.upActorId ?? 0;
    message.downActorId = object.downActorId ?? 0;
    message.upFragmentId = object.upFragmentId ?? 0;
    message.downFragmentId = object.downFragmentId ?? 0;
    return message;
  },
};

function createBaseGetStreamRequest_AddPermits(): GetStreamRequest_AddPermits {
  return { permits: 0 };
}

export const GetStreamRequest_AddPermits = {
  fromJSON(object: any): GetStreamRequest_AddPermits {
    return { permits: isSet(object.permits) ? Number(object.permits) : 0 };
  },

  toJSON(message: GetStreamRequest_AddPermits): unknown {
    const obj: any = {};
    message.permits !== undefined && (obj.permits = Math.round(message.permits));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamRequest_AddPermits>, I>>(object: I): GetStreamRequest_AddPermits {
    const message = createBaseGetStreamRequest_AddPermits();
    message.permits = object.permits ?? 0;
    return message;
  },
};

function createBaseGetStreamResponse(): GetStreamResponse {
  return { message: undefined, permits: 0 };
}

export const GetStreamResponse = {
  fromJSON(object: any): GetStreamResponse {
    return {
      message: isSet(object.message) ? StreamMessage.fromJSON(object.message) : undefined,
      permits: isSet(object.permits) ? Number(object.permits) : 0,
    };
  },

  toJSON(message: GetStreamResponse): unknown {
    const obj: any = {};
    message.message !== undefined &&
      (obj.message = message.message ? StreamMessage.toJSON(message.message) : undefined);
    message.permits !== undefined && (obj.permits = Math.round(message.permits));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetStreamResponse>, I>>(object: I): GetStreamResponse {
    const message = createBaseGetStreamResponse();
    message.message = (object.message !== undefined && object.message !== null)
      ? StreamMessage.fromPartial(object.message)
      : undefined;
    message.permits = object.permits ?? 0;
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
