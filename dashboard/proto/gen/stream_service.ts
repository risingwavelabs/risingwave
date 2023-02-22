/* eslint-disable */
import { ActorInfo, Status } from "./common";
import { SstableInfo, TableStats } from "./hummock";
import { Barrier, StreamActor } from "./stream_plan";

export const protobufPackage = "stream_service";

/** Describe the fragments which will be running on this node */
export interface UpdateActorsRequest {
  requestId: string;
  actors: StreamActor[];
}

export interface UpdateActorsResponse {
  status: Status | undefined;
}

export interface BroadcastActorInfoTableRequest {
  info: ActorInfo[];
}

/** Create channels and gRPC connections for a fragment */
export interface BuildActorsRequest {
  requestId: string;
  actorId: number[];
}

export interface BuildActorsResponse {
  requestId: string;
  status: Status | undefined;
}

export interface DropActorsRequest {
  requestId: string;
  actorIds: number[];
}

export interface DropActorsResponse {
  requestId: string;
  status: Status | undefined;
}

export interface ForceStopActorsRequest {
  requestId: string;
}

export interface ForceStopActorsResponse {
  requestId: string;
  status: Status | undefined;
}

export interface InjectBarrierRequest {
  requestId: string;
  barrier: Barrier | undefined;
  actorIdsToSend: number[];
  actorIdsToCollect: number[];
}

export interface InjectBarrierResponse {
  requestId: string;
  status: Status | undefined;
}

export interface BarrierCompleteRequest {
  requestId: string;
  prevEpoch: number;
}

export interface BarrierCompleteResponse {
  requestId: string;
  status: Status | undefined;
  createMviewProgress: BarrierCompleteResponse_CreateMviewProgress[];
  syncedSstables: BarrierCompleteResponse_GroupedSstableInfo[];
  workerId: number;
}

export interface BarrierCompleteResponse_CreateMviewProgress {
  chainActorId: number;
  done: boolean;
  consumedEpoch: number;
  consumedRows: number;
}

export interface BarrierCompleteResponse_GroupedSstableInfo {
  compactionGroupId: number;
  sst: SstableInfo | undefined;
  tableStatsMap: { [key: number]: TableStats };
}

export interface BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry {
  key: number;
  value: TableStats | undefined;
}

/** Before starting streaming, the leader node broadcast the actor-host table to needed workers. */
export interface BroadcastActorInfoTableResponse {
  status: Status | undefined;
}

export interface WaitEpochCommitRequest {
  epoch: number;
}

export interface WaitEpochCommitResponse {
  status: Status | undefined;
}

function createBaseUpdateActorsRequest(): UpdateActorsRequest {
  return { requestId: "", actors: [] };
}

export const UpdateActorsRequest = {
  fromJSON(object: any): UpdateActorsRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => StreamActor.fromJSON(e)) : [],
    };
  },

  toJSON(message: UpdateActorsRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    if (message.actors) {
      obj.actors = message.actors.map((e) => e ? StreamActor.toJSON(e) : undefined);
    } else {
      obj.actors = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateActorsRequest>, I>>(object: I): UpdateActorsRequest {
    const message = createBaseUpdateActorsRequest();
    message.requestId = object.requestId ?? "";
    message.actors = object.actors?.map((e) => StreamActor.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUpdateActorsResponse(): UpdateActorsResponse {
  return { status: undefined };
}

export const UpdateActorsResponse = {
  fromJSON(object: any): UpdateActorsResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: UpdateActorsResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateActorsResponse>, I>>(object: I): UpdateActorsResponse {
    const message = createBaseUpdateActorsResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseBroadcastActorInfoTableRequest(): BroadcastActorInfoTableRequest {
  return { info: [] };
}

export const BroadcastActorInfoTableRequest = {
  fromJSON(object: any): BroadcastActorInfoTableRequest {
    return { info: Array.isArray(object?.info) ? object.info.map((e: any) => ActorInfo.fromJSON(e)) : [] };
  },

  toJSON(message: BroadcastActorInfoTableRequest): unknown {
    const obj: any = {};
    if (message.info) {
      obj.info = message.info.map((e) => e ? ActorInfo.toJSON(e) : undefined);
    } else {
      obj.info = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BroadcastActorInfoTableRequest>, I>>(
    object: I,
  ): BroadcastActorInfoTableRequest {
    const message = createBaseBroadcastActorInfoTableRequest();
    message.info = object.info?.map((e) => ActorInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseBuildActorsRequest(): BuildActorsRequest {
  return { requestId: "", actorId: [] };
}

export const BuildActorsRequest = {
  fromJSON(object: any): BuildActorsRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      actorId: Array.isArray(object?.actorId) ? object.actorId.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: BuildActorsRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    if (message.actorId) {
      obj.actorId = message.actorId.map((e) => Math.round(e));
    } else {
      obj.actorId = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BuildActorsRequest>, I>>(object: I): BuildActorsRequest {
    const message = createBaseBuildActorsRequest();
    message.requestId = object.requestId ?? "";
    message.actorId = object.actorId?.map((e) => e) || [];
    return message;
  },
};

function createBaseBuildActorsResponse(): BuildActorsResponse {
  return { requestId: "", status: undefined };
}

export const BuildActorsResponse = {
  fromJSON(object: any): BuildActorsResponse {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
    };
  },

  toJSON(message: BuildActorsResponse): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BuildActorsResponse>, I>>(object: I): BuildActorsResponse {
    const message = createBaseBuildActorsResponse();
    message.requestId = object.requestId ?? "";
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseDropActorsRequest(): DropActorsRequest {
  return { requestId: "", actorIds: [] };
}

export const DropActorsRequest = {
  fromJSON(object: any): DropActorsRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      actorIds: Array.isArray(object?.actorIds) ? object.actorIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: DropActorsRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    if (message.actorIds) {
      obj.actorIds = message.actorIds.map((e) => Math.round(e));
    } else {
      obj.actorIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropActorsRequest>, I>>(object: I): DropActorsRequest {
    const message = createBaseDropActorsRequest();
    message.requestId = object.requestId ?? "";
    message.actorIds = object.actorIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseDropActorsResponse(): DropActorsResponse {
  return { requestId: "", status: undefined };
}

export const DropActorsResponse = {
  fromJSON(object: any): DropActorsResponse {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
    };
  },

  toJSON(message: DropActorsResponse): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropActorsResponse>, I>>(object: I): DropActorsResponse {
    const message = createBaseDropActorsResponse();
    message.requestId = object.requestId ?? "";
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseForceStopActorsRequest(): ForceStopActorsRequest {
  return { requestId: "" };
}

export const ForceStopActorsRequest = {
  fromJSON(object: any): ForceStopActorsRequest {
    return { requestId: isSet(object.requestId) ? String(object.requestId) : "" };
  },

  toJSON(message: ForceStopActorsRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ForceStopActorsRequest>, I>>(object: I): ForceStopActorsRequest {
    const message = createBaseForceStopActorsRequest();
    message.requestId = object.requestId ?? "";
    return message;
  },
};

function createBaseForceStopActorsResponse(): ForceStopActorsResponse {
  return { requestId: "", status: undefined };
}

export const ForceStopActorsResponse = {
  fromJSON(object: any): ForceStopActorsResponse {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
    };
  },

  toJSON(message: ForceStopActorsResponse): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ForceStopActorsResponse>, I>>(object: I): ForceStopActorsResponse {
    const message = createBaseForceStopActorsResponse();
    message.requestId = object.requestId ?? "";
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseInjectBarrierRequest(): InjectBarrierRequest {
  return { requestId: "", barrier: undefined, actorIdsToSend: [], actorIdsToCollect: [] };
}

export const InjectBarrierRequest = {
  fromJSON(object: any): InjectBarrierRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      barrier: isSet(object.barrier) ? Barrier.fromJSON(object.barrier) : undefined,
      actorIdsToSend: Array.isArray(object?.actorIdsToSend) ? object.actorIdsToSend.map((e: any) => Number(e)) : [],
      actorIdsToCollect: Array.isArray(object?.actorIdsToCollect)
        ? object.actorIdsToCollect.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: InjectBarrierRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.barrier !== undefined && (obj.barrier = message.barrier ? Barrier.toJSON(message.barrier) : undefined);
    if (message.actorIdsToSend) {
      obj.actorIdsToSend = message.actorIdsToSend.map((e) => Math.round(e));
    } else {
      obj.actorIdsToSend = [];
    }
    if (message.actorIdsToCollect) {
      obj.actorIdsToCollect = message.actorIdsToCollect.map((e) => Math.round(e));
    } else {
      obj.actorIdsToCollect = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InjectBarrierRequest>, I>>(object: I): InjectBarrierRequest {
    const message = createBaseInjectBarrierRequest();
    message.requestId = object.requestId ?? "";
    message.barrier = (object.barrier !== undefined && object.barrier !== null)
      ? Barrier.fromPartial(object.barrier)
      : undefined;
    message.actorIdsToSend = object.actorIdsToSend?.map((e) => e) || [];
    message.actorIdsToCollect = object.actorIdsToCollect?.map((e) => e) || [];
    return message;
  },
};

function createBaseInjectBarrierResponse(): InjectBarrierResponse {
  return { requestId: "", status: undefined };
}

export const InjectBarrierResponse = {
  fromJSON(object: any): InjectBarrierResponse {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
    };
  },

  toJSON(message: InjectBarrierResponse): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InjectBarrierResponse>, I>>(object: I): InjectBarrierResponse {
    const message = createBaseInjectBarrierResponse();
    message.requestId = object.requestId ?? "";
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseBarrierCompleteRequest(): BarrierCompleteRequest {
  return { requestId: "", prevEpoch: 0 };
}

export const BarrierCompleteRequest = {
  fromJSON(object: any): BarrierCompleteRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      prevEpoch: isSet(object.prevEpoch) ? Number(object.prevEpoch) : 0,
    };
  },

  toJSON(message: BarrierCompleteRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.prevEpoch !== undefined && (obj.prevEpoch = Math.round(message.prevEpoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteRequest>, I>>(object: I): BarrierCompleteRequest {
    const message = createBaseBarrierCompleteRequest();
    message.requestId = object.requestId ?? "";
    message.prevEpoch = object.prevEpoch ?? 0;
    return message;
  },
};

function createBaseBarrierCompleteResponse(): BarrierCompleteResponse {
  return { requestId: "", status: undefined, createMviewProgress: [], syncedSstables: [], workerId: 0 };
}

export const BarrierCompleteResponse = {
  fromJSON(object: any): BarrierCompleteResponse {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      createMviewProgress: Array.isArray(object?.createMviewProgress)
        ? object.createMviewProgress.map((e: any) => BarrierCompleteResponse_CreateMviewProgress.fromJSON(e))
        : [],
      syncedSstables: Array.isArray(object?.syncedSstables)
        ? object.syncedSstables.map((e: any) => BarrierCompleteResponse_GroupedSstableInfo.fromJSON(e))
        : [],
      workerId: isSet(object.workerId) ? Number(object.workerId) : 0,
    };
  },

  toJSON(message: BarrierCompleteResponse): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    if (message.createMviewProgress) {
      obj.createMviewProgress = message.createMviewProgress.map((e) =>
        e ? BarrierCompleteResponse_CreateMviewProgress.toJSON(e) : undefined
      );
    } else {
      obj.createMviewProgress = [];
    }
    if (message.syncedSstables) {
      obj.syncedSstables = message.syncedSstables.map((e) =>
        e ? BarrierCompleteResponse_GroupedSstableInfo.toJSON(e) : undefined
      );
    } else {
      obj.syncedSstables = [];
    }
    message.workerId !== undefined && (obj.workerId = Math.round(message.workerId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse>, I>>(object: I): BarrierCompleteResponse {
    const message = createBaseBarrierCompleteResponse();
    message.requestId = object.requestId ?? "";
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.createMviewProgress =
      object.createMviewProgress?.map((e) => BarrierCompleteResponse_CreateMviewProgress.fromPartial(e)) || [];
    message.syncedSstables =
      object.syncedSstables?.map((e) => BarrierCompleteResponse_GroupedSstableInfo.fromPartial(e)) || [];
    message.workerId = object.workerId ?? 0;
    return message;
  },
};

function createBaseBarrierCompleteResponse_CreateMviewProgress(): BarrierCompleteResponse_CreateMviewProgress {
  return { chainActorId: 0, done: false, consumedEpoch: 0, consumedRows: 0 };
}

export const BarrierCompleteResponse_CreateMviewProgress = {
  fromJSON(object: any): BarrierCompleteResponse_CreateMviewProgress {
    return {
      chainActorId: isSet(object.chainActorId) ? Number(object.chainActorId) : 0,
      done: isSet(object.done) ? Boolean(object.done) : false,
      consumedEpoch: isSet(object.consumedEpoch) ? Number(object.consumedEpoch) : 0,
      consumedRows: isSet(object.consumedRows) ? Number(object.consumedRows) : 0,
    };
  },

  toJSON(message: BarrierCompleteResponse_CreateMviewProgress): unknown {
    const obj: any = {};
    message.chainActorId !== undefined && (obj.chainActorId = Math.round(message.chainActorId));
    message.done !== undefined && (obj.done = message.done);
    message.consumedEpoch !== undefined && (obj.consumedEpoch = Math.round(message.consumedEpoch));
    message.consumedRows !== undefined && (obj.consumedRows = Math.round(message.consumedRows));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse_CreateMviewProgress>, I>>(
    object: I,
  ): BarrierCompleteResponse_CreateMviewProgress {
    const message = createBaseBarrierCompleteResponse_CreateMviewProgress();
    message.chainActorId = object.chainActorId ?? 0;
    message.done = object.done ?? false;
    message.consumedEpoch = object.consumedEpoch ?? 0;
    message.consumedRows = object.consumedRows ?? 0;
    return message;
  },
};

function createBaseBarrierCompleteResponse_GroupedSstableInfo(): BarrierCompleteResponse_GroupedSstableInfo {
  return { compactionGroupId: 0, sst: undefined, tableStatsMap: {} };
}

export const BarrierCompleteResponse_GroupedSstableInfo = {
  fromJSON(object: any): BarrierCompleteResponse_GroupedSstableInfo {
    return {
      compactionGroupId: isSet(object.compactionGroupId) ? Number(object.compactionGroupId) : 0,
      sst: isSet(object.sst) ? SstableInfo.fromJSON(object.sst) : undefined,
      tableStatsMap: isObject(object.tableStatsMap)
        ? Object.entries(object.tableStatsMap).reduce<{ [key: number]: TableStats }>((acc, [key, value]) => {
          acc[Number(key)] = TableStats.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: BarrierCompleteResponse_GroupedSstableInfo): unknown {
    const obj: any = {};
    message.compactionGroupId !== undefined && (obj.compactionGroupId = Math.round(message.compactionGroupId));
    message.sst !== undefined && (obj.sst = message.sst ? SstableInfo.toJSON(message.sst) : undefined);
    obj.tableStatsMap = {};
    if (message.tableStatsMap) {
      Object.entries(message.tableStatsMap).forEach(([k, v]) => {
        obj.tableStatsMap[k] = TableStats.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse_GroupedSstableInfo>, I>>(
    object: I,
  ): BarrierCompleteResponse_GroupedSstableInfo {
    const message = createBaseBarrierCompleteResponse_GroupedSstableInfo();
    message.compactionGroupId = object.compactionGroupId ?? 0;
    message.sst = (object.sst !== undefined && object.sst !== null) ? SstableInfo.fromPartial(object.sst) : undefined;
    message.tableStatsMap = Object.entries(object.tableStatsMap ?? {}).reduce<{ [key: number]: TableStats }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = TableStats.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseBarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry(): BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry {
  return { key: 0, value: undefined };
}

export const BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry = {
  fromJSON(object: any): BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableStats.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? TableStats.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry>, I>>(
    object: I,
  ): BarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry {
    const message = createBaseBarrierCompleteResponse_GroupedSstableInfo_TableStatsMapEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableStats.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseBroadcastActorInfoTableResponse(): BroadcastActorInfoTableResponse {
  return { status: undefined };
}

export const BroadcastActorInfoTableResponse = {
  fromJSON(object: any): BroadcastActorInfoTableResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: BroadcastActorInfoTableResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BroadcastActorInfoTableResponse>, I>>(
    object: I,
  ): BroadcastActorInfoTableResponse {
    const message = createBaseBroadcastActorInfoTableResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseWaitEpochCommitRequest(): WaitEpochCommitRequest {
  return { epoch: 0 };
}

export const WaitEpochCommitRequest = {
  fromJSON(object: any): WaitEpochCommitRequest {
    return { epoch: isSet(object.epoch) ? Number(object.epoch) : 0 };
  },

  toJSON(message: WaitEpochCommitRequest): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<WaitEpochCommitRequest>, I>>(object: I): WaitEpochCommitRequest {
    const message = createBaseWaitEpochCommitRequest();
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseWaitEpochCommitResponse(): WaitEpochCommitResponse {
  return { status: undefined };
}

export const WaitEpochCommitResponse = {
  fromJSON(object: any): WaitEpochCommitResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: WaitEpochCommitResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<WaitEpochCommitResponse>, I>>(object: I): WaitEpochCommitResponse {
    const message = createBaseWaitEpochCommitResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
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

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
