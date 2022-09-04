/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Source } from "./catalog";
import { ActorInfo, Status } from "./common";
import { Epoch } from "./data";
import { SstableInfo } from "./hummock";
import { Barrier, StreamActor } from "./stream_plan";

export const protobufPackage = "stream_service";

export interface HangingChannel {
  upstream: ActorInfo | undefined;
  downstream: ActorInfo | undefined;
}

/** Describe the fragments which will be running on this node */
export interface UpdateActorsRequest {
  requestId: string;
  actors: StreamActor[];
  hangingChannels: HangingChannel[];
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
  epoch: Epoch | undefined;
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
  /**
   * Whether the collected barriers do checkpoint. It is usually the same as barrier's checkpoint
   * unless it fails to compete with another barrier (checkpoint = true) for sync.
   */
  checkpoint: boolean;
}

export interface BarrierCompleteResponse_CreateMviewProgress {
  chainActorId: number;
  done: boolean;
  consumedEpoch: number;
}

export interface BarrierCompleteResponse_GroupedSstableInfo {
  compactionGroupId: number;
  sst: SstableInfo | undefined;
}

/** Before starting streaming, the leader node broadcast the actor-host table to needed workers. */
export interface BroadcastActorInfoTableResponse {
  status: Status | undefined;
}

export interface CreateSourceRequest {
  source: Source | undefined;
}

export interface CreateSourceResponse {
  status: Status | undefined;
}

export interface DropSourceRequest {
  sourceId: number;
}

export interface DropSourceResponse {
  status: Status | undefined;
}

export interface SyncSourcesRequest {
  sources: Source[];
}

export interface SyncSourcesResponse {
  status: Status | undefined;
}

function createBaseHangingChannel(): HangingChannel {
  return { upstream: undefined, downstream: undefined };
}

export const HangingChannel = {
  encode(message: HangingChannel, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.upstream !== undefined) {
      ActorInfo.encode(message.upstream, writer.uint32(10).fork()).ldelim();
    }
    if (message.downstream !== undefined) {
      ActorInfo.encode(message.downstream, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HangingChannel {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHangingChannel();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.upstream = ActorInfo.decode(reader, reader.uint32());
          break;
        case 2:
          message.downstream = ActorInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HangingChannel {
    return {
      upstream: isSet(object.upstream) ? ActorInfo.fromJSON(object.upstream) : undefined,
      downstream: isSet(object.downstream) ? ActorInfo.fromJSON(object.downstream) : undefined,
    };
  },

  toJSON(message: HangingChannel): unknown {
    const obj: any = {};
    message.upstream !== undefined &&
      (obj.upstream = message.upstream ? ActorInfo.toJSON(message.upstream) : undefined);
    message.downstream !== undefined &&
      (obj.downstream = message.downstream ? ActorInfo.toJSON(message.downstream) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HangingChannel>, I>>(object: I): HangingChannel {
    const message = createBaseHangingChannel();
    message.upstream = (object.upstream !== undefined && object.upstream !== null)
      ? ActorInfo.fromPartial(object.upstream)
      : undefined;
    message.downstream = (object.downstream !== undefined && object.downstream !== null)
      ? ActorInfo.fromPartial(object.downstream)
      : undefined;
    return message;
  },
};

function createBaseUpdateActorsRequest(): UpdateActorsRequest {
  return { requestId: "", actors: [], hangingChannels: [] };
}

export const UpdateActorsRequest = {
  encode(message: UpdateActorsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    for (const v of message.actors) {
      StreamActor.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.hangingChannels) {
      HangingChannel.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UpdateActorsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUpdateActorsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.actors.push(StreamActor.decode(reader, reader.uint32()));
          break;
        case 3:
          message.hangingChannels.push(HangingChannel.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UpdateActorsRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => StreamActor.fromJSON(e)) : [],
      hangingChannels: Array.isArray(object?.hangingChannels)
        ? object.hangingChannels.map((e: any) => HangingChannel.fromJSON(e))
        : [],
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
    if (message.hangingChannels) {
      obj.hangingChannels = message.hangingChannels.map((e) => e ? HangingChannel.toJSON(e) : undefined);
    } else {
      obj.hangingChannels = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateActorsRequest>, I>>(object: I): UpdateActorsRequest {
    const message = createBaseUpdateActorsRequest();
    message.requestId = object.requestId ?? "";
    message.actors = object.actors?.map((e) => StreamActor.fromPartial(e)) || [];
    message.hangingChannels = object.hangingChannels?.map((e) => HangingChannel.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUpdateActorsResponse(): UpdateActorsResponse {
  return { status: undefined };
}

export const UpdateActorsResponse = {
  encode(message: UpdateActorsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UpdateActorsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUpdateActorsResponse();
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
  encode(message: BroadcastActorInfoTableRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.info) {
      ActorInfo.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BroadcastActorInfoTableRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBroadcastActorInfoTableRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.info.push(ActorInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: BuildActorsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    writer.uint32(18).fork();
    for (const v of message.actorId) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BuildActorsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBuildActorsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.actorId.push(reader.uint32());
            }
          } else {
            message.actorId.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: BuildActorsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BuildActorsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBuildActorsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.status = Status.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: DropActorsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    writer.uint32(18).fork();
    for (const v of message.actorIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropActorsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropActorsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.actorIds.push(reader.uint32());
            }
          } else {
            message.actorIds.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: DropActorsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DropActorsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDropActorsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.status = Status.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  return { requestId: "", epoch: undefined };
}

export const ForceStopActorsRequest = {
  encode(message: ForceStopActorsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.epoch !== undefined) {
      Epoch.encode(message.epoch, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ForceStopActorsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseForceStopActorsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.epoch = Epoch.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ForceStopActorsRequest {
    return {
      requestId: isSet(object.requestId) ? String(object.requestId) : "",
      epoch: isSet(object.epoch) ? Epoch.fromJSON(object.epoch) : undefined,
    };
  },

  toJSON(message: ForceStopActorsRequest): unknown {
    const obj: any = {};
    message.requestId !== undefined && (obj.requestId = message.requestId);
    message.epoch !== undefined && (obj.epoch = message.epoch ? Epoch.toJSON(message.epoch) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ForceStopActorsRequest>, I>>(object: I): ForceStopActorsRequest {
    const message = createBaseForceStopActorsRequest();
    message.requestId = object.requestId ?? "";
    message.epoch = (object.epoch !== undefined && object.epoch !== null) ? Epoch.fromPartial(object.epoch) : undefined;
    return message;
  },
};

function createBaseForceStopActorsResponse(): ForceStopActorsResponse {
  return { requestId: "", status: undefined };
}

export const ForceStopActorsResponse = {
  encode(message: ForceStopActorsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ForceStopActorsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseForceStopActorsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.status = Status.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: InjectBarrierRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.barrier !== undefined) {
      Barrier.encode(message.barrier, writer.uint32(18).fork()).ldelim();
    }
    writer.uint32(26).fork();
    for (const v of message.actorIdsToSend) {
      writer.uint32(v);
    }
    writer.ldelim();
    writer.uint32(34).fork();
    for (const v of message.actorIdsToCollect) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): InjectBarrierRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseInjectBarrierRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.barrier = Barrier.decode(reader, reader.uint32());
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.actorIdsToSend.push(reader.uint32());
            }
          } else {
            message.actorIdsToSend.push(reader.uint32());
          }
          break;
        case 4:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.actorIdsToCollect.push(reader.uint32());
            }
          } else {
            message.actorIdsToCollect.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: InjectBarrierResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): InjectBarrierResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseInjectBarrierResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.status = Status.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  encode(message: BarrierCompleteRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.prevEpoch !== 0) {
      writer.uint32(16).uint64(message.prevEpoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BarrierCompleteRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBarrierCompleteRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.prevEpoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
  return {
    requestId: "",
    status: undefined,
    createMviewProgress: [],
    syncedSstables: [],
    workerId: 0,
    checkpoint: false,
  };
}

export const BarrierCompleteResponse = {
  encode(message: BarrierCompleteResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.requestId !== "") {
      writer.uint32(10).string(message.requestId);
    }
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.createMviewProgress) {
      BarrierCompleteResponse_CreateMviewProgress.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    for (const v of message.syncedSstables) {
      BarrierCompleteResponse_GroupedSstableInfo.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    if (message.workerId !== 0) {
      writer.uint32(40).uint32(message.workerId);
    }
    if (message.checkpoint === true) {
      writer.uint32(48).bool(message.checkpoint);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BarrierCompleteResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBarrierCompleteResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.requestId = reader.string();
          break;
        case 2:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 3:
          message.createMviewProgress.push(BarrierCompleteResponse_CreateMviewProgress.decode(reader, reader.uint32()));
          break;
        case 4:
          message.syncedSstables.push(BarrierCompleteResponse_GroupedSstableInfo.decode(reader, reader.uint32()));
          break;
        case 5:
          message.workerId = reader.uint32();
          break;
        case 6:
          message.checkpoint = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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
      checkpoint: isSet(object.checkpoint) ? Boolean(object.checkpoint) : false,
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
    message.checkpoint !== undefined && (obj.checkpoint = message.checkpoint);
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
    message.checkpoint = object.checkpoint ?? false;
    return message;
  },
};

function createBaseBarrierCompleteResponse_CreateMviewProgress(): BarrierCompleteResponse_CreateMviewProgress {
  return { chainActorId: 0, done: false, consumedEpoch: 0 };
}

export const BarrierCompleteResponse_CreateMviewProgress = {
  encode(message: BarrierCompleteResponse_CreateMviewProgress, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.chainActorId !== 0) {
      writer.uint32(8).uint32(message.chainActorId);
    }
    if (message.done === true) {
      writer.uint32(16).bool(message.done);
    }
    if (message.consumedEpoch !== 0) {
      writer.uint32(24).uint64(message.consumedEpoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BarrierCompleteResponse_CreateMviewProgress {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBarrierCompleteResponse_CreateMviewProgress();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.chainActorId = reader.uint32();
          break;
        case 2:
          message.done = reader.bool();
          break;
        case 3:
          message.consumedEpoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): BarrierCompleteResponse_CreateMviewProgress {
    return {
      chainActorId: isSet(object.chainActorId) ? Number(object.chainActorId) : 0,
      done: isSet(object.done) ? Boolean(object.done) : false,
      consumedEpoch: isSet(object.consumedEpoch) ? Number(object.consumedEpoch) : 0,
    };
  },

  toJSON(message: BarrierCompleteResponse_CreateMviewProgress): unknown {
    const obj: any = {};
    message.chainActorId !== undefined && (obj.chainActorId = Math.round(message.chainActorId));
    message.done !== undefined && (obj.done = message.done);
    message.consumedEpoch !== undefined && (obj.consumedEpoch = Math.round(message.consumedEpoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse_CreateMviewProgress>, I>>(
    object: I,
  ): BarrierCompleteResponse_CreateMviewProgress {
    const message = createBaseBarrierCompleteResponse_CreateMviewProgress();
    message.chainActorId = object.chainActorId ?? 0;
    message.done = object.done ?? false;
    message.consumedEpoch = object.consumedEpoch ?? 0;
    return message;
  },
};

function createBaseBarrierCompleteResponse_GroupedSstableInfo(): BarrierCompleteResponse_GroupedSstableInfo {
  return { compactionGroupId: 0, sst: undefined };
}

export const BarrierCompleteResponse_GroupedSstableInfo = {
  encode(message: BarrierCompleteResponse_GroupedSstableInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.compactionGroupId !== 0) {
      writer.uint32(8).uint64(message.compactionGroupId);
    }
    if (message.sst !== undefined) {
      SstableInfo.encode(message.sst, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BarrierCompleteResponse_GroupedSstableInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBarrierCompleteResponse_GroupedSstableInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.compactionGroupId = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.sst = SstableInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): BarrierCompleteResponse_GroupedSstableInfo {
    return {
      compactionGroupId: isSet(object.compactionGroupId) ? Number(object.compactionGroupId) : 0,
      sst: isSet(object.sst) ? SstableInfo.fromJSON(object.sst) : undefined,
    };
  },

  toJSON(message: BarrierCompleteResponse_GroupedSstableInfo): unknown {
    const obj: any = {};
    message.compactionGroupId !== undefined && (obj.compactionGroupId = Math.round(message.compactionGroupId));
    message.sst !== undefined && (obj.sst = message.sst ? SstableInfo.toJSON(message.sst) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BarrierCompleteResponse_GroupedSstableInfo>, I>>(
    object: I,
  ): BarrierCompleteResponse_GroupedSstableInfo {
    const message = createBaseBarrierCompleteResponse_GroupedSstableInfo();
    message.compactionGroupId = object.compactionGroupId ?? 0;
    message.sst = (object.sst !== undefined && object.sst !== null) ? SstableInfo.fromPartial(object.sst) : undefined;
    return message;
  },
};

function createBaseBroadcastActorInfoTableResponse(): BroadcastActorInfoTableResponse {
  return { status: undefined };
}

export const BroadcastActorInfoTableResponse = {
  encode(message: BroadcastActorInfoTableResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): BroadcastActorInfoTableResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseBroadcastActorInfoTableResponse();
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
  return { status: undefined };
}

export const CreateSourceResponse = {
  encode(message: CreateSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
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
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CreateSourceResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: CreateSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreateSourceResponse>, I>>(object: I): CreateSourceResponse {
    const message = createBaseCreateSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
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
  return { status: undefined };
}

export const DropSourceResponse = {
  encode(message: DropSourceResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
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
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DropSourceResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: DropSourceResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DropSourceResponse>, I>>(object: I): DropSourceResponse {
    const message = createBaseDropSourceResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseSyncSourcesRequest(): SyncSourcesRequest {
  return { sources: [] };
}

export const SyncSourcesRequest = {
  encode(message: SyncSourcesRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.sources) {
      Source.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SyncSourcesRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSyncSourcesRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sources.push(Source.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SyncSourcesRequest {
    return { sources: Array.isArray(object?.sources) ? object.sources.map((e: any) => Source.fromJSON(e)) : [] };
  },

  toJSON(message: SyncSourcesRequest): unknown {
    const obj: any = {};
    if (message.sources) {
      obj.sources = message.sources.map((e) => e ? Source.toJSON(e) : undefined);
    } else {
      obj.sources = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SyncSourcesRequest>, I>>(object: I): SyncSourcesRequest {
    const message = createBaseSyncSourcesRequest();
    message.sources = object.sources?.map((e) => Source.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSyncSourcesResponse(): SyncSourcesResponse {
  return { status: undefined };
}

export const SyncSourcesResponse = {
  encode(message: SyncSourcesResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SyncSourcesResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSyncSourcesResponse();
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

  fromJSON(object: any): SyncSourcesResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: SyncSourcesResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SyncSourcesResponse>, I>>(object: I): SyncSourcesResponse {
    const message = createBaseSyncSourcesResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
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
