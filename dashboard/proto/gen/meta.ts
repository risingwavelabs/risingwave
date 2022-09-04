/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Database, Index, Schema, Sink, Source, StreamSourceInfo, Table } from "./catalog";
import {
  HostAddress,
  ParallelUnit,
  ParallelUnitMapping,
  Status,
  WorkerNode,
  WorkerType,
  workerTypeFromJSON,
  workerTypeToJSON,
} from "./common";
import { HummockSnapshot, HummockVersion, HummockVersionDeltas } from "./hummock";
import { ConnectorSplits } from "./source";
import {
  Dispatcher,
  FragmentType,
  fragmentTypeFromJSON,
  fragmentTypeToJSON,
  StreamActor,
  StreamNode,
} from "./stream_plan";
import { UserInfo } from "./user";

export const protobufPackage = "meta";

export interface HeartbeatRequest {
  nodeId: number;
  /** Lightweight info piggybacked by heartbeat request. */
  info: HeartbeatRequest_ExtraInfo[];
}

export interface HeartbeatRequest_ExtraInfo {
  hummockGcWatermark: number | undefined;
}

export interface HeartbeatResponse {
  status: Status | undefined;
}

/** Fragments of a Materialized View */
export interface TableFragments {
  tableId: number;
  fragments: { [key: number]: TableFragments_Fragment };
  actorStatus: { [key: number]: TableFragments_ActorStatus };
}

/** Current state of actor */
export enum TableFragments_ActorState {
  UNSPECIFIED = 0,
  /** INACTIVE - Initial state after creation */
  INACTIVE = 1,
  /** RUNNING - Running normally */
  RUNNING = 2,
  UNRECOGNIZED = -1,
}

export function tableFragments_ActorStateFromJSON(object: any): TableFragments_ActorState {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TableFragments_ActorState.UNSPECIFIED;
    case 1:
    case "INACTIVE":
      return TableFragments_ActorState.INACTIVE;
    case 2:
    case "RUNNING":
      return TableFragments_ActorState.RUNNING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TableFragments_ActorState.UNRECOGNIZED;
  }
}

export function tableFragments_ActorStateToJSON(object: TableFragments_ActorState): string {
  switch (object) {
    case TableFragments_ActorState.UNSPECIFIED:
      return "UNSPECIFIED";
    case TableFragments_ActorState.INACTIVE:
      return "INACTIVE";
    case TableFragments_ActorState.RUNNING:
      return "RUNNING";
    case TableFragments_ActorState.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

/** Runtime information of an actor */
export interface TableFragments_ActorStatus {
  /** Current on which parallel unit */
  parallelUnit:
    | ParallelUnit
    | undefined;
  /** Current state */
  state: TableFragments_ActorState;
}

export interface TableFragments_Fragment {
  fragmentId: number;
  fragmentType: FragmentType;
  distributionType: TableFragments_Fragment_FragmentDistributionType;
  actors: StreamActor[];
  /** Vnode mapping (which should be set in upstream dispatcher) of the fragment. */
  vnodeMapping: ParallelUnitMapping | undefined;
  stateTableIds: number[];
}

export enum TableFragments_Fragment_FragmentDistributionType {
  UNSPECIFIED = 0,
  SINGLE = 1,
  HASH = 2,
  UNRECOGNIZED = -1,
}

export function tableFragments_Fragment_FragmentDistributionTypeFromJSON(
  object: any,
): TableFragments_Fragment_FragmentDistributionType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED;
    case 1:
    case "SINGLE":
      return TableFragments_Fragment_FragmentDistributionType.SINGLE;
    case 2:
    case "HASH":
      return TableFragments_Fragment_FragmentDistributionType.HASH;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TableFragments_Fragment_FragmentDistributionType.UNRECOGNIZED;
  }
}

export function tableFragments_Fragment_FragmentDistributionTypeToJSON(
  object: TableFragments_Fragment_FragmentDistributionType,
): string {
  switch (object) {
    case TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED:
      return "UNSPECIFIED";
    case TableFragments_Fragment_FragmentDistributionType.SINGLE:
      return "SINGLE";
    case TableFragments_Fragment_FragmentDistributionType.HASH:
      return "HASH";
    case TableFragments_Fragment_FragmentDistributionType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface TableFragments_FragmentsEntry {
  key: number;
  value: TableFragments_Fragment | undefined;
}

export interface TableFragments_ActorStatusEntry {
  key: number;
  value: TableFragments_ActorStatus | undefined;
}

/** TODO: remove this when dashboard refactored. */
export interface ActorLocation {
  node: WorkerNode | undefined;
  actors: StreamActor[];
}

export interface FlushRequest {
}

export interface FlushResponse {
  status: Status | undefined;
  snapshot: HummockSnapshot | undefined;
}

export interface ListTableFragmentsRequest {
  tableIds: number[];
}

export interface ListTableFragmentsResponse {
  tableFragments: { [key: number]: ListTableFragmentsResponse_TableFragmentInfo };
}

export interface ListTableFragmentsResponse_ActorInfo {
  id: number;
  node: StreamNode | undefined;
  dispatcher: Dispatcher[];
}

export interface ListTableFragmentsResponse_FragmentInfo {
  id: number;
  actors: ListTableFragmentsResponse_ActorInfo[];
}

export interface ListTableFragmentsResponse_TableFragmentInfo {
  fragments: ListTableFragmentsResponse_FragmentInfo[];
}

export interface ListTableFragmentsResponse_TableFragmentsEntry {
  key: number;
  value: ListTableFragmentsResponse_TableFragmentInfo | undefined;
}

export interface AddWorkerNodeRequest {
  workerType: WorkerType;
  host: HostAddress | undefined;
  workerNodeParallelism: number;
}

export interface AddWorkerNodeResponse {
  status: Status | undefined;
  node: WorkerNode | undefined;
}

export interface ActivateWorkerNodeRequest {
  host: HostAddress | undefined;
}

export interface ActivateWorkerNodeResponse {
  status: Status | undefined;
}

export interface DeleteWorkerNodeRequest {
  host: HostAddress | undefined;
}

export interface DeleteWorkerNodeResponse {
  status: Status | undefined;
}

export interface ListAllNodesRequest {
  workerType: WorkerType;
  /** Whether to include nodes still starting */
  includeStartingNodes: boolean;
}

export interface ListAllNodesResponse {
  status: Status | undefined;
  nodes: WorkerNode[];
}

/** Below for notification service. */
export interface SubscribeRequest {
  workerType: WorkerType;
  host: HostAddress | undefined;
}

export interface MetaSnapshot {
  nodes: WorkerNode[];
  databases: Database[];
  schemas: Schema[];
  sources: Source[];
  sinks: Sink[];
  tables: Table[];
  indexes: Index[];
  users: UserInfo[];
  hummockVersion: HummockVersion | undefined;
  parallelUnitMappings: ParallelUnitMapping[];
  hummockSnapshot: HummockSnapshot | undefined;
}

export interface SubscribeResponse {
  status: Status | undefined;
  operation: SubscribeResponse_Operation;
  version: number;
  node: WorkerNode | undefined;
  database: Database | undefined;
  schema: Schema | undefined;
  table: Table | undefined;
  source: Source | undefined;
  sink: Sink | undefined;
  index: Index | undefined;
  user: UserInfo | undefined;
  hummockSnapshot: HummockSnapshot | undefined;
  parallelUnitMapping: ParallelUnitMapping | undefined;
  hummockVersionDeltas: HummockVersionDeltas | undefined;
  snapshot: MetaSnapshot | undefined;
}

export enum SubscribeResponse_Operation {
  UNSPECIFIED = 0,
  ADD = 1,
  DELETE = 2,
  UPDATE = 3,
  SNAPSHOT = 4,
  UNRECOGNIZED = -1,
}

export function subscribeResponse_OperationFromJSON(object: any): SubscribeResponse_Operation {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return SubscribeResponse_Operation.UNSPECIFIED;
    case 1:
    case "ADD":
      return SubscribeResponse_Operation.ADD;
    case 2:
    case "DELETE":
      return SubscribeResponse_Operation.DELETE;
    case 3:
    case "UPDATE":
      return SubscribeResponse_Operation.UPDATE;
    case 4:
    case "SNAPSHOT":
      return SubscribeResponse_Operation.SNAPSHOT;
    case -1:
    case "UNRECOGNIZED":
    default:
      return SubscribeResponse_Operation.UNRECOGNIZED;
  }
}

export function subscribeResponse_OperationToJSON(object: SubscribeResponse_Operation): string {
  switch (object) {
    case SubscribeResponse_Operation.UNSPECIFIED:
      return "UNSPECIFIED";
    case SubscribeResponse_Operation.ADD:
      return "ADD";
    case SubscribeResponse_Operation.DELETE:
      return "DELETE";
    case SubscribeResponse_Operation.UPDATE:
      return "UPDATE";
    case SubscribeResponse_Operation.SNAPSHOT:
      return "SNAPSHOT";
    case SubscribeResponse_Operation.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface MetaLeaderInfo {
  nodeAddress: string;
  leaseId: number;
}

export interface MetaLeaseInfo {
  leader: MetaLeaderInfo | undefined;
  leaseRegisterTime: number;
  leaseExpireTime: number;
}

export interface PauseRequest {
}

export interface PauseResponse {
}

export interface ResumeRequest {
}

export interface ResumeResponse {
}

export interface GetClusterInfoRequest {
}

export interface GetClusterInfoResponse {
  workerNodes: WorkerNode[];
  tableFragments: TableFragments[];
  actorSplits: { [key: number]: ConnectorSplits };
  streamSourceInfos: { [key: number]: StreamSourceInfo };
}

export interface GetClusterInfoResponse_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

export interface GetClusterInfoResponse_StreamSourceInfosEntry {
  key: number;
  value: StreamSourceInfo | undefined;
}

function createBaseHeartbeatRequest(): HeartbeatRequest {
  return { nodeId: 0, info: [] };
}

export const HeartbeatRequest = {
  encode(message: HeartbeatRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.nodeId !== 0) {
      writer.uint32(8).uint32(message.nodeId);
    }
    for (const v of message.info) {
      HeartbeatRequest_ExtraInfo.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HeartbeatRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHeartbeatRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.nodeId = reader.uint32();
          break;
        case 2:
          message.info.push(HeartbeatRequest_ExtraInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HeartbeatRequest {
    return {
      nodeId: isSet(object.nodeId) ? Number(object.nodeId) : 0,
      info: Array.isArray(object?.info) ? object.info.map((e: any) => HeartbeatRequest_ExtraInfo.fromJSON(e)) : [],
    };
  },

  toJSON(message: HeartbeatRequest): unknown {
    const obj: any = {};
    message.nodeId !== undefined && (obj.nodeId = Math.round(message.nodeId));
    if (message.info) {
      obj.info = message.info.map((e) => e ? HeartbeatRequest_ExtraInfo.toJSON(e) : undefined);
    } else {
      obj.info = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HeartbeatRequest>, I>>(object: I): HeartbeatRequest {
    const message = createBaseHeartbeatRequest();
    message.nodeId = object.nodeId ?? 0;
    message.info = object.info?.map((e) => HeartbeatRequest_ExtraInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHeartbeatRequest_ExtraInfo(): HeartbeatRequest_ExtraInfo {
  return { hummockGcWatermark: undefined };
}

export const HeartbeatRequest_ExtraInfo = {
  encode(message: HeartbeatRequest_ExtraInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.hummockGcWatermark !== undefined) {
      writer.uint32(8).uint64(message.hummockGcWatermark);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HeartbeatRequest_ExtraInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHeartbeatRequest_ExtraInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.hummockGcWatermark = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HeartbeatRequest_ExtraInfo {
    return { hummockGcWatermark: isSet(object.hummockGcWatermark) ? Number(object.hummockGcWatermark) : undefined };
  },

  toJSON(message: HeartbeatRequest_ExtraInfo): unknown {
    const obj: any = {};
    message.hummockGcWatermark !== undefined && (obj.hummockGcWatermark = Math.round(message.hummockGcWatermark));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HeartbeatRequest_ExtraInfo>, I>>(object: I): HeartbeatRequest_ExtraInfo {
    const message = createBaseHeartbeatRequest_ExtraInfo();
    message.hummockGcWatermark = object.hummockGcWatermark ?? undefined;
    return message;
  },
};

function createBaseHeartbeatResponse(): HeartbeatResponse {
  return { status: undefined };
}

export const HeartbeatResponse = {
  encode(message: HeartbeatResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HeartbeatResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHeartbeatResponse();
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

  fromJSON(object: any): HeartbeatResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: HeartbeatResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HeartbeatResponse>, I>>(object: I): HeartbeatResponse {
    const message = createBaseHeartbeatResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseTableFragments(): TableFragments {
  return { tableId: 0, fragments: {}, actorStatus: {} };
}

export const TableFragments = {
  encode(message: TableFragments, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableId !== 0) {
      writer.uint32(8).uint32(message.tableId);
    }
    Object.entries(message.fragments).forEach(([key, value]) => {
      TableFragments_FragmentsEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    Object.entries(message.actorStatus).forEach(([key, value]) => {
      TableFragments_ActorStatusEntry.encode({ key: key as any, value }, writer.uint32(26).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFragments {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFragments();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableId = reader.uint32();
          break;
        case 2:
          const entry2 = TableFragments_FragmentsEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.fragments[entry2.key] = entry2.value;
          }
          break;
        case 3:
          const entry3 = TableFragments_ActorStatusEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.actorStatus[entry3.key] = entry3.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFragments {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      fragments: isObject(object.fragments)
        ? Object.entries(object.fragments).reduce<{ [key: number]: TableFragments_Fragment }>((acc, [key, value]) => {
          acc[Number(key)] = TableFragments_Fragment.fromJSON(value);
          return acc;
        }, {})
        : {},
      actorStatus: isObject(object.actorStatus)
        ? Object.entries(object.actorStatus).reduce<{ [key: number]: TableFragments_ActorStatus }>(
          (acc, [key, value]) => {
            acc[Number(key)] = TableFragments_ActorStatus.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: TableFragments): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    obj.fragments = {};
    if (message.fragments) {
      Object.entries(message.fragments).forEach(([k, v]) => {
        obj.fragments[k] = TableFragments_Fragment.toJSON(v);
      });
    }
    obj.actorStatus = {};
    if (message.actorStatus) {
      Object.entries(message.actorStatus).forEach(([k, v]) => {
        obj.actorStatus[k] = TableFragments_ActorStatus.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments>, I>>(object: I): TableFragments {
    const message = createBaseTableFragments();
    message.tableId = object.tableId ?? 0;
    message.fragments = Object.entries(object.fragments ?? {}).reduce<{ [key: number]: TableFragments_Fragment }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = TableFragments_Fragment.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.actorStatus = Object.entries(object.actorStatus ?? {}).reduce<
      { [key: number]: TableFragments_ActorStatus }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = TableFragments_ActorStatus.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseTableFragments_ActorStatus(): TableFragments_ActorStatus {
  return { parallelUnit: undefined, state: 0 };
}

export const TableFragments_ActorStatus = {
  encode(message: TableFragments_ActorStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.parallelUnit !== undefined) {
      ParallelUnit.encode(message.parallelUnit, writer.uint32(10).fork()).ldelim();
    }
    if (message.state !== 0) {
      writer.uint32(16).int32(message.state);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFragments_ActorStatus {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFragments_ActorStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.parallelUnit = ParallelUnit.decode(reader, reader.uint32());
          break;
        case 2:
          message.state = reader.int32() as any;
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFragments_ActorStatus {
    return {
      parallelUnit: isSet(object.parallelUnit) ? ParallelUnit.fromJSON(object.parallelUnit) : undefined,
      state: isSet(object.state) ? tableFragments_ActorStateFromJSON(object.state) : 0,
    };
  },

  toJSON(message: TableFragments_ActorStatus): unknown {
    const obj: any = {};
    message.parallelUnit !== undefined &&
      (obj.parallelUnit = message.parallelUnit ? ParallelUnit.toJSON(message.parallelUnit) : undefined);
    message.state !== undefined && (obj.state = tableFragments_ActorStateToJSON(message.state));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_ActorStatus>, I>>(object: I): TableFragments_ActorStatus {
    const message = createBaseTableFragments_ActorStatus();
    message.parallelUnit = (object.parallelUnit !== undefined && object.parallelUnit !== null)
      ? ParallelUnit.fromPartial(object.parallelUnit)
      : undefined;
    message.state = object.state ?? 0;
    return message;
  },
};

function createBaseTableFragments_Fragment(): TableFragments_Fragment {
  return {
    fragmentId: 0,
    fragmentType: 0,
    distributionType: 0,
    actors: [],
    vnodeMapping: undefined,
    stateTableIds: [],
  };
}

export const TableFragments_Fragment = {
  encode(message: TableFragments_Fragment, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.fragmentId !== 0) {
      writer.uint32(8).uint32(message.fragmentId);
    }
    if (message.fragmentType !== 0) {
      writer.uint32(16).int32(message.fragmentType);
    }
    if (message.distributionType !== 0) {
      writer.uint32(24).int32(message.distributionType);
    }
    for (const v of message.actors) {
      StreamActor.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    if (message.vnodeMapping !== undefined) {
      ParallelUnitMapping.encode(message.vnodeMapping, writer.uint32(42).fork()).ldelim();
    }
    writer.uint32(50).fork();
    for (const v of message.stateTableIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFragments_Fragment {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFragments_Fragment();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.fragmentId = reader.uint32();
          break;
        case 2:
          message.fragmentType = reader.int32() as any;
          break;
        case 3:
          message.distributionType = reader.int32() as any;
          break;
        case 4:
          message.actors.push(StreamActor.decode(reader, reader.uint32()));
          break;
        case 5:
          message.vnodeMapping = ParallelUnitMapping.decode(reader, reader.uint32());
          break;
        case 6:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.stateTableIds.push(reader.uint32());
            }
          } else {
            message.stateTableIds.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFragments_Fragment {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      fragmentType: isSet(object.fragmentType) ? fragmentTypeFromJSON(object.fragmentType) : 0,
      distributionType: isSet(object.distributionType)
        ? tableFragments_Fragment_FragmentDistributionTypeFromJSON(object.distributionType)
        : 0,
      actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => StreamActor.fromJSON(e)) : [],
      vnodeMapping: isSet(object.vnodeMapping) ? ParallelUnitMapping.fromJSON(object.vnodeMapping) : undefined,
      stateTableIds: Array.isArray(object?.stateTableIds) ? object.stateTableIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: TableFragments_Fragment): unknown {
    const obj: any = {};
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.fragmentType !== undefined && (obj.fragmentType = fragmentTypeToJSON(message.fragmentType));
    message.distributionType !== undefined &&
      (obj.distributionType = tableFragments_Fragment_FragmentDistributionTypeToJSON(message.distributionType));
    if (message.actors) {
      obj.actors = message.actors.map((e) => e ? StreamActor.toJSON(e) : undefined);
    } else {
      obj.actors = [];
    }
    message.vnodeMapping !== undefined &&
      (obj.vnodeMapping = message.vnodeMapping ? ParallelUnitMapping.toJSON(message.vnodeMapping) : undefined);
    if (message.stateTableIds) {
      obj.stateTableIds = message.stateTableIds.map((e) => Math.round(e));
    } else {
      obj.stateTableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_Fragment>, I>>(object: I): TableFragments_Fragment {
    const message = createBaseTableFragments_Fragment();
    message.fragmentId = object.fragmentId ?? 0;
    message.fragmentType = object.fragmentType ?? 0;
    message.distributionType = object.distributionType ?? 0;
    message.actors = object.actors?.map((e) => StreamActor.fromPartial(e)) || [];
    message.vnodeMapping = (object.vnodeMapping !== undefined && object.vnodeMapping !== null)
      ? ParallelUnitMapping.fromPartial(object.vnodeMapping)
      : undefined;
    message.stateTableIds = object.stateTableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseTableFragments_FragmentsEntry(): TableFragments_FragmentsEntry {
  return { key: 0, value: undefined };
}

export const TableFragments_FragmentsEntry = {
  encode(message: TableFragments_FragmentsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      TableFragments_Fragment.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFragments_FragmentsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFragments_FragmentsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = TableFragments_Fragment.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFragments_FragmentsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableFragments_Fragment.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: TableFragments_FragmentsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? TableFragments_Fragment.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_FragmentsEntry>, I>>(
    object: I,
  ): TableFragments_FragmentsEntry {
    const message = createBaseTableFragments_FragmentsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableFragments_Fragment.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseTableFragments_ActorStatusEntry(): TableFragments_ActorStatusEntry {
  return { key: 0, value: undefined };
}

export const TableFragments_ActorStatusEntry = {
  encode(message: TableFragments_ActorStatusEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      TableFragments_ActorStatus.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFragments_ActorStatusEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFragments_ActorStatusEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = TableFragments_ActorStatus.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFragments_ActorStatusEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableFragments_ActorStatus.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: TableFragments_ActorStatusEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? TableFragments_ActorStatus.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_ActorStatusEntry>, I>>(
    object: I,
  ): TableFragments_ActorStatusEntry {
    const message = createBaseTableFragments_ActorStatusEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableFragments_ActorStatus.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseActorLocation(): ActorLocation {
  return { node: undefined, actors: [] };
}

export const ActorLocation = {
  encode(message: ActorLocation, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.node !== undefined) {
      WorkerNode.encode(message.node, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.actors) {
      StreamActor.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ActorLocation {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseActorLocation();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.node = WorkerNode.decode(reader, reader.uint32());
          break;
        case 2:
          message.actors.push(StreamActor.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ActorLocation {
    return {
      node: isSet(object.node) ? WorkerNode.fromJSON(object.node) : undefined,
      actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => StreamActor.fromJSON(e)) : [],
    };
  },

  toJSON(message: ActorLocation): unknown {
    const obj: any = {};
    message.node !== undefined && (obj.node = message.node ? WorkerNode.toJSON(message.node) : undefined);
    if (message.actors) {
      obj.actors = message.actors.map((e) => e ? StreamActor.toJSON(e) : undefined);
    } else {
      obj.actors = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ActorLocation>, I>>(object: I): ActorLocation {
    const message = createBaseActorLocation();
    message.node = (object.node !== undefined && object.node !== null)
      ? WorkerNode.fromPartial(object.node)
      : undefined;
    message.actors = object.actors?.map((e) => StreamActor.fromPartial(e)) || [];
    return message;
  },
};

function createBaseFlushRequest(): FlushRequest {
  return {};
}

export const FlushRequest = {
  encode(_: FlushRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FlushRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFlushRequest();
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

  fromJSON(_: any): FlushRequest {
    return {};
  },

  toJSON(_: FlushRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FlushRequest>, I>>(_: I): FlushRequest {
    const message = createBaseFlushRequest();
    return message;
  },
};

function createBaseFlushResponse(): FlushResponse {
  return { status: undefined, snapshot: undefined };
}

export const FlushResponse = {
  encode(message: FlushResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.snapshot !== undefined) {
      HummockSnapshot.encode(message.snapshot, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FlushResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFlushResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.snapshot = HummockSnapshot.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): FlushResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      snapshot: isSet(object.snapshot) ? HummockSnapshot.fromJSON(object.snapshot) : undefined,
    };
  },

  toJSON(message: FlushResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.snapshot !== undefined &&
      (obj.snapshot = message.snapshot ? HummockSnapshot.toJSON(message.snapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FlushResponse>, I>>(object: I): FlushResponse {
    const message = createBaseFlushResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.snapshot = (object.snapshot !== undefined && object.snapshot !== null)
      ? HummockSnapshot.fromPartial(object.snapshot)
      : undefined;
    return message;
  },
};

function createBaseListTableFragmentsRequest(): ListTableFragmentsRequest {
  return { tableIds: [] };
}

export const ListTableFragmentsRequest = {
  encode(message: ListTableFragmentsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.tableIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.tableIds.push(reader.uint32());
            }
          } else {
            message.tableIds.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsRequest {
    return { tableIds: Array.isArray(object?.tableIds) ? object.tableIds.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: ListTableFragmentsRequest): unknown {
    const obj: any = {};
    if (message.tableIds) {
      obj.tableIds = message.tableIds.map((e) => Math.round(e));
    } else {
      obj.tableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsRequest>, I>>(object: I): ListTableFragmentsRequest {
    const message = createBaseListTableFragmentsRequest();
    message.tableIds = object.tableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseListTableFragmentsResponse(): ListTableFragmentsResponse {
  return { tableFragments: {} };
}

export const ListTableFragmentsResponse = {
  encode(message: ListTableFragmentsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    Object.entries(message.tableFragments).forEach(([key, value]) => {
      ListTableFragmentsResponse_TableFragmentsEntry.encode({ key: key as any, value }, writer.uint32(10).fork())
        .ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          const entry1 = ListTableFragmentsResponse_TableFragmentsEntry.decode(reader, reader.uint32());
          if (entry1.value !== undefined) {
            message.tableFragments[entry1.key] = entry1.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsResponse {
    return {
      tableFragments: isObject(object.tableFragments)
        ? Object.entries(object.tableFragments).reduce<{ [key: number]: ListTableFragmentsResponse_TableFragmentInfo }>(
          (acc, [key, value]) => {
            acc[Number(key)] = ListTableFragmentsResponse_TableFragmentInfo.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: ListTableFragmentsResponse): unknown {
    const obj: any = {};
    obj.tableFragments = {};
    if (message.tableFragments) {
      Object.entries(message.tableFragments).forEach(([k, v]) => {
        obj.tableFragments[k] = ListTableFragmentsResponse_TableFragmentInfo.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse>, I>>(object: I): ListTableFragmentsResponse {
    const message = createBaseListTableFragmentsResponse();
    message.tableFragments = Object.entries(object.tableFragments ?? {}).reduce<
      { [key: number]: ListTableFragmentsResponse_TableFragmentInfo }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = ListTableFragmentsResponse_TableFragmentInfo.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseListTableFragmentsResponse_ActorInfo(): ListTableFragmentsResponse_ActorInfo {
  return { id: 0, node: undefined, dispatcher: [] };
}

export const ListTableFragmentsResponse_ActorInfo = {
  encode(message: ListTableFragmentsResponse_ActorInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    if (message.node !== undefined) {
      StreamNode.encode(message.node, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.dispatcher) {
      Dispatcher.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsResponse_ActorInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsResponse_ActorInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 2:
          message.node = StreamNode.decode(reader, reader.uint32());
          break;
        case 3:
          message.dispatcher.push(Dispatcher.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsResponse_ActorInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      node: isSet(object.node) ? StreamNode.fromJSON(object.node) : undefined,
      dispatcher: Array.isArray(object?.dispatcher) ? object.dispatcher.map((e: any) => Dispatcher.fromJSON(e)) : [],
    };
  },

  toJSON(message: ListTableFragmentsResponse_ActorInfo): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.node !== undefined && (obj.node = message.node ? StreamNode.toJSON(message.node) : undefined);
    if (message.dispatcher) {
      obj.dispatcher = message.dispatcher.map((e) => e ? Dispatcher.toJSON(e) : undefined);
    } else {
      obj.dispatcher = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse_ActorInfo>, I>>(
    object: I,
  ): ListTableFragmentsResponse_ActorInfo {
    const message = createBaseListTableFragmentsResponse_ActorInfo();
    message.id = object.id ?? 0;
    message.node = (object.node !== undefined && object.node !== null)
      ? StreamNode.fromPartial(object.node)
      : undefined;
    message.dispatcher = object.dispatcher?.map((e) => Dispatcher.fromPartial(e)) || [];
    return message;
  },
};

function createBaseListTableFragmentsResponse_FragmentInfo(): ListTableFragmentsResponse_FragmentInfo {
  return { id: 0, actors: [] };
}

export const ListTableFragmentsResponse_FragmentInfo = {
  encode(message: ListTableFragmentsResponse_FragmentInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint32(message.id);
    }
    for (const v of message.actors) {
      ListTableFragmentsResponse_ActorInfo.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsResponse_FragmentInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsResponse_FragmentInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = reader.uint32();
          break;
        case 4:
          message.actors.push(ListTableFragmentsResponse_ActorInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsResponse_FragmentInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      actors: Array.isArray(object?.actors)
        ? object.actors.map((e: any) => ListTableFragmentsResponse_ActorInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: ListTableFragmentsResponse_FragmentInfo): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    if (message.actors) {
      obj.actors = message.actors.map((e) => e ? ListTableFragmentsResponse_ActorInfo.toJSON(e) : undefined);
    } else {
      obj.actors = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse_FragmentInfo>, I>>(
    object: I,
  ): ListTableFragmentsResponse_FragmentInfo {
    const message = createBaseListTableFragmentsResponse_FragmentInfo();
    message.id = object.id ?? 0;
    message.actors = object.actors?.map((e) => ListTableFragmentsResponse_ActorInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseListTableFragmentsResponse_TableFragmentInfo(): ListTableFragmentsResponse_TableFragmentInfo {
  return { fragments: [] };
}

export const ListTableFragmentsResponse_TableFragmentInfo = {
  encode(message: ListTableFragmentsResponse_TableFragmentInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.fragments) {
      ListTableFragmentsResponse_FragmentInfo.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsResponse_TableFragmentInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsResponse_TableFragmentInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.fragments.push(ListTableFragmentsResponse_FragmentInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsResponse_TableFragmentInfo {
    return {
      fragments: Array.isArray(object?.fragments)
        ? object.fragments.map((e: any) => ListTableFragmentsResponse_FragmentInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: ListTableFragmentsResponse_TableFragmentInfo): unknown {
    const obj: any = {};
    if (message.fragments) {
      obj.fragments = message.fragments.map((e) => e ? ListTableFragmentsResponse_FragmentInfo.toJSON(e) : undefined);
    } else {
      obj.fragments = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse_TableFragmentInfo>, I>>(
    object: I,
  ): ListTableFragmentsResponse_TableFragmentInfo {
    const message = createBaseListTableFragmentsResponse_TableFragmentInfo();
    message.fragments = object.fragments?.map((e) => ListTableFragmentsResponse_FragmentInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseListTableFragmentsResponse_TableFragmentsEntry(): ListTableFragmentsResponse_TableFragmentsEntry {
  return { key: 0, value: undefined };
}

export const ListTableFragmentsResponse_TableFragmentsEntry = {
  encode(
    message: ListTableFragmentsResponse_TableFragmentsEntry,
    writer: _m0.Writer = _m0.Writer.create(),
  ): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      ListTableFragmentsResponse_TableFragmentInfo.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListTableFragmentsResponse_TableFragmentsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListTableFragmentsResponse_TableFragmentsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = ListTableFragmentsResponse_TableFragmentInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListTableFragmentsResponse_TableFragmentsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ListTableFragmentsResponse_TableFragmentInfo.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: ListTableFragmentsResponse_TableFragmentsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? ListTableFragmentsResponse_TableFragmentInfo.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse_TableFragmentsEntry>, I>>(
    object: I,
  ): ListTableFragmentsResponse_TableFragmentsEntry {
    const message = createBaseListTableFragmentsResponse_TableFragmentsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ListTableFragmentsResponse_TableFragmentInfo.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseAddWorkerNodeRequest(): AddWorkerNodeRequest {
  return { workerType: 0, host: undefined, workerNodeParallelism: 0 };
}

export const AddWorkerNodeRequest = {
  encode(message: AddWorkerNodeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.workerType !== 0) {
      writer.uint32(8).int32(message.workerType);
    }
    if (message.host !== undefined) {
      HostAddress.encode(message.host, writer.uint32(18).fork()).ldelim();
    }
    if (message.workerNodeParallelism !== 0) {
      writer.uint32(24).uint64(message.workerNodeParallelism);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AddWorkerNodeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAddWorkerNodeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.workerType = reader.int32() as any;
          break;
        case 2:
          message.host = HostAddress.decode(reader, reader.uint32());
          break;
        case 3:
          message.workerNodeParallelism = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AddWorkerNodeRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : 0,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
      workerNodeParallelism: isSet(object.workerNodeParallelism) ? Number(object.workerNodeParallelism) : 0,
    };
  },

  toJSON(message: AddWorkerNodeRequest): unknown {
    const obj: any = {};
    message.workerType !== undefined && (obj.workerType = workerTypeToJSON(message.workerType));
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    message.workerNodeParallelism !== undefined &&
      (obj.workerNodeParallelism = Math.round(message.workerNodeParallelism));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddWorkerNodeRequest>, I>>(object: I): AddWorkerNodeRequest {
    const message = createBaseAddWorkerNodeRequest();
    message.workerType = object.workerType ?? 0;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    message.workerNodeParallelism = object.workerNodeParallelism ?? 0;
    return message;
  },
};

function createBaseAddWorkerNodeResponse(): AddWorkerNodeResponse {
  return { status: undefined, node: undefined };
}

export const AddWorkerNodeResponse = {
  encode(message: AddWorkerNodeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.node !== undefined) {
      WorkerNode.encode(message.node, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AddWorkerNodeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAddWorkerNodeResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.node = WorkerNode.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AddWorkerNodeResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      node: isSet(object.node) ? WorkerNode.fromJSON(object.node) : undefined,
    };
  },

  toJSON(message: AddWorkerNodeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.node !== undefined && (obj.node = message.node ? WorkerNode.toJSON(message.node) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddWorkerNodeResponse>, I>>(object: I): AddWorkerNodeResponse {
    const message = createBaseAddWorkerNodeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.node = (object.node !== undefined && object.node !== null)
      ? WorkerNode.fromPartial(object.node)
      : undefined;
    return message;
  },
};

function createBaseActivateWorkerNodeRequest(): ActivateWorkerNodeRequest {
  return { host: undefined };
}

export const ActivateWorkerNodeRequest = {
  encode(message: ActivateWorkerNodeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.host !== undefined) {
      HostAddress.encode(message.host, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ActivateWorkerNodeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseActivateWorkerNodeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.host = HostAddress.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ActivateWorkerNodeRequest {
    return { host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined };
  },

  toJSON(message: ActivateWorkerNodeRequest): unknown {
    const obj: any = {};
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ActivateWorkerNodeRequest>, I>>(object: I): ActivateWorkerNodeRequest {
    const message = createBaseActivateWorkerNodeRequest();
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    return message;
  },
};

function createBaseActivateWorkerNodeResponse(): ActivateWorkerNodeResponse {
  return { status: undefined };
}

export const ActivateWorkerNodeResponse = {
  encode(message: ActivateWorkerNodeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ActivateWorkerNodeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseActivateWorkerNodeResponse();
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

  fromJSON(object: any): ActivateWorkerNodeResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: ActivateWorkerNodeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ActivateWorkerNodeResponse>, I>>(object: I): ActivateWorkerNodeResponse {
    const message = createBaseActivateWorkerNodeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseDeleteWorkerNodeRequest(): DeleteWorkerNodeRequest {
  return { host: undefined };
}

export const DeleteWorkerNodeRequest = {
  encode(message: DeleteWorkerNodeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.host !== undefined) {
      HostAddress.encode(message.host, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DeleteWorkerNodeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDeleteWorkerNodeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.host = HostAddress.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DeleteWorkerNodeRequest {
    return { host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined };
  },

  toJSON(message: DeleteWorkerNodeRequest): unknown {
    const obj: any = {};
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeleteWorkerNodeRequest>, I>>(object: I): DeleteWorkerNodeRequest {
    const message = createBaseDeleteWorkerNodeRequest();
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    return message;
  },
};

function createBaseDeleteWorkerNodeResponse(): DeleteWorkerNodeResponse {
  return { status: undefined };
}

export const DeleteWorkerNodeResponse = {
  encode(message: DeleteWorkerNodeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DeleteWorkerNodeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDeleteWorkerNodeResponse();
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

  fromJSON(object: any): DeleteWorkerNodeResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: DeleteWorkerNodeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeleteWorkerNodeResponse>, I>>(object: I): DeleteWorkerNodeResponse {
    const message = createBaseDeleteWorkerNodeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseListAllNodesRequest(): ListAllNodesRequest {
  return { workerType: 0, includeStartingNodes: false };
}

export const ListAllNodesRequest = {
  encode(message: ListAllNodesRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.workerType !== 0) {
      writer.uint32(8).int32(message.workerType);
    }
    if (message.includeStartingNodes === true) {
      writer.uint32(16).bool(message.includeStartingNodes);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListAllNodesRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListAllNodesRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.workerType = reader.int32() as any;
          break;
        case 2:
          message.includeStartingNodes = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListAllNodesRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : 0,
      includeStartingNodes: isSet(object.includeStartingNodes) ? Boolean(object.includeStartingNodes) : false,
    };
  },

  toJSON(message: ListAllNodesRequest): unknown {
    const obj: any = {};
    message.workerType !== undefined && (obj.workerType = workerTypeToJSON(message.workerType));
    message.includeStartingNodes !== undefined && (obj.includeStartingNodes = message.includeStartingNodes);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListAllNodesRequest>, I>>(object: I): ListAllNodesRequest {
    const message = createBaseListAllNodesRequest();
    message.workerType = object.workerType ?? 0;
    message.includeStartingNodes = object.includeStartingNodes ?? false;
    return message;
  },
};

function createBaseListAllNodesResponse(): ListAllNodesResponse {
  return { status: undefined, nodes: [] };
}

export const ListAllNodesResponse = {
  encode(message: ListAllNodesResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.nodes) {
      WorkerNode.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListAllNodesResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListAllNodesResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.nodes.push(WorkerNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListAllNodesResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      nodes: Array.isArray(object?.nodes) ? object.nodes.map((e: any) => WorkerNode.fromJSON(e)) : [],
    };
  },

  toJSON(message: ListAllNodesResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    if (message.nodes) {
      obj.nodes = message.nodes.map((e) => e ? WorkerNode.toJSON(e) : undefined);
    } else {
      obj.nodes = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListAllNodesResponse>, I>>(object: I): ListAllNodesResponse {
    const message = createBaseListAllNodesResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.nodes = object.nodes?.map((e) => WorkerNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSubscribeRequest(): SubscribeRequest {
  return { workerType: 0, host: undefined };
}

export const SubscribeRequest = {
  encode(message: SubscribeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.workerType !== 0) {
      writer.uint32(8).int32(message.workerType);
    }
    if (message.host !== undefined) {
      HostAddress.encode(message.host, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SubscribeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSubscribeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.workerType = reader.int32() as any;
          break;
        case 2:
          message.host = HostAddress.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SubscribeRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : 0,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
    };
  },

  toJSON(message: SubscribeRequest): unknown {
    const obj: any = {};
    message.workerType !== undefined && (obj.workerType = workerTypeToJSON(message.workerType));
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeRequest>, I>>(object: I): SubscribeRequest {
    const message = createBaseSubscribeRequest();
    message.workerType = object.workerType ?? 0;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    return message;
  },
};

function createBaseMetaSnapshot(): MetaSnapshot {
  return {
    nodes: [],
    databases: [],
    schemas: [],
    sources: [],
    sinks: [],
    tables: [],
    indexes: [],
    users: [],
    hummockVersion: undefined,
    parallelUnitMappings: [],
    hummockSnapshot: undefined,
  };
}

export const MetaSnapshot = {
  encode(message: MetaSnapshot, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.nodes) {
      WorkerNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.databases) {
      Database.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    for (const v of message.schemas) {
      Schema.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    for (const v of message.sources) {
      Source.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    for (const v of message.sinks) {
      Sink.encode(v!, writer.uint32(42).fork()).ldelim();
    }
    for (const v of message.tables) {
      Table.encode(v!, writer.uint32(50).fork()).ldelim();
    }
    for (const v of message.indexes) {
      Index.encode(v!, writer.uint32(58).fork()).ldelim();
    }
    for (const v of message.users) {
      UserInfo.encode(v!, writer.uint32(66).fork()).ldelim();
    }
    if (message.hummockVersion !== undefined) {
      HummockVersion.encode(message.hummockVersion, writer.uint32(74).fork()).ldelim();
    }
    for (const v of message.parallelUnitMappings) {
      ParallelUnitMapping.encode(v!, writer.uint32(82).fork()).ldelim();
    }
    if (message.hummockSnapshot !== undefined) {
      HummockSnapshot.encode(message.hummockSnapshot, writer.uint32(90).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MetaSnapshot {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMetaSnapshot();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.nodes.push(WorkerNode.decode(reader, reader.uint32()));
          break;
        case 2:
          message.databases.push(Database.decode(reader, reader.uint32()));
          break;
        case 3:
          message.schemas.push(Schema.decode(reader, reader.uint32()));
          break;
        case 4:
          message.sources.push(Source.decode(reader, reader.uint32()));
          break;
        case 5:
          message.sinks.push(Sink.decode(reader, reader.uint32()));
          break;
        case 6:
          message.tables.push(Table.decode(reader, reader.uint32()));
          break;
        case 7:
          message.indexes.push(Index.decode(reader, reader.uint32()));
          break;
        case 8:
          message.users.push(UserInfo.decode(reader, reader.uint32()));
          break;
        case 9:
          message.hummockVersion = HummockVersion.decode(reader, reader.uint32());
          break;
        case 10:
          message.parallelUnitMappings.push(ParallelUnitMapping.decode(reader, reader.uint32()));
          break;
        case 11:
          message.hummockSnapshot = HummockSnapshot.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): MetaSnapshot {
    return {
      nodes: Array.isArray(object?.nodes) ? object.nodes.map((e: any) => WorkerNode.fromJSON(e)) : [],
      databases: Array.isArray(object?.databases) ? object.databases.map((e: any) => Database.fromJSON(e)) : [],
      schemas: Array.isArray(object?.schemas) ? object.schemas.map((e: any) => Schema.fromJSON(e)) : [],
      sources: Array.isArray(object?.sources) ? object.sources.map((e: any) => Source.fromJSON(e)) : [],
      sinks: Array.isArray(object?.sinks) ? object.sinks.map((e: any) => Sink.fromJSON(e)) : [],
      tables: Array.isArray(object?.tables) ? object.tables.map((e: any) => Table.fromJSON(e)) : [],
      indexes: Array.isArray(object?.indexes) ? object.indexes.map((e: any) => Index.fromJSON(e)) : [],
      users: Array.isArray(object?.users) ? object.users.map((e: any) => UserInfo.fromJSON(e)) : [],
      hummockVersion: isSet(object.hummockVersion) ? HummockVersion.fromJSON(object.hummockVersion) : undefined,
      parallelUnitMappings: Array.isArray(object?.parallelUnitMappings)
        ? object.parallelUnitMappings.map((e: any) => ParallelUnitMapping.fromJSON(e))
        : [],
      hummockSnapshot: isSet(object.hummockSnapshot) ? HummockSnapshot.fromJSON(object.hummockSnapshot) : undefined,
    };
  },

  toJSON(message: MetaSnapshot): unknown {
    const obj: any = {};
    if (message.nodes) {
      obj.nodes = message.nodes.map((e) => e ? WorkerNode.toJSON(e) : undefined);
    } else {
      obj.nodes = [];
    }
    if (message.databases) {
      obj.databases = message.databases.map((e) => e ? Database.toJSON(e) : undefined);
    } else {
      obj.databases = [];
    }
    if (message.schemas) {
      obj.schemas = message.schemas.map((e) => e ? Schema.toJSON(e) : undefined);
    } else {
      obj.schemas = [];
    }
    if (message.sources) {
      obj.sources = message.sources.map((e) => e ? Source.toJSON(e) : undefined);
    } else {
      obj.sources = [];
    }
    if (message.sinks) {
      obj.sinks = message.sinks.map((e) => e ? Sink.toJSON(e) : undefined);
    } else {
      obj.sinks = [];
    }
    if (message.tables) {
      obj.tables = message.tables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.tables = [];
    }
    if (message.indexes) {
      obj.indexes = message.indexes.map((e) => e ? Index.toJSON(e) : undefined);
    } else {
      obj.indexes = [];
    }
    if (message.users) {
      obj.users = message.users.map((e) => e ? UserInfo.toJSON(e) : undefined);
    } else {
      obj.users = [];
    }
    message.hummockVersion !== undefined &&
      (obj.hummockVersion = message.hummockVersion ? HummockVersion.toJSON(message.hummockVersion) : undefined);
    if (message.parallelUnitMappings) {
      obj.parallelUnitMappings = message.parallelUnitMappings.map((e) => e ? ParallelUnitMapping.toJSON(e) : undefined);
    } else {
      obj.parallelUnitMappings = [];
    }
    message.hummockSnapshot !== undefined &&
      (obj.hummockSnapshot = message.hummockSnapshot ? HummockSnapshot.toJSON(message.hummockSnapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaSnapshot>, I>>(object: I): MetaSnapshot {
    const message = createBaseMetaSnapshot();
    message.nodes = object.nodes?.map((e) => WorkerNode.fromPartial(e)) || [];
    message.databases = object.databases?.map((e) => Database.fromPartial(e)) || [];
    message.schemas = object.schemas?.map((e) => Schema.fromPartial(e)) || [];
    message.sources = object.sources?.map((e) => Source.fromPartial(e)) || [];
    message.sinks = object.sinks?.map((e) => Sink.fromPartial(e)) || [];
    message.tables = object.tables?.map((e) => Table.fromPartial(e)) || [];
    message.indexes = object.indexes?.map((e) => Index.fromPartial(e)) || [];
    message.users = object.users?.map((e) => UserInfo.fromPartial(e)) || [];
    message.hummockVersion = (object.hummockVersion !== undefined && object.hummockVersion !== null)
      ? HummockVersion.fromPartial(object.hummockVersion)
      : undefined;
    message.parallelUnitMappings = object.parallelUnitMappings?.map((e) => ParallelUnitMapping.fromPartial(e)) || [];
    message.hummockSnapshot = (object.hummockSnapshot !== undefined && object.hummockSnapshot !== null)
      ? HummockSnapshot.fromPartial(object.hummockSnapshot)
      : undefined;
    return message;
  },
};

function createBaseSubscribeResponse(): SubscribeResponse {
  return {
    status: undefined,
    operation: 0,
    version: 0,
    node: undefined,
    database: undefined,
    schema: undefined,
    table: undefined,
    source: undefined,
    sink: undefined,
    index: undefined,
    user: undefined,
    hummockSnapshot: undefined,
    parallelUnitMapping: undefined,
    hummockVersionDeltas: undefined,
    snapshot: undefined,
  };
}

export const SubscribeResponse = {
  encode(message: SubscribeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.operation !== 0) {
      writer.uint32(16).int32(message.operation);
    }
    if (message.version !== 0) {
      writer.uint32(24).uint64(message.version);
    }
    if (message.node !== undefined) {
      WorkerNode.encode(message.node, writer.uint32(34).fork()).ldelim();
    }
    if (message.database !== undefined) {
      Database.encode(message.database, writer.uint32(42).fork()).ldelim();
    }
    if (message.schema !== undefined) {
      Schema.encode(message.schema, writer.uint32(50).fork()).ldelim();
    }
    if (message.table !== undefined) {
      Table.encode(message.table, writer.uint32(58).fork()).ldelim();
    }
    if (message.source !== undefined) {
      Source.encode(message.source, writer.uint32(66).fork()).ldelim();
    }
    if (message.sink !== undefined) {
      Sink.encode(message.sink, writer.uint32(74).fork()).ldelim();
    }
    if (message.index !== undefined) {
      Index.encode(message.index, writer.uint32(82).fork()).ldelim();
    }
    if (message.user !== undefined) {
      UserInfo.encode(message.user, writer.uint32(90).fork()).ldelim();
    }
    if (message.hummockSnapshot !== undefined) {
      HummockSnapshot.encode(message.hummockSnapshot, writer.uint32(98).fork()).ldelim();
    }
    if (message.parallelUnitMapping !== undefined) {
      ParallelUnitMapping.encode(message.parallelUnitMapping, writer.uint32(106).fork()).ldelim();
    }
    if (message.hummockVersionDeltas !== undefined) {
      HummockVersionDeltas.encode(message.hummockVersionDeltas, writer.uint32(114).fork()).ldelim();
    }
    if (message.snapshot !== undefined) {
      MetaSnapshot.encode(message.snapshot, writer.uint32(162).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SubscribeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSubscribeResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.operation = reader.int32() as any;
          break;
        case 3:
          message.version = longToNumber(reader.uint64() as Long);
          break;
        case 4:
          message.node = WorkerNode.decode(reader, reader.uint32());
          break;
        case 5:
          message.database = Database.decode(reader, reader.uint32());
          break;
        case 6:
          message.schema = Schema.decode(reader, reader.uint32());
          break;
        case 7:
          message.table = Table.decode(reader, reader.uint32());
          break;
        case 8:
          message.source = Source.decode(reader, reader.uint32());
          break;
        case 9:
          message.sink = Sink.decode(reader, reader.uint32());
          break;
        case 10:
          message.index = Index.decode(reader, reader.uint32());
          break;
        case 11:
          message.user = UserInfo.decode(reader, reader.uint32());
          break;
        case 12:
          message.hummockSnapshot = HummockSnapshot.decode(reader, reader.uint32());
          break;
        case 13:
          message.parallelUnitMapping = ParallelUnitMapping.decode(reader, reader.uint32());
          break;
        case 14:
          message.hummockVersionDeltas = HummockVersionDeltas.decode(reader, reader.uint32());
          break;
        case 20:
          message.snapshot = MetaSnapshot.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SubscribeResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      operation: isSet(object.operation) ? subscribeResponse_OperationFromJSON(object.operation) : 0,
      version: isSet(object.version) ? Number(object.version) : 0,
      node: isSet(object.node) ? WorkerNode.fromJSON(object.node) : undefined,
      database: isSet(object.database) ? Database.fromJSON(object.database) : undefined,
      schema: isSet(object.schema) ? Schema.fromJSON(object.schema) : undefined,
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      source: isSet(object.source) ? Source.fromJSON(object.source) : undefined,
      sink: isSet(object.sink) ? Sink.fromJSON(object.sink) : undefined,
      index: isSet(object.index) ? Index.fromJSON(object.index) : undefined,
      user: isSet(object.user) ? UserInfo.fromJSON(object.user) : undefined,
      hummockSnapshot: isSet(object.hummockSnapshot) ? HummockSnapshot.fromJSON(object.hummockSnapshot) : undefined,
      parallelUnitMapping: isSet(object.parallelUnitMapping)
        ? ParallelUnitMapping.fromJSON(object.parallelUnitMapping)
        : undefined,
      hummockVersionDeltas: isSet(object.hummockVersionDeltas)
        ? HummockVersionDeltas.fromJSON(object.hummockVersionDeltas)
        : undefined,
      snapshot: isSet(object.snapshot) ? MetaSnapshot.fromJSON(object.snapshot) : undefined,
    };
  },

  toJSON(message: SubscribeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.operation !== undefined && (obj.operation = subscribeResponse_OperationToJSON(message.operation));
    message.version !== undefined && (obj.version = Math.round(message.version));
    message.node !== undefined && (obj.node = message.node ? WorkerNode.toJSON(message.node) : undefined);
    message.database !== undefined && (obj.database = message.database ? Database.toJSON(message.database) : undefined);
    message.schema !== undefined && (obj.schema = message.schema ? Schema.toJSON(message.schema) : undefined);
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    message.source !== undefined && (obj.source = message.source ? Source.toJSON(message.source) : undefined);
    message.sink !== undefined && (obj.sink = message.sink ? Sink.toJSON(message.sink) : undefined);
    message.index !== undefined && (obj.index = message.index ? Index.toJSON(message.index) : undefined);
    message.user !== undefined && (obj.user = message.user ? UserInfo.toJSON(message.user) : undefined);
    message.hummockSnapshot !== undefined &&
      (obj.hummockSnapshot = message.hummockSnapshot ? HummockSnapshot.toJSON(message.hummockSnapshot) : undefined);
    message.parallelUnitMapping !== undefined && (obj.parallelUnitMapping = message.parallelUnitMapping
      ? ParallelUnitMapping.toJSON(message.parallelUnitMapping)
      : undefined);
    message.hummockVersionDeltas !== undefined && (obj.hummockVersionDeltas = message.hummockVersionDeltas
      ? HummockVersionDeltas.toJSON(message.hummockVersionDeltas)
      : undefined);
    message.snapshot !== undefined &&
      (obj.snapshot = message.snapshot ? MetaSnapshot.toJSON(message.snapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeResponse>, I>>(object: I): SubscribeResponse {
    const message = createBaseSubscribeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.operation = object.operation ?? 0;
    message.version = object.version ?? 0;
    message.node = (object.node !== undefined && object.node !== null)
      ? WorkerNode.fromPartial(object.node)
      : undefined;
    message.database = (object.database !== undefined && object.database !== null)
      ? Database.fromPartial(object.database)
      : undefined;
    message.schema = (object.schema !== undefined && object.schema !== null)
      ? Schema.fromPartial(object.schema)
      : undefined;
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.source = (object.source !== undefined && object.source !== null)
      ? Source.fromPartial(object.source)
      : undefined;
    message.sink = (object.sink !== undefined && object.sink !== null) ? Sink.fromPartial(object.sink) : undefined;
    message.index = (object.index !== undefined && object.index !== null) ? Index.fromPartial(object.index) : undefined;
    message.user = (object.user !== undefined && object.user !== null) ? UserInfo.fromPartial(object.user) : undefined;
    message.hummockSnapshot = (object.hummockSnapshot !== undefined && object.hummockSnapshot !== null)
      ? HummockSnapshot.fromPartial(object.hummockSnapshot)
      : undefined;
    message.parallelUnitMapping = (object.parallelUnitMapping !== undefined && object.parallelUnitMapping !== null)
      ? ParallelUnitMapping.fromPartial(object.parallelUnitMapping)
      : undefined;
    message.hummockVersionDeltas = (object.hummockVersionDeltas !== undefined && object.hummockVersionDeltas !== null)
      ? HummockVersionDeltas.fromPartial(object.hummockVersionDeltas)
      : undefined;
    message.snapshot = (object.snapshot !== undefined && object.snapshot !== null)
      ? MetaSnapshot.fromPartial(object.snapshot)
      : undefined;
    return message;
  },
};

function createBaseMetaLeaderInfo(): MetaLeaderInfo {
  return { nodeAddress: "", leaseId: 0 };
}

export const MetaLeaderInfo = {
  encode(message: MetaLeaderInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.nodeAddress !== "") {
      writer.uint32(10).string(message.nodeAddress);
    }
    if (message.leaseId !== 0) {
      writer.uint32(16).uint64(message.leaseId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MetaLeaderInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMetaLeaderInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.nodeAddress = reader.string();
          break;
        case 2:
          message.leaseId = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): MetaLeaderInfo {
    return {
      nodeAddress: isSet(object.nodeAddress) ? String(object.nodeAddress) : "",
      leaseId: isSet(object.leaseId) ? Number(object.leaseId) : 0,
    };
  },

  toJSON(message: MetaLeaderInfo): unknown {
    const obj: any = {};
    message.nodeAddress !== undefined && (obj.nodeAddress = message.nodeAddress);
    message.leaseId !== undefined && (obj.leaseId = Math.round(message.leaseId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaLeaderInfo>, I>>(object: I): MetaLeaderInfo {
    const message = createBaseMetaLeaderInfo();
    message.nodeAddress = object.nodeAddress ?? "";
    message.leaseId = object.leaseId ?? 0;
    return message;
  },
};

function createBaseMetaLeaseInfo(): MetaLeaseInfo {
  return { leader: undefined, leaseRegisterTime: 0, leaseExpireTime: 0 };
}

export const MetaLeaseInfo = {
  encode(message: MetaLeaseInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.leader !== undefined) {
      MetaLeaderInfo.encode(message.leader, writer.uint32(10).fork()).ldelim();
    }
    if (message.leaseRegisterTime !== 0) {
      writer.uint32(16).uint64(message.leaseRegisterTime);
    }
    if (message.leaseExpireTime !== 0) {
      writer.uint32(24).uint64(message.leaseExpireTime);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MetaLeaseInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMetaLeaseInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.leader = MetaLeaderInfo.decode(reader, reader.uint32());
          break;
        case 2:
          message.leaseRegisterTime = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          message.leaseExpireTime = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): MetaLeaseInfo {
    return {
      leader: isSet(object.leader) ? MetaLeaderInfo.fromJSON(object.leader) : undefined,
      leaseRegisterTime: isSet(object.leaseRegisterTime) ? Number(object.leaseRegisterTime) : 0,
      leaseExpireTime: isSet(object.leaseExpireTime) ? Number(object.leaseExpireTime) : 0,
    };
  },

  toJSON(message: MetaLeaseInfo): unknown {
    const obj: any = {};
    message.leader !== undefined && (obj.leader = message.leader ? MetaLeaderInfo.toJSON(message.leader) : undefined);
    message.leaseRegisterTime !== undefined && (obj.leaseRegisterTime = Math.round(message.leaseRegisterTime));
    message.leaseExpireTime !== undefined && (obj.leaseExpireTime = Math.round(message.leaseExpireTime));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaLeaseInfo>, I>>(object: I): MetaLeaseInfo {
    const message = createBaseMetaLeaseInfo();
    message.leader = (object.leader !== undefined && object.leader !== null)
      ? MetaLeaderInfo.fromPartial(object.leader)
      : undefined;
    message.leaseRegisterTime = object.leaseRegisterTime ?? 0;
    message.leaseExpireTime = object.leaseExpireTime ?? 0;
    return message;
  },
};

function createBasePauseRequest(): PauseRequest {
  return {};
}

export const PauseRequest = {
  encode(_: PauseRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PauseRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePauseRequest();
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

  fromJSON(_: any): PauseRequest {
    return {};
  },

  toJSON(_: PauseRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PauseRequest>, I>>(_: I): PauseRequest {
    const message = createBasePauseRequest();
    return message;
  },
};

function createBasePauseResponse(): PauseResponse {
  return {};
}

export const PauseResponse = {
  encode(_: PauseResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PauseResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePauseResponse();
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

  fromJSON(_: any): PauseResponse {
    return {};
  },

  toJSON(_: PauseResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PauseResponse>, I>>(_: I): PauseResponse {
    const message = createBasePauseResponse();
    return message;
  },
};

function createBaseResumeRequest(): ResumeRequest {
  return {};
}

export const ResumeRequest = {
  encode(_: ResumeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ResumeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseResumeRequest();
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

  fromJSON(_: any): ResumeRequest {
    return {};
  },

  toJSON(_: ResumeRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ResumeRequest>, I>>(_: I): ResumeRequest {
    const message = createBaseResumeRequest();
    return message;
  },
};

function createBaseResumeResponse(): ResumeResponse {
  return {};
}

export const ResumeResponse = {
  encode(_: ResumeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ResumeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseResumeResponse();
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

  fromJSON(_: any): ResumeResponse {
    return {};
  },

  toJSON(_: ResumeResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ResumeResponse>, I>>(_: I): ResumeResponse {
    const message = createBaseResumeResponse();
    return message;
  },
};

function createBaseGetClusterInfoRequest(): GetClusterInfoRequest {
  return {};
}

export const GetClusterInfoRequest = {
  encode(_: GetClusterInfoRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetClusterInfoRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetClusterInfoRequest();
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

  fromJSON(_: any): GetClusterInfoRequest {
    return {};
  },

  toJSON(_: GetClusterInfoRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetClusterInfoRequest>, I>>(_: I): GetClusterInfoRequest {
    const message = createBaseGetClusterInfoRequest();
    return message;
  },
};

function createBaseGetClusterInfoResponse(): GetClusterInfoResponse {
  return { workerNodes: [], tableFragments: [], actorSplits: {}, streamSourceInfos: {} };
}

export const GetClusterInfoResponse = {
  encode(message: GetClusterInfoResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.workerNodes) {
      WorkerNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.tableFragments) {
      TableFragments.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    Object.entries(message.actorSplits).forEach(([key, value]) => {
      GetClusterInfoResponse_ActorSplitsEntry.encode({ key: key as any, value }, writer.uint32(26).fork()).ldelim();
    });
    Object.entries(message.streamSourceInfos).forEach(([key, value]) => {
      GetClusterInfoResponse_StreamSourceInfosEntry.encode({ key: key as any, value }, writer.uint32(34).fork())
        .ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetClusterInfoResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetClusterInfoResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.workerNodes.push(WorkerNode.decode(reader, reader.uint32()));
          break;
        case 2:
          message.tableFragments.push(TableFragments.decode(reader, reader.uint32()));
          break;
        case 3:
          const entry3 = GetClusterInfoResponse_ActorSplitsEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.actorSplits[entry3.key] = entry3.value;
          }
          break;
        case 4:
          const entry4 = GetClusterInfoResponse_StreamSourceInfosEntry.decode(reader, reader.uint32());
          if (entry4.value !== undefined) {
            message.streamSourceInfos[entry4.key] = entry4.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetClusterInfoResponse {
    return {
      workerNodes: Array.isArray(object?.workerNodes) ? object.workerNodes.map((e: any) => WorkerNode.fromJSON(e)) : [],
      tableFragments: Array.isArray(object?.tableFragments)
        ? object.tableFragments.map((e: any) => TableFragments.fromJSON(e))
        : [],
      actorSplits: isObject(object.actorSplits)
        ? Object.entries(object.actorSplits).reduce<{ [key: number]: ConnectorSplits }>((acc, [key, value]) => {
          acc[Number(key)] = ConnectorSplits.fromJSON(value);
          return acc;
        }, {})
        : {},
      streamSourceInfos: isObject(object.streamSourceInfos)
        ? Object.entries(object.streamSourceInfos).reduce<{ [key: number]: StreamSourceInfo }>((acc, [key, value]) => {
          acc[Number(key)] = StreamSourceInfo.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: GetClusterInfoResponse): unknown {
    const obj: any = {};
    if (message.workerNodes) {
      obj.workerNodes = message.workerNodes.map((e) => e ? WorkerNode.toJSON(e) : undefined);
    } else {
      obj.workerNodes = [];
    }
    if (message.tableFragments) {
      obj.tableFragments = message.tableFragments.map((e) => e ? TableFragments.toJSON(e) : undefined);
    } else {
      obj.tableFragments = [];
    }
    obj.actorSplits = {};
    if (message.actorSplits) {
      Object.entries(message.actorSplits).forEach(([k, v]) => {
        obj.actorSplits[k] = ConnectorSplits.toJSON(v);
      });
    }
    obj.streamSourceInfos = {};
    if (message.streamSourceInfos) {
      Object.entries(message.streamSourceInfos).forEach(([k, v]) => {
        obj.streamSourceInfos[k] = StreamSourceInfo.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetClusterInfoResponse>, I>>(object: I): GetClusterInfoResponse {
    const message = createBaseGetClusterInfoResponse();
    message.workerNodes = object.workerNodes?.map((e) => WorkerNode.fromPartial(e)) || [];
    message.tableFragments = object.tableFragments?.map((e) => TableFragments.fromPartial(e)) || [];
    message.actorSplits = Object.entries(object.actorSplits ?? {}).reduce<{ [key: number]: ConnectorSplits }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = ConnectorSplits.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.streamSourceInfos = Object.entries(object.streamSourceInfos ?? {}).reduce<
      { [key: number]: StreamSourceInfo }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = StreamSourceInfo.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseGetClusterInfoResponse_ActorSplitsEntry(): GetClusterInfoResponse_ActorSplitsEntry {
  return { key: 0, value: undefined };
}

export const GetClusterInfoResponse_ActorSplitsEntry = {
  encode(message: GetClusterInfoResponse_ActorSplitsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      ConnectorSplits.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetClusterInfoResponse_ActorSplitsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetClusterInfoResponse_ActorSplitsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = ConnectorSplits.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetClusterInfoResponse_ActorSplitsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ConnectorSplits.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: GetClusterInfoResponse_ActorSplitsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? ConnectorSplits.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetClusterInfoResponse_ActorSplitsEntry>, I>>(
    object: I,
  ): GetClusterInfoResponse_ActorSplitsEntry {
    const message = createBaseGetClusterInfoResponse_ActorSplitsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ConnectorSplits.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseGetClusterInfoResponse_StreamSourceInfosEntry(): GetClusterInfoResponse_StreamSourceInfosEntry {
  return { key: 0, value: undefined };
}

export const GetClusterInfoResponse_StreamSourceInfosEntry = {
  encode(message: GetClusterInfoResponse_StreamSourceInfosEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      StreamSourceInfo.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetClusterInfoResponse_StreamSourceInfosEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetClusterInfoResponse_StreamSourceInfosEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = StreamSourceInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetClusterInfoResponse_StreamSourceInfosEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? StreamSourceInfo.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: GetClusterInfoResponse_StreamSourceInfosEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? StreamSourceInfo.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetClusterInfoResponse_StreamSourceInfosEntry>, I>>(
    object: I,
  ): GetClusterInfoResponse_StreamSourceInfosEntry {
    const message = createBaseGetClusterInfoResponse_StreamSourceInfosEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? StreamSourceInfo.fromPartial(object.value)
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

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
