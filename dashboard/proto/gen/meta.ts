/* eslint-disable */
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
  info?: { $case: "hummockGcWatermark"; hummockGcWatermark: number };
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
export const TableFragments_ActorState = {
  UNSPECIFIED: "UNSPECIFIED",
  /** INACTIVE - Initial state after creation */
  INACTIVE: "INACTIVE",
  /** RUNNING - Running normally */
  RUNNING: "RUNNING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TableFragments_ActorState = typeof TableFragments_ActorState[keyof typeof TableFragments_ActorState];

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

export const TableFragments_Fragment_FragmentDistributionType = {
  UNSPECIFIED: "UNSPECIFIED",
  SINGLE: "SINGLE",
  HASH: "HASH",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TableFragments_Fragment_FragmentDistributionType =
  typeof TableFragments_Fragment_FragmentDistributionType[
    keyof typeof TableFragments_Fragment_FragmentDistributionType
  ];

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
  info?:
    | { $case: "node"; node: WorkerNode }
    | { $case: "database"; database: Database }
    | { $case: "schema"; schema: Schema }
    | { $case: "table"; table: Table }
    | { $case: "source"; source: Source }
    | { $case: "sink"; sink: Sink }
    | { $case: "index"; index: Index }
    | { $case: "user"; user: UserInfo }
    | { $case: "hummockSnapshot"; hummockSnapshot: HummockSnapshot }
    | { $case: "parallelUnitMapping"; parallelUnitMapping: ParallelUnitMapping }
    | { $case: "hummockVersionDeltas"; hummockVersionDeltas: HummockVersionDeltas }
    | { $case: "snapshot"; snapshot: MetaSnapshot };
}

export const SubscribeResponse_Operation = {
  UNSPECIFIED: "UNSPECIFIED",
  ADD: "ADD",
  DELETE: "DELETE",
  UPDATE: "UPDATE",
  SNAPSHOT: "SNAPSHOT",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type SubscribeResponse_Operation = typeof SubscribeResponse_Operation[keyof typeof SubscribeResponse_Operation];

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
  return { info: undefined };
}

export const HeartbeatRequest_ExtraInfo = {
  fromJSON(object: any): HeartbeatRequest_ExtraInfo {
    return {
      info: isSet(object.hummockGcWatermark)
        ? { $case: "hummockGcWatermark", hummockGcWatermark: Number(object.hummockGcWatermark) }
        : undefined,
    };
  },

  toJSON(message: HeartbeatRequest_ExtraInfo): unknown {
    const obj: any = {};
    message.info?.$case === "hummockGcWatermark" &&
      (obj.hummockGcWatermark = Math.round(message.info?.hummockGcWatermark));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HeartbeatRequest_ExtraInfo>, I>>(object: I): HeartbeatRequest_ExtraInfo {
    const message = createBaseHeartbeatRequest_ExtraInfo();
    if (
      object.info?.$case === "hummockGcWatermark" &&
      object.info?.hummockGcWatermark !== undefined &&
      object.info?.hummockGcWatermark !== null
    ) {
      message.info = { $case: "hummockGcWatermark", hummockGcWatermark: object.info.hummockGcWatermark };
    }
    return message;
  },
};

function createBaseHeartbeatResponse(): HeartbeatResponse {
  return { status: undefined };
}

export const HeartbeatResponse = {
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
  return { parallelUnit: undefined, state: TableFragments_ActorState.UNSPECIFIED };
}

export const TableFragments_ActorStatus = {
  fromJSON(object: any): TableFragments_ActorStatus {
    return {
      parallelUnit: isSet(object.parallelUnit) ? ParallelUnit.fromJSON(object.parallelUnit) : undefined,
      state: isSet(object.state)
        ? tableFragments_ActorStateFromJSON(object.state)
        : TableFragments_ActorState.UNSPECIFIED,
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
    message.state = object.state ?? TableFragments_ActorState.UNSPECIFIED;
    return message;
  },
};

function createBaseTableFragments_Fragment(): TableFragments_Fragment {
  return {
    fragmentId: 0,
    fragmentType: FragmentType.FRAGMENT_UNSPECIFIED,
    distributionType: TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED,
    actors: [],
    vnodeMapping: undefined,
    stateTableIds: [],
  };
}

export const TableFragments_Fragment = {
  fromJSON(object: any): TableFragments_Fragment {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      fragmentType: isSet(object.fragmentType)
        ? fragmentTypeFromJSON(object.fragmentType)
        : FragmentType.FRAGMENT_UNSPECIFIED,
      distributionType: isSet(object.distributionType)
        ? tableFragments_Fragment_FragmentDistributionTypeFromJSON(object.distributionType)
        : TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED,
      actors: Array.isArray(object?.actors)
        ? object.actors.map((e: any) => StreamActor.fromJSON(e))
        : [],
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
    message.fragmentType = object.fragmentType ?? FragmentType.FRAGMENT_UNSPECIFIED;
    message.distributionType = object.distributionType ?? TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED;
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
  return { workerType: WorkerType.UNSPECIFIED, host: undefined, workerNodeParallelism: 0 };
}

export const AddWorkerNodeRequest = {
  fromJSON(object: any): AddWorkerNodeRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : WorkerType.UNSPECIFIED,
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
    message.workerType = object.workerType ?? WorkerType.UNSPECIFIED;
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
  return { workerType: WorkerType.UNSPECIFIED, includeStartingNodes: false };
}

export const ListAllNodesRequest = {
  fromJSON(object: any): ListAllNodesRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : WorkerType.UNSPECIFIED,
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
    message.workerType = object.workerType ?? WorkerType.UNSPECIFIED;
    message.includeStartingNodes = object.includeStartingNodes ?? false;
    return message;
  },
};

function createBaseListAllNodesResponse(): ListAllNodesResponse {
  return { status: undefined, nodes: [] };
}

export const ListAllNodesResponse = {
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
  return { workerType: WorkerType.UNSPECIFIED, host: undefined };
}

export const SubscribeRequest = {
  fromJSON(object: any): SubscribeRequest {
    return {
      workerType: isSet(object.workerType) ? workerTypeFromJSON(object.workerType) : WorkerType.UNSPECIFIED,
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
    message.workerType = object.workerType ?? WorkerType.UNSPECIFIED;
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
  return { status: undefined, operation: SubscribeResponse_Operation.UNSPECIFIED, version: 0, info: undefined };
}

export const SubscribeResponse = {
  fromJSON(object: any): SubscribeResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      operation: isSet(object.operation)
        ? subscribeResponse_OperationFromJSON(object.operation)
        : SubscribeResponse_Operation.UNSPECIFIED,
      version: isSet(object.version) ? Number(object.version) : 0,
      info: isSet(object.node)
        ? { $case: "node", node: WorkerNode.fromJSON(object.node) }
        : isSet(object.database)
        ? { $case: "database", database: Database.fromJSON(object.database) }
        : isSet(object.schema)
        ? { $case: "schema", schema: Schema.fromJSON(object.schema) }
        : isSet(object.table)
        ? { $case: "table", table: Table.fromJSON(object.table) }
        : isSet(object.source)
        ? { $case: "source", source: Source.fromJSON(object.source) }
        : isSet(object.sink)
        ? { $case: "sink", sink: Sink.fromJSON(object.sink) }
        : isSet(object.index)
        ? { $case: "index", index: Index.fromJSON(object.index) }
        : isSet(object.user)
        ? { $case: "user", user: UserInfo.fromJSON(object.user) }
        : isSet(object.hummockSnapshot)
        ? { $case: "hummockSnapshot", hummockSnapshot: HummockSnapshot.fromJSON(object.hummockSnapshot) }
        : isSet(object.parallelUnitMapping)
        ? {
          $case: "parallelUnitMapping",
          parallelUnitMapping: ParallelUnitMapping.fromJSON(object.parallelUnitMapping),
        }
        : isSet(object.hummockVersionDeltas)
        ? {
          $case: "hummockVersionDeltas",
          hummockVersionDeltas: HummockVersionDeltas.fromJSON(object.hummockVersionDeltas),
        }
        : isSet(object.snapshot)
        ? { $case: "snapshot", snapshot: MetaSnapshot.fromJSON(object.snapshot) }
        : undefined,
    };
  },

  toJSON(message: SubscribeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.operation !== undefined && (obj.operation = subscribeResponse_OperationToJSON(message.operation));
    message.version !== undefined && (obj.version = Math.round(message.version));
    message.info?.$case === "node" &&
      (obj.node = message.info?.node ? WorkerNode.toJSON(message.info?.node) : undefined);
    message.info?.$case === "database" &&
      (obj.database = message.info?.database ? Database.toJSON(message.info?.database) : undefined);
    message.info?.$case === "schema" &&
      (obj.schema = message.info?.schema ? Schema.toJSON(message.info?.schema) : undefined);
    message.info?.$case === "table" &&
      (obj.table = message.info?.table ? Table.toJSON(message.info?.table) : undefined);
    message.info?.$case === "source" &&
      (obj.source = message.info?.source ? Source.toJSON(message.info?.source) : undefined);
    message.info?.$case === "sink" && (obj.sink = message.info?.sink ? Sink.toJSON(message.info?.sink) : undefined);
    message.info?.$case === "index" &&
      (obj.index = message.info?.index ? Index.toJSON(message.info?.index) : undefined);
    message.info?.$case === "user" && (obj.user = message.info?.user ? UserInfo.toJSON(message.info?.user) : undefined);
    message.info?.$case === "hummockSnapshot" && (obj.hummockSnapshot = message.info?.hummockSnapshot
      ? HummockSnapshot.toJSON(message.info?.hummockSnapshot)
      : undefined);
    message.info?.$case === "parallelUnitMapping" && (obj.parallelUnitMapping = message.info?.parallelUnitMapping
      ? ParallelUnitMapping.toJSON(message.info?.parallelUnitMapping)
      : undefined);
    message.info?.$case === "hummockVersionDeltas" && (obj.hummockVersionDeltas = message.info?.hummockVersionDeltas
      ? HummockVersionDeltas.toJSON(message.info?.hummockVersionDeltas)
      : undefined);
    message.info?.$case === "snapshot" &&
      (obj.snapshot = message.info?.snapshot ? MetaSnapshot.toJSON(message.info?.snapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeResponse>, I>>(object: I): SubscribeResponse {
    const message = createBaseSubscribeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.operation = object.operation ?? SubscribeResponse_Operation.UNSPECIFIED;
    message.version = object.version ?? 0;
    if (object.info?.$case === "node" && object.info?.node !== undefined && object.info?.node !== null) {
      message.info = { $case: "node", node: WorkerNode.fromPartial(object.info.node) };
    }
    if (object.info?.$case === "database" && object.info?.database !== undefined && object.info?.database !== null) {
      message.info = { $case: "database", database: Database.fromPartial(object.info.database) };
    }
    if (object.info?.$case === "schema" && object.info?.schema !== undefined && object.info?.schema !== null) {
      message.info = { $case: "schema", schema: Schema.fromPartial(object.info.schema) };
    }
    if (object.info?.$case === "table" && object.info?.table !== undefined && object.info?.table !== null) {
      message.info = { $case: "table", table: Table.fromPartial(object.info.table) };
    }
    if (object.info?.$case === "source" && object.info?.source !== undefined && object.info?.source !== null) {
      message.info = { $case: "source", source: Source.fromPartial(object.info.source) };
    }
    if (object.info?.$case === "sink" && object.info?.sink !== undefined && object.info?.sink !== null) {
      message.info = { $case: "sink", sink: Sink.fromPartial(object.info.sink) };
    }
    if (object.info?.$case === "index" && object.info?.index !== undefined && object.info?.index !== null) {
      message.info = { $case: "index", index: Index.fromPartial(object.info.index) };
    }
    if (object.info?.$case === "user" && object.info?.user !== undefined && object.info?.user !== null) {
      message.info = { $case: "user", user: UserInfo.fromPartial(object.info.user) };
    }
    if (
      object.info?.$case === "hummockSnapshot" &&
      object.info?.hummockSnapshot !== undefined &&
      object.info?.hummockSnapshot !== null
    ) {
      message.info = {
        $case: "hummockSnapshot",
        hummockSnapshot: HummockSnapshot.fromPartial(object.info.hummockSnapshot),
      };
    }
    if (
      object.info?.$case === "parallelUnitMapping" &&
      object.info?.parallelUnitMapping !== undefined &&
      object.info?.parallelUnitMapping !== null
    ) {
      message.info = {
        $case: "parallelUnitMapping",
        parallelUnitMapping: ParallelUnitMapping.fromPartial(object.info.parallelUnitMapping),
      };
    }
    if (
      object.info?.$case === "hummockVersionDeltas" &&
      object.info?.hummockVersionDeltas !== undefined &&
      object.info?.hummockVersionDeltas !== null
    ) {
      message.info = {
        $case: "hummockVersionDeltas",
        hummockVersionDeltas: HummockVersionDeltas.fromPartial(object.info.hummockVersionDeltas),
      };
    }
    if (object.info?.$case === "snapshot" && object.info?.snapshot !== undefined && object.info?.snapshot !== null) {
      message.info = { $case: "snapshot", snapshot: MetaSnapshot.fromPartial(object.info.snapshot) };
    }
    return message;
  },
};

function createBaseMetaLeaderInfo(): MetaLeaderInfo {
  return { nodeAddress: "", leaseId: 0 };
}

export const MetaLeaderInfo = {
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
