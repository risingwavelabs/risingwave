/* eslint-disable */
import { MetaBackupManifestId } from "./backup_service";
import { Database, Function, Index, Schema, Sink, Source, Table, View } from "./catalog";
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
import { HummockSnapshot, HummockVersion, HummockVersionDeltas, WriteLimits } from "./hummock";
import { ConnectorSplits } from "./source";
import { Dispatcher, StreamActor, StreamEnvironment, StreamNode } from "./stream_plan";
import { UserInfo } from "./user";

export const protobufPackage = "meta";

export const SubscribeType = {
  UNSPECIFIED: "UNSPECIFIED",
  FRONTEND: "FRONTEND",
  HUMMOCK: "HUMMOCK",
  COMPACTOR: "COMPACTOR",
  COMPUTE: "COMPUTE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type SubscribeType = typeof SubscribeType[keyof typeof SubscribeType];

export function subscribeTypeFromJSON(object: any): SubscribeType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return SubscribeType.UNSPECIFIED;
    case 1:
    case "FRONTEND":
      return SubscribeType.FRONTEND;
    case 2:
    case "HUMMOCK":
      return SubscribeType.HUMMOCK;
    case 3:
    case "COMPACTOR":
      return SubscribeType.COMPACTOR;
    case 4:
    case "COMPUTE":
      return SubscribeType.COMPUTE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return SubscribeType.UNRECOGNIZED;
  }
}

export function subscribeTypeToJSON(object: SubscribeType): string {
  switch (object) {
    case SubscribeType.UNSPECIFIED:
      return "UNSPECIFIED";
    case SubscribeType.FRONTEND:
      return "FRONTEND";
    case SubscribeType.HUMMOCK:
      return "HUMMOCK";
    case SubscribeType.COMPACTOR:
      return "COMPACTOR";
    case SubscribeType.COMPUTE:
      return "COMPUTE";
    case SubscribeType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

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

/** Fragments of a Streaming Job */
export interface TableFragments {
  tableId: number;
  state: TableFragments_State;
  fragments: { [key: number]: TableFragments_Fragment };
  actorStatus: { [key: number]: TableFragments_ActorStatus };
  actorSplits: { [key: number]: ConnectorSplits };
  env: StreamEnvironment | undefined;
}

/** The state of the fragments of this table */
export const TableFragments_State = {
  UNSPECIFIED: "UNSPECIFIED",
  /** INITIAL - The streaming job is initial. */
  INITIAL: "INITIAL",
  /** CREATING - The streaming job is creating. */
  CREATING: "CREATING",
  /** CREATED - The streaming job has been created. */
  CREATED: "CREATED",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TableFragments_State = typeof TableFragments_State[keyof typeof TableFragments_State];

export function tableFragments_StateFromJSON(object: any): TableFragments_State {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TableFragments_State.UNSPECIFIED;
    case 1:
    case "INITIAL":
      return TableFragments_State.INITIAL;
    case 2:
    case "CREATING":
      return TableFragments_State.CREATING;
    case 3:
    case "CREATED":
      return TableFragments_State.CREATED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TableFragments_State.UNRECOGNIZED;
  }
}

export function tableFragments_StateToJSON(object: TableFragments_State): string {
  switch (object) {
    case TableFragments_State.UNSPECIFIED:
      return "UNSPECIFIED";
    case TableFragments_State.INITIAL:
      return "INITIAL";
    case TableFragments_State.CREATING:
      return "CREATING";
    case TableFragments_State.CREATED:
      return "CREATED";
    case TableFragments_State.UNRECOGNIZED:
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
  state: TableFragments_ActorStatus_ActorState;
}

/** Current state of actor */
export const TableFragments_ActorStatus_ActorState = {
  UNSPECIFIED: "UNSPECIFIED",
  /** INACTIVE - Initial state after creation */
  INACTIVE: "INACTIVE",
  /** RUNNING - Running normally */
  RUNNING: "RUNNING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TableFragments_ActorStatus_ActorState =
  typeof TableFragments_ActorStatus_ActorState[keyof typeof TableFragments_ActorStatus_ActorState];

export function tableFragments_ActorStatus_ActorStateFromJSON(object: any): TableFragments_ActorStatus_ActorState {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TableFragments_ActorStatus_ActorState.UNSPECIFIED;
    case 1:
    case "INACTIVE":
      return TableFragments_ActorStatus_ActorState.INACTIVE;
    case 2:
    case "RUNNING":
      return TableFragments_ActorStatus_ActorState.RUNNING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TableFragments_ActorStatus_ActorState.UNRECOGNIZED;
  }
}

export function tableFragments_ActorStatus_ActorStateToJSON(object: TableFragments_ActorStatus_ActorState): string {
  switch (object) {
    case TableFragments_ActorStatus_ActorState.UNSPECIFIED:
      return "UNSPECIFIED";
    case TableFragments_ActorStatus_ActorState.INACTIVE:
      return "INACTIVE";
    case TableFragments_ActorStatus_ActorState.RUNNING:
      return "RUNNING";
    case TableFragments_ActorStatus_ActorState.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface TableFragments_Fragment {
  fragmentId: number;
  /** Bitwise-OR of FragmentTypeFlags */
  fragmentTypeMask: number;
  distributionType: TableFragments_Fragment_FragmentDistributionType;
  actors: StreamActor[];
  /**
   * Vnode mapping (which should be set in upstream dispatcher) of the fragment.
   * This field is always set to `Some`. For singleton, the parallel unit for all vnodes will be the same.
   */
  vnodeMapping: ParallelUnitMapping | undefined;
  stateTableIds: number[];
  /**
   * Note that this can be derived backwards from the upstream actors of the Actor held by the Fragment,
   * but in some scenarios (e.g. Scaling) it will lead to a lot of duplicate code,
   * so we pre-generate and store it here, this member will only be initialized when creating the Fragment
   * and modified when creating the mv-on-mv
   */
  upstreamFragmentIds: number[];
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

export interface TableFragments_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

/** / Parallel unit mapping with fragment id, used for notification. */
export interface FragmentParallelUnitMapping {
  fragmentId: number;
  mapping: ParallelUnitMapping | undefined;
}

/** TODO: remove this when dashboard refactored. */
export interface ActorLocation {
  node: WorkerNode | undefined;
  actors: StreamActor[];
}

export interface FlushRequest {
  checkpoint: boolean;
}

export interface FlushResponse {
  status: Status | undefined;
  snapshot: HummockSnapshot | undefined;
}

export interface CreatingJobInfo {
  databaseId: number;
  schemaId: number;
  name: string;
}

export interface CancelCreatingJobsRequest {
  infos: CreatingJobInfo[];
}

export interface CancelCreatingJobsResponse {
  status: Status | undefined;
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
  env: StreamEnvironment | undefined;
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
  subscribeType: SubscribeType;
  host: HostAddress | undefined;
  workerId: number;
}

export interface MetaSnapshot {
  databases: Database[];
  schemas: Schema[];
  sources: Source[];
  sinks: Sink[];
  tables: Table[];
  indexes: Index[];
  views: View[];
  functions: Function[];
  users: UserInfo[];
  parallelUnitMappings: FragmentParallelUnitMapping[];
  nodes: WorkerNode[];
  hummockSnapshot: HummockSnapshot | undefined;
  hummockVersion: HummockVersion | undefined;
  metaBackupManifestId: MetaBackupManifestId | undefined;
  hummockWriteLimits: WriteLimits | undefined;
  version: MetaSnapshot_SnapshotVersion | undefined;
}

export interface MetaSnapshot_SnapshotVersion {
  catalogVersion: number;
  parallelUnitMappingVersion: number;
  workerNodeVersion: number;
}

export interface SubscribeResponse {
  status: Status | undefined;
  operation: SubscribeResponse_Operation;
  version: number;
  info?:
    | { $case: "database"; database: Database }
    | { $case: "schema"; schema: Schema }
    | { $case: "table"; table: Table }
    | { $case: "source"; source: Source }
    | { $case: "sink"; sink: Sink }
    | { $case: "index"; index: Index }
    | { $case: "view"; view: View }
    | { $case: "function"; function: Function }
    | { $case: "user"; user: UserInfo }
    | { $case: "parallelUnitMapping"; parallelUnitMapping: FragmentParallelUnitMapping }
    | { $case: "node"; node: WorkerNode }
    | { $case: "hummockSnapshot"; hummockSnapshot: HummockSnapshot }
    | { $case: "hummockVersionDeltas"; hummockVersionDeltas: HummockVersionDeltas }
    | { $case: "snapshot"; snapshot: MetaSnapshot }
    | { $case: "metaBackupManifestId"; metaBackupManifestId: MetaBackupManifestId }
    | { $case: "systemParams"; systemParams: SystemParams }
    | { $case: "hummockWriteLimits"; hummockWriteLimits: WriteLimits };
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
  sourceInfos: { [key: number]: Source };
}

export interface GetClusterInfoResponse_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

export interface GetClusterInfoResponse_SourceInfosEntry {
  key: number;
  value: Source | undefined;
}

export interface RescheduleRequest {
  /** reschedule plan for each fragment */
  reschedules: { [key: number]: RescheduleRequest_Reschedule };
}

export interface RescheduleRequest_Reschedule {
  addedParallelUnits: number[];
  removedParallelUnits: number[];
}

export interface RescheduleRequest_ReschedulesEntry {
  key: number;
  value: RescheduleRequest_Reschedule | undefined;
}

export interface RescheduleResponse {
  success: boolean;
}

export interface MembersRequest {
}

export interface MetaMember {
  address: HostAddress | undefined;
  isLeader: boolean;
}

export interface MembersResponse {
  members: MetaMember[];
}

/**
 * The schema for persisted system parameters.
 * Note on backward compatibility:
 * - Do not remove deprecated fields. Mark them as deprecated both after the field definition and in `system_params/mod.rs` instead.
 * - Do not rename existing fields, since each field is stored separately in the meta store with the field name as the key.
 * - To modify (rename, change the type or semantic of) a field, introduce a new field suffixed by the version.
 */
export interface SystemParams {
  barrierIntervalMs?: number | undefined;
  checkpointFrequency?: number | undefined;
  sstableSizeMb?: number | undefined;
  blockSizeKb?: number | undefined;
  bloomFalsePositive?: number | undefined;
  stateStore?: string | undefined;
  dataDirectory?: string | undefined;
  backupStorageUrl?: string | undefined;
  backupStorageDirectory?: string | undefined;
}

export interface GetSystemParamsRequest {
}

export interface GetSystemParamsResponse {
  params: SystemParams | undefined;
}

export interface SetSystemParamRequest {
  param: string;
  /** None means set to default value. */
  value?: string | undefined;
}

export interface SetSystemParamResponse {
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
  return {
    tableId: 0,
    state: TableFragments_State.UNSPECIFIED,
    fragments: {},
    actorStatus: {},
    actorSplits: {},
    env: undefined,
  };
}

export const TableFragments = {
  fromJSON(object: any): TableFragments {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      state: isSet(object.state) ? tableFragments_StateFromJSON(object.state) : TableFragments_State.UNSPECIFIED,
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
      actorSplits: isObject(object.actorSplits)
        ? Object.entries(object.actorSplits).reduce<{ [key: number]: ConnectorSplits }>((acc, [key, value]) => {
          acc[Number(key)] = ConnectorSplits.fromJSON(value);
          return acc;
        }, {})
        : {},
      env: isSet(object.env) ? StreamEnvironment.fromJSON(object.env) : undefined,
    };
  },

  toJSON(message: TableFragments): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.state !== undefined && (obj.state = tableFragments_StateToJSON(message.state));
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
    obj.actorSplits = {};
    if (message.actorSplits) {
      Object.entries(message.actorSplits).forEach(([k, v]) => {
        obj.actorSplits[k] = ConnectorSplits.toJSON(v);
      });
    }
    message.env !== undefined && (obj.env = message.env ? StreamEnvironment.toJSON(message.env) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments>, I>>(object: I): TableFragments {
    const message = createBaseTableFragments();
    message.tableId = object.tableId ?? 0;
    message.state = object.state ?? TableFragments_State.UNSPECIFIED;
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
    message.actorSplits = Object.entries(object.actorSplits ?? {}).reduce<{ [key: number]: ConnectorSplits }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = ConnectorSplits.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.env = (object.env !== undefined && object.env !== null)
      ? StreamEnvironment.fromPartial(object.env)
      : undefined;
    return message;
  },
};

function createBaseTableFragments_ActorStatus(): TableFragments_ActorStatus {
  return { parallelUnit: undefined, state: TableFragments_ActorStatus_ActorState.UNSPECIFIED };
}

export const TableFragments_ActorStatus = {
  fromJSON(object: any): TableFragments_ActorStatus {
    return {
      parallelUnit: isSet(object.parallelUnit) ? ParallelUnit.fromJSON(object.parallelUnit) : undefined,
      state: isSet(object.state)
        ? tableFragments_ActorStatus_ActorStateFromJSON(object.state)
        : TableFragments_ActorStatus_ActorState.UNSPECIFIED,
    };
  },

  toJSON(message: TableFragments_ActorStatus): unknown {
    const obj: any = {};
    message.parallelUnit !== undefined &&
      (obj.parallelUnit = message.parallelUnit ? ParallelUnit.toJSON(message.parallelUnit) : undefined);
    message.state !== undefined && (obj.state = tableFragments_ActorStatus_ActorStateToJSON(message.state));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_ActorStatus>, I>>(object: I): TableFragments_ActorStatus {
    const message = createBaseTableFragments_ActorStatus();
    message.parallelUnit = (object.parallelUnit !== undefined && object.parallelUnit !== null)
      ? ParallelUnit.fromPartial(object.parallelUnit)
      : undefined;
    message.state = object.state ?? TableFragments_ActorStatus_ActorState.UNSPECIFIED;
    return message;
  },
};

function createBaseTableFragments_Fragment(): TableFragments_Fragment {
  return {
    fragmentId: 0,
    fragmentTypeMask: 0,
    distributionType: TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED,
    actors: [],
    vnodeMapping: undefined,
    stateTableIds: [],
    upstreamFragmentIds: [],
  };
}

export const TableFragments_Fragment = {
  fromJSON(object: any): TableFragments_Fragment {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      fragmentTypeMask: isSet(object.fragmentTypeMask) ? Number(object.fragmentTypeMask) : 0,
      distributionType: isSet(object.distributionType)
        ? tableFragments_Fragment_FragmentDistributionTypeFromJSON(object.distributionType)
        : TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED,
      actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => StreamActor.fromJSON(e)) : [],
      vnodeMapping: isSet(object.vnodeMapping) ? ParallelUnitMapping.fromJSON(object.vnodeMapping) : undefined,
      stateTableIds: Array.isArray(object?.stateTableIds) ? object.stateTableIds.map((e: any) => Number(e)) : [],
      upstreamFragmentIds: Array.isArray(object?.upstreamFragmentIds)
        ? object.upstreamFragmentIds.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: TableFragments_Fragment): unknown {
    const obj: any = {};
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.fragmentTypeMask !== undefined && (obj.fragmentTypeMask = Math.round(message.fragmentTypeMask));
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
    if (message.upstreamFragmentIds) {
      obj.upstreamFragmentIds = message.upstreamFragmentIds.map((e) => Math.round(e));
    } else {
      obj.upstreamFragmentIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_Fragment>, I>>(object: I): TableFragments_Fragment {
    const message = createBaseTableFragments_Fragment();
    message.fragmentId = object.fragmentId ?? 0;
    message.fragmentTypeMask = object.fragmentTypeMask ?? 0;
    message.distributionType = object.distributionType ?? TableFragments_Fragment_FragmentDistributionType.UNSPECIFIED;
    message.actors = object.actors?.map((e) => StreamActor.fromPartial(e)) || [];
    message.vnodeMapping = (object.vnodeMapping !== undefined && object.vnodeMapping !== null)
      ? ParallelUnitMapping.fromPartial(object.vnodeMapping)
      : undefined;
    message.stateTableIds = object.stateTableIds?.map((e) => e) || [];
    message.upstreamFragmentIds = object.upstreamFragmentIds?.map((e) => e) || [];
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

function createBaseTableFragments_ActorSplitsEntry(): TableFragments_ActorSplitsEntry {
  return { key: 0, value: undefined };
}

export const TableFragments_ActorSplitsEntry = {
  fromJSON(object: any): TableFragments_ActorSplitsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ConnectorSplits.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: TableFragments_ActorSplitsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? ConnectorSplits.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFragments_ActorSplitsEntry>, I>>(
    object: I,
  ): TableFragments_ActorSplitsEntry {
    const message = createBaseTableFragments_ActorSplitsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ConnectorSplits.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseFragmentParallelUnitMapping(): FragmentParallelUnitMapping {
  return { fragmentId: 0, mapping: undefined };
}

export const FragmentParallelUnitMapping = {
  fromJSON(object: any): FragmentParallelUnitMapping {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      mapping: isSet(object.mapping) ? ParallelUnitMapping.fromJSON(object.mapping) : undefined,
    };
  },

  toJSON(message: FragmentParallelUnitMapping): unknown {
    const obj: any = {};
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.mapping !== undefined &&
      (obj.mapping = message.mapping ? ParallelUnitMapping.toJSON(message.mapping) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FragmentParallelUnitMapping>, I>>(object: I): FragmentParallelUnitMapping {
    const message = createBaseFragmentParallelUnitMapping();
    message.fragmentId = object.fragmentId ?? 0;
    message.mapping = (object.mapping !== undefined && object.mapping !== null)
      ? ParallelUnitMapping.fromPartial(object.mapping)
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
  return { checkpoint: false };
}

export const FlushRequest = {
  fromJSON(object: any): FlushRequest {
    return { checkpoint: isSet(object.checkpoint) ? Boolean(object.checkpoint) : false };
  },

  toJSON(message: FlushRequest): unknown {
    const obj: any = {};
    message.checkpoint !== undefined && (obj.checkpoint = message.checkpoint);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FlushRequest>, I>>(object: I): FlushRequest {
    const message = createBaseFlushRequest();
    message.checkpoint = object.checkpoint ?? false;
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

function createBaseCreatingJobInfo(): CreatingJobInfo {
  return { databaseId: 0, schemaId: 0, name: "" };
}

export const CreatingJobInfo = {
  fromJSON(object: any): CreatingJobInfo {
    return {
      databaseId: isSet(object.databaseId) ? Number(object.databaseId) : 0,
      schemaId: isSet(object.schemaId) ? Number(object.schemaId) : 0,
      name: isSet(object.name) ? String(object.name) : "",
    };
  },

  toJSON(message: CreatingJobInfo): unknown {
    const obj: any = {};
    message.databaseId !== undefined && (obj.databaseId = Math.round(message.databaseId));
    message.schemaId !== undefined && (obj.schemaId = Math.round(message.schemaId));
    message.name !== undefined && (obj.name = message.name);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CreatingJobInfo>, I>>(object: I): CreatingJobInfo {
    const message = createBaseCreatingJobInfo();
    message.databaseId = object.databaseId ?? 0;
    message.schemaId = object.schemaId ?? 0;
    message.name = object.name ?? "";
    return message;
  },
};

function createBaseCancelCreatingJobsRequest(): CancelCreatingJobsRequest {
  return { infos: [] };
}

export const CancelCreatingJobsRequest = {
  fromJSON(object: any): CancelCreatingJobsRequest {
    return { infos: Array.isArray(object?.infos) ? object.infos.map((e: any) => CreatingJobInfo.fromJSON(e)) : [] };
  },

  toJSON(message: CancelCreatingJobsRequest): unknown {
    const obj: any = {};
    if (message.infos) {
      obj.infos = message.infos.map((e) => e ? CreatingJobInfo.toJSON(e) : undefined);
    } else {
      obj.infos = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CancelCreatingJobsRequest>, I>>(object: I): CancelCreatingJobsRequest {
    const message = createBaseCancelCreatingJobsRequest();
    message.infos = object.infos?.map((e) => CreatingJobInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCancelCreatingJobsResponse(): CancelCreatingJobsResponse {
  return { status: undefined };
}

export const CancelCreatingJobsResponse = {
  fromJSON(object: any): CancelCreatingJobsResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: CancelCreatingJobsResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CancelCreatingJobsResponse>, I>>(object: I): CancelCreatingJobsResponse {
    const message = createBaseCancelCreatingJobsResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
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
  return { fragments: [], env: undefined };
}

export const ListTableFragmentsResponse_TableFragmentInfo = {
  fromJSON(object: any): ListTableFragmentsResponse_TableFragmentInfo {
    return {
      fragments: Array.isArray(object?.fragments)
        ? object.fragments.map((e: any) => ListTableFragmentsResponse_FragmentInfo.fromJSON(e))
        : [],
      env: isSet(object.env) ? StreamEnvironment.fromJSON(object.env) : undefined,
    };
  },

  toJSON(message: ListTableFragmentsResponse_TableFragmentInfo): unknown {
    const obj: any = {};
    if (message.fragments) {
      obj.fragments = message.fragments.map((e) => e ? ListTableFragmentsResponse_FragmentInfo.toJSON(e) : undefined);
    } else {
      obj.fragments = [];
    }
    message.env !== undefined && (obj.env = message.env ? StreamEnvironment.toJSON(message.env) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListTableFragmentsResponse_TableFragmentInfo>, I>>(
    object: I,
  ): ListTableFragmentsResponse_TableFragmentInfo {
    const message = createBaseListTableFragmentsResponse_TableFragmentInfo();
    message.fragments = object.fragments?.map((e) => ListTableFragmentsResponse_FragmentInfo.fromPartial(e)) || [];
    message.env = (object.env !== undefined && object.env !== null)
      ? StreamEnvironment.fromPartial(object.env)
      : undefined;
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
  return { subscribeType: SubscribeType.UNSPECIFIED, host: undefined, workerId: 0 };
}

export const SubscribeRequest = {
  fromJSON(object: any): SubscribeRequest {
    return {
      subscribeType: isSet(object.subscribeType)
        ? subscribeTypeFromJSON(object.subscribeType)
        : SubscribeType.UNSPECIFIED,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
      workerId: isSet(object.workerId) ? Number(object.workerId) : 0,
    };
  },

  toJSON(message: SubscribeRequest): unknown {
    const obj: any = {};
    message.subscribeType !== undefined && (obj.subscribeType = subscribeTypeToJSON(message.subscribeType));
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    message.workerId !== undefined && (obj.workerId = Math.round(message.workerId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeRequest>, I>>(object: I): SubscribeRequest {
    const message = createBaseSubscribeRequest();
    message.subscribeType = object.subscribeType ?? SubscribeType.UNSPECIFIED;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    message.workerId = object.workerId ?? 0;
    return message;
  },
};

function createBaseMetaSnapshot(): MetaSnapshot {
  return {
    databases: [],
    schemas: [],
    sources: [],
    sinks: [],
    tables: [],
    indexes: [],
    views: [],
    functions: [],
    users: [],
    parallelUnitMappings: [],
    nodes: [],
    hummockSnapshot: undefined,
    hummockVersion: undefined,
    metaBackupManifestId: undefined,
    hummockWriteLimits: undefined,
    version: undefined,
  };
}

export const MetaSnapshot = {
  fromJSON(object: any): MetaSnapshot {
    return {
      databases: Array.isArray(object?.databases) ? object.databases.map((e: any) => Database.fromJSON(e)) : [],
      schemas: Array.isArray(object?.schemas) ? object.schemas.map((e: any) => Schema.fromJSON(e)) : [],
      sources: Array.isArray(object?.sources) ? object.sources.map((e: any) => Source.fromJSON(e)) : [],
      sinks: Array.isArray(object?.sinks) ? object.sinks.map((e: any) => Sink.fromJSON(e)) : [],
      tables: Array.isArray(object?.tables) ? object.tables.map((e: any) => Table.fromJSON(e)) : [],
      indexes: Array.isArray(object?.indexes) ? object.indexes.map((e: any) => Index.fromJSON(e)) : [],
      views: Array.isArray(object?.views) ? object.views.map((e: any) => View.fromJSON(e)) : [],
      functions: Array.isArray(object?.functions) ? object.functions.map((e: any) => Function.fromJSON(e)) : [],
      users: Array.isArray(object?.users) ? object.users.map((e: any) => UserInfo.fromJSON(e)) : [],
      parallelUnitMappings: Array.isArray(object?.parallelUnitMappings)
        ? object.parallelUnitMappings.map((e: any) => FragmentParallelUnitMapping.fromJSON(e))
        : [],
      nodes: Array.isArray(object?.nodes)
        ? object.nodes.map((e: any) => WorkerNode.fromJSON(e))
        : [],
      hummockSnapshot: isSet(object.hummockSnapshot) ? HummockSnapshot.fromJSON(object.hummockSnapshot) : undefined,
      hummockVersion: isSet(object.hummockVersion) ? HummockVersion.fromJSON(object.hummockVersion) : undefined,
      metaBackupManifestId: isSet(object.metaBackupManifestId)
        ? MetaBackupManifestId.fromJSON(object.metaBackupManifestId)
        : undefined,
      hummockWriteLimits: isSet(object.hummockWriteLimits)
        ? WriteLimits.fromJSON(object.hummockWriteLimits)
        : undefined,
      version: isSet(object.version) ? MetaSnapshot_SnapshotVersion.fromJSON(object.version) : undefined,
    };
  },

  toJSON(message: MetaSnapshot): unknown {
    const obj: any = {};
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
    if (message.views) {
      obj.views = message.views.map((e) => e ? View.toJSON(e) : undefined);
    } else {
      obj.views = [];
    }
    if (message.functions) {
      obj.functions = message.functions.map((e) => e ? Function.toJSON(e) : undefined);
    } else {
      obj.functions = [];
    }
    if (message.users) {
      obj.users = message.users.map((e) => e ? UserInfo.toJSON(e) : undefined);
    } else {
      obj.users = [];
    }
    if (message.parallelUnitMappings) {
      obj.parallelUnitMappings = message.parallelUnitMappings.map((e) =>
        e ? FragmentParallelUnitMapping.toJSON(e) : undefined
      );
    } else {
      obj.parallelUnitMappings = [];
    }
    if (message.nodes) {
      obj.nodes = message.nodes.map((e) => e ? WorkerNode.toJSON(e) : undefined);
    } else {
      obj.nodes = [];
    }
    message.hummockSnapshot !== undefined &&
      (obj.hummockSnapshot = message.hummockSnapshot ? HummockSnapshot.toJSON(message.hummockSnapshot) : undefined);
    message.hummockVersion !== undefined &&
      (obj.hummockVersion = message.hummockVersion ? HummockVersion.toJSON(message.hummockVersion) : undefined);
    message.metaBackupManifestId !== undefined && (obj.metaBackupManifestId = message.metaBackupManifestId
      ? MetaBackupManifestId.toJSON(message.metaBackupManifestId)
      : undefined);
    message.hummockWriteLimits !== undefined &&
      (obj.hummockWriteLimits = message.hummockWriteLimits
        ? WriteLimits.toJSON(message.hummockWriteLimits)
        : undefined);
    message.version !== undefined &&
      (obj.version = message.version ? MetaSnapshot_SnapshotVersion.toJSON(message.version) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaSnapshot>, I>>(object: I): MetaSnapshot {
    const message = createBaseMetaSnapshot();
    message.databases = object.databases?.map((e) => Database.fromPartial(e)) || [];
    message.schemas = object.schemas?.map((e) => Schema.fromPartial(e)) || [];
    message.sources = object.sources?.map((e) => Source.fromPartial(e)) || [];
    message.sinks = object.sinks?.map((e) => Sink.fromPartial(e)) || [];
    message.tables = object.tables?.map((e) => Table.fromPartial(e)) || [];
    message.indexes = object.indexes?.map((e) => Index.fromPartial(e)) || [];
    message.views = object.views?.map((e) => View.fromPartial(e)) || [];
    message.functions = object.functions?.map((e) => Function.fromPartial(e)) || [];
    message.users = object.users?.map((e) => UserInfo.fromPartial(e)) || [];
    message.parallelUnitMappings =
      object.parallelUnitMappings?.map((e) => FragmentParallelUnitMapping.fromPartial(e)) || [];
    message.nodes = object.nodes?.map((e) => WorkerNode.fromPartial(e)) || [];
    message.hummockSnapshot = (object.hummockSnapshot !== undefined && object.hummockSnapshot !== null)
      ? HummockSnapshot.fromPartial(object.hummockSnapshot)
      : undefined;
    message.hummockVersion = (object.hummockVersion !== undefined && object.hummockVersion !== null)
      ? HummockVersion.fromPartial(object.hummockVersion)
      : undefined;
    message.metaBackupManifestId = (object.metaBackupManifestId !== undefined && object.metaBackupManifestId !== null)
      ? MetaBackupManifestId.fromPartial(object.metaBackupManifestId)
      : undefined;
    message.hummockWriteLimits = (object.hummockWriteLimits !== undefined && object.hummockWriteLimits !== null)
      ? WriteLimits.fromPartial(object.hummockWriteLimits)
      : undefined;
    message.version = (object.version !== undefined && object.version !== null)
      ? MetaSnapshot_SnapshotVersion.fromPartial(object.version)
      : undefined;
    return message;
  },
};

function createBaseMetaSnapshot_SnapshotVersion(): MetaSnapshot_SnapshotVersion {
  return { catalogVersion: 0, parallelUnitMappingVersion: 0, workerNodeVersion: 0 };
}

export const MetaSnapshot_SnapshotVersion = {
  fromJSON(object: any): MetaSnapshot_SnapshotVersion {
    return {
      catalogVersion: isSet(object.catalogVersion) ? Number(object.catalogVersion) : 0,
      parallelUnitMappingVersion: isSet(object.parallelUnitMappingVersion)
        ? Number(object.parallelUnitMappingVersion)
        : 0,
      workerNodeVersion: isSet(object.workerNodeVersion) ? Number(object.workerNodeVersion) : 0,
    };
  },

  toJSON(message: MetaSnapshot_SnapshotVersion): unknown {
    const obj: any = {};
    message.catalogVersion !== undefined && (obj.catalogVersion = Math.round(message.catalogVersion));
    message.parallelUnitMappingVersion !== undefined &&
      (obj.parallelUnitMappingVersion = Math.round(message.parallelUnitMappingVersion));
    message.workerNodeVersion !== undefined && (obj.workerNodeVersion = Math.round(message.workerNodeVersion));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaSnapshot_SnapshotVersion>, I>>(object: I): MetaSnapshot_SnapshotVersion {
    const message = createBaseMetaSnapshot_SnapshotVersion();
    message.catalogVersion = object.catalogVersion ?? 0;
    message.parallelUnitMappingVersion = object.parallelUnitMappingVersion ?? 0;
    message.workerNodeVersion = object.workerNodeVersion ?? 0;
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
      info: isSet(object.database)
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
        : isSet(object.view)
        ? { $case: "view", view: View.fromJSON(object.view) }
        : isSet(object.function)
        ? { $case: "function", function: Function.fromJSON(object.function) }
        : isSet(object.user)
        ? { $case: "user", user: UserInfo.fromJSON(object.user) }
        : isSet(object.parallelUnitMapping)
        ? {
          $case: "parallelUnitMapping",
          parallelUnitMapping: FragmentParallelUnitMapping.fromJSON(object.parallelUnitMapping),
        }
        : isSet(object.node)
        ? { $case: "node", node: WorkerNode.fromJSON(object.node) }
        : isSet(object.hummockSnapshot)
        ? { $case: "hummockSnapshot", hummockSnapshot: HummockSnapshot.fromJSON(object.hummockSnapshot) }
        : isSet(object.hummockVersionDeltas)
        ? {
          $case: "hummockVersionDeltas",
          hummockVersionDeltas: HummockVersionDeltas.fromJSON(object.hummockVersionDeltas),
        }
        : isSet(object.snapshot)
        ? { $case: "snapshot", snapshot: MetaSnapshot.fromJSON(object.snapshot) }
        : isSet(object.metaBackupManifestId)
        ? {
          $case: "metaBackupManifestId",
          metaBackupManifestId: MetaBackupManifestId.fromJSON(object.metaBackupManifestId),
        }
        : isSet(object.systemParams)
        ? { $case: "systemParams", systemParams: SystemParams.fromJSON(object.systemParams) }
        : isSet(object.hummockWriteLimits)
        ? { $case: "hummockWriteLimits", hummockWriteLimits: WriteLimits.fromJSON(object.hummockWriteLimits) }
        : undefined,
    };
  },

  toJSON(message: SubscribeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.operation !== undefined && (obj.operation = subscribeResponse_OperationToJSON(message.operation));
    message.version !== undefined && (obj.version = Math.round(message.version));
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
    message.info?.$case === "view" && (obj.view = message.info?.view ? View.toJSON(message.info?.view) : undefined);
    message.info?.$case === "function" &&
      (obj.function = message.info?.function ? Function.toJSON(message.info?.function) : undefined);
    message.info?.$case === "user" && (obj.user = message.info?.user ? UserInfo.toJSON(message.info?.user) : undefined);
    message.info?.$case === "parallelUnitMapping" && (obj.parallelUnitMapping = message.info?.parallelUnitMapping
      ? FragmentParallelUnitMapping.toJSON(message.info?.parallelUnitMapping)
      : undefined);
    message.info?.$case === "node" &&
      (obj.node = message.info?.node ? WorkerNode.toJSON(message.info?.node) : undefined);
    message.info?.$case === "hummockSnapshot" && (obj.hummockSnapshot = message.info?.hummockSnapshot
      ? HummockSnapshot.toJSON(message.info?.hummockSnapshot)
      : undefined);
    message.info?.$case === "hummockVersionDeltas" && (obj.hummockVersionDeltas = message.info?.hummockVersionDeltas
      ? HummockVersionDeltas.toJSON(message.info?.hummockVersionDeltas)
      : undefined);
    message.info?.$case === "snapshot" &&
      (obj.snapshot = message.info?.snapshot ? MetaSnapshot.toJSON(message.info?.snapshot) : undefined);
    message.info?.$case === "metaBackupManifestId" && (obj.metaBackupManifestId = message.info?.metaBackupManifestId
      ? MetaBackupManifestId.toJSON(message.info?.metaBackupManifestId)
      : undefined);
    message.info?.$case === "systemParams" &&
      (obj.systemParams = message.info?.systemParams ? SystemParams.toJSON(message.info?.systemParams) : undefined);
    message.info?.$case === "hummockWriteLimits" && (obj.hummockWriteLimits = message.info?.hummockWriteLimits
      ? WriteLimits.toJSON(message.info?.hummockWriteLimits)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeResponse>, I>>(object: I): SubscribeResponse {
    const message = createBaseSubscribeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.operation = object.operation ?? SubscribeResponse_Operation.UNSPECIFIED;
    message.version = object.version ?? 0;
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
    if (object.info?.$case === "view" && object.info?.view !== undefined && object.info?.view !== null) {
      message.info = { $case: "view", view: View.fromPartial(object.info.view) };
    }
    if (object.info?.$case === "function" && object.info?.function !== undefined && object.info?.function !== null) {
      message.info = { $case: "function", function: Function.fromPartial(object.info.function) };
    }
    if (object.info?.$case === "user" && object.info?.user !== undefined && object.info?.user !== null) {
      message.info = { $case: "user", user: UserInfo.fromPartial(object.info.user) };
    }
    if (
      object.info?.$case === "parallelUnitMapping" &&
      object.info?.parallelUnitMapping !== undefined &&
      object.info?.parallelUnitMapping !== null
    ) {
      message.info = {
        $case: "parallelUnitMapping",
        parallelUnitMapping: FragmentParallelUnitMapping.fromPartial(object.info.parallelUnitMapping),
      };
    }
    if (object.info?.$case === "node" && object.info?.node !== undefined && object.info?.node !== null) {
      message.info = { $case: "node", node: WorkerNode.fromPartial(object.info.node) };
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
    if (
      object.info?.$case === "metaBackupManifestId" &&
      object.info?.metaBackupManifestId !== undefined &&
      object.info?.metaBackupManifestId !== null
    ) {
      message.info = {
        $case: "metaBackupManifestId",
        metaBackupManifestId: MetaBackupManifestId.fromPartial(object.info.metaBackupManifestId),
      };
    }
    if (
      object.info?.$case === "systemParams" &&
      object.info?.systemParams !== undefined &&
      object.info?.systemParams !== null
    ) {
      message.info = { $case: "systemParams", systemParams: SystemParams.fromPartial(object.info.systemParams) };
    }
    if (
      object.info?.$case === "hummockWriteLimits" &&
      object.info?.hummockWriteLimits !== undefined &&
      object.info?.hummockWriteLimits !== null
    ) {
      message.info = {
        $case: "hummockWriteLimits",
        hummockWriteLimits: WriteLimits.fromPartial(object.info.hummockWriteLimits),
      };
    }
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
  return { workerNodes: [], tableFragments: [], actorSplits: {}, sourceInfos: {} };
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
      sourceInfos: isObject(object.sourceInfos)
        ? Object.entries(object.sourceInfos).reduce<{ [key: number]: Source }>((acc, [key, value]) => {
          acc[Number(key)] = Source.fromJSON(value);
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
    obj.sourceInfos = {};
    if (message.sourceInfos) {
      Object.entries(message.sourceInfos).forEach(([k, v]) => {
        obj.sourceInfos[k] = Source.toJSON(v);
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
    message.sourceInfos = Object.entries(object.sourceInfos ?? {}).reduce<{ [key: number]: Source }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = Source.fromPartial(value);
        }
        return acc;
      },
      {},
    );
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

function createBaseGetClusterInfoResponse_SourceInfosEntry(): GetClusterInfoResponse_SourceInfosEntry {
  return { key: 0, value: undefined };
}

export const GetClusterInfoResponse_SourceInfosEntry = {
  fromJSON(object: any): GetClusterInfoResponse_SourceInfosEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? Source.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: GetClusterInfoResponse_SourceInfosEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? Source.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetClusterInfoResponse_SourceInfosEntry>, I>>(
    object: I,
  ): GetClusterInfoResponse_SourceInfosEntry {
    const message = createBaseGetClusterInfoResponse_SourceInfosEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? Source.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseRescheduleRequest(): RescheduleRequest {
  return { reschedules: {} };
}

export const RescheduleRequest = {
  fromJSON(object: any): RescheduleRequest {
    return {
      reschedules: isObject(object.reschedules)
        ? Object.entries(object.reschedules).reduce<{ [key: number]: RescheduleRequest_Reschedule }>(
          (acc, [key, value]) => {
            acc[Number(key)] = RescheduleRequest_Reschedule.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
    };
  },

  toJSON(message: RescheduleRequest): unknown {
    const obj: any = {};
    obj.reschedules = {};
    if (message.reschedules) {
      Object.entries(message.reschedules).forEach(([k, v]) => {
        obj.reschedules[k] = RescheduleRequest_Reschedule.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RescheduleRequest>, I>>(object: I): RescheduleRequest {
    const message = createBaseRescheduleRequest();
    message.reschedules = Object.entries(object.reschedules ?? {}).reduce<
      { [key: number]: RescheduleRequest_Reschedule }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = RescheduleRequest_Reschedule.fromPartial(value);
      }
      return acc;
    }, {});
    return message;
  },
};

function createBaseRescheduleRequest_Reschedule(): RescheduleRequest_Reschedule {
  return { addedParallelUnits: [], removedParallelUnits: [] };
}

export const RescheduleRequest_Reschedule = {
  fromJSON(object: any): RescheduleRequest_Reschedule {
    return {
      addedParallelUnits: Array.isArray(object?.addedParallelUnits)
        ? object.addedParallelUnits.map((e: any) => Number(e))
        : [],
      removedParallelUnits: Array.isArray(object?.removedParallelUnits)
        ? object.removedParallelUnits.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: RescheduleRequest_Reschedule): unknown {
    const obj: any = {};
    if (message.addedParallelUnits) {
      obj.addedParallelUnits = message.addedParallelUnits.map((e) => Math.round(e));
    } else {
      obj.addedParallelUnits = [];
    }
    if (message.removedParallelUnits) {
      obj.removedParallelUnits = message.removedParallelUnits.map((e) => Math.round(e));
    } else {
      obj.removedParallelUnits = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RescheduleRequest_Reschedule>, I>>(object: I): RescheduleRequest_Reschedule {
    const message = createBaseRescheduleRequest_Reschedule();
    message.addedParallelUnits = object.addedParallelUnits?.map((e) => e) || [];
    message.removedParallelUnits = object.removedParallelUnits?.map((e) => e) || [];
    return message;
  },
};

function createBaseRescheduleRequest_ReschedulesEntry(): RescheduleRequest_ReschedulesEntry {
  return { key: 0, value: undefined };
}

export const RescheduleRequest_ReschedulesEntry = {
  fromJSON(object: any): RescheduleRequest_ReschedulesEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? RescheduleRequest_Reschedule.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: RescheduleRequest_ReschedulesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? RescheduleRequest_Reschedule.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RescheduleRequest_ReschedulesEntry>, I>>(
    object: I,
  ): RescheduleRequest_ReschedulesEntry {
    const message = createBaseRescheduleRequest_ReschedulesEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? RescheduleRequest_Reschedule.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseRescheduleResponse(): RescheduleResponse {
  return { success: false };
}

export const RescheduleResponse = {
  fromJSON(object: any): RescheduleResponse {
    return { success: isSet(object.success) ? Boolean(object.success) : false };
  },

  toJSON(message: RescheduleResponse): unknown {
    const obj: any = {};
    message.success !== undefined && (obj.success = message.success);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RescheduleResponse>, I>>(object: I): RescheduleResponse {
    const message = createBaseRescheduleResponse();
    message.success = object.success ?? false;
    return message;
  },
};

function createBaseMembersRequest(): MembersRequest {
  return {};
}

export const MembersRequest = {
  fromJSON(_: any): MembersRequest {
    return {};
  },

  toJSON(_: MembersRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MembersRequest>, I>>(_: I): MembersRequest {
    const message = createBaseMembersRequest();
    return message;
  },
};

function createBaseMetaMember(): MetaMember {
  return { address: undefined, isLeader: false };
}

export const MetaMember = {
  fromJSON(object: any): MetaMember {
    return {
      address: isSet(object.address) ? HostAddress.fromJSON(object.address) : undefined,
      isLeader: isSet(object.isLeader) ? Boolean(object.isLeader) : false,
    };
  },

  toJSON(message: MetaMember): unknown {
    const obj: any = {};
    message.address !== undefined && (obj.address = message.address ? HostAddress.toJSON(message.address) : undefined);
    message.isLeader !== undefined && (obj.isLeader = message.isLeader);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MetaMember>, I>>(object: I): MetaMember {
    const message = createBaseMetaMember();
    message.address = (object.address !== undefined && object.address !== null)
      ? HostAddress.fromPartial(object.address)
      : undefined;
    message.isLeader = object.isLeader ?? false;
    return message;
  },
};

function createBaseMembersResponse(): MembersResponse {
  return { members: [] };
}

export const MembersResponse = {
  fromJSON(object: any): MembersResponse {
    return { members: Array.isArray(object?.members) ? object.members.map((e: any) => MetaMember.fromJSON(e)) : [] };
  },

  toJSON(message: MembersResponse): unknown {
    const obj: any = {};
    if (message.members) {
      obj.members = message.members.map((e) => e ? MetaMember.toJSON(e) : undefined);
    } else {
      obj.members = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MembersResponse>, I>>(object: I): MembersResponse {
    const message = createBaseMembersResponse();
    message.members = object.members?.map((e) => MetaMember.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSystemParams(): SystemParams {
  return {
    barrierIntervalMs: undefined,
    checkpointFrequency: undefined,
    sstableSizeMb: undefined,
    blockSizeKb: undefined,
    bloomFalsePositive: undefined,
    stateStore: undefined,
    dataDirectory: undefined,
    backupStorageUrl: undefined,
    backupStorageDirectory: undefined,
  };
}

export const SystemParams = {
  fromJSON(object: any): SystemParams {
    return {
      barrierIntervalMs: isSet(object.barrierIntervalMs) ? Number(object.barrierIntervalMs) : undefined,
      checkpointFrequency: isSet(object.checkpointFrequency) ? Number(object.checkpointFrequency) : undefined,
      sstableSizeMb: isSet(object.sstableSizeMb) ? Number(object.sstableSizeMb) : undefined,
      blockSizeKb: isSet(object.blockSizeKb) ? Number(object.blockSizeKb) : undefined,
      bloomFalsePositive: isSet(object.bloomFalsePositive) ? Number(object.bloomFalsePositive) : undefined,
      stateStore: isSet(object.stateStore) ? String(object.stateStore) : undefined,
      dataDirectory: isSet(object.dataDirectory) ? String(object.dataDirectory) : undefined,
      backupStorageUrl: isSet(object.backupStorageUrl) ? String(object.backupStorageUrl) : undefined,
      backupStorageDirectory: isSet(object.backupStorageDirectory) ? String(object.backupStorageDirectory) : undefined,
    };
  },

  toJSON(message: SystemParams): unknown {
    const obj: any = {};
    message.barrierIntervalMs !== undefined && (obj.barrierIntervalMs = Math.round(message.barrierIntervalMs));
    message.checkpointFrequency !== undefined && (obj.checkpointFrequency = Math.round(message.checkpointFrequency));
    message.sstableSizeMb !== undefined && (obj.sstableSizeMb = Math.round(message.sstableSizeMb));
    message.blockSizeKb !== undefined && (obj.blockSizeKb = Math.round(message.blockSizeKb));
    message.bloomFalsePositive !== undefined && (obj.bloomFalsePositive = message.bloomFalsePositive);
    message.stateStore !== undefined && (obj.stateStore = message.stateStore);
    message.dataDirectory !== undefined && (obj.dataDirectory = message.dataDirectory);
    message.backupStorageUrl !== undefined && (obj.backupStorageUrl = message.backupStorageUrl);
    message.backupStorageDirectory !== undefined && (obj.backupStorageDirectory = message.backupStorageDirectory);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SystemParams>, I>>(object: I): SystemParams {
    const message = createBaseSystemParams();
    message.barrierIntervalMs = object.barrierIntervalMs ?? undefined;
    message.checkpointFrequency = object.checkpointFrequency ?? undefined;
    message.sstableSizeMb = object.sstableSizeMb ?? undefined;
    message.blockSizeKb = object.blockSizeKb ?? undefined;
    message.bloomFalsePositive = object.bloomFalsePositive ?? undefined;
    message.stateStore = object.stateStore ?? undefined;
    message.dataDirectory = object.dataDirectory ?? undefined;
    message.backupStorageUrl = object.backupStorageUrl ?? undefined;
    message.backupStorageDirectory = object.backupStorageDirectory ?? undefined;
    return message;
  },
};

function createBaseGetSystemParamsRequest(): GetSystemParamsRequest {
  return {};
}

export const GetSystemParamsRequest = {
  fromJSON(_: any): GetSystemParamsRequest {
    return {};
  },

  toJSON(_: GetSystemParamsRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetSystemParamsRequest>, I>>(_: I): GetSystemParamsRequest {
    const message = createBaseGetSystemParamsRequest();
    return message;
  },
};

function createBaseGetSystemParamsResponse(): GetSystemParamsResponse {
  return { params: undefined };
}

export const GetSystemParamsResponse = {
  fromJSON(object: any): GetSystemParamsResponse {
    return { params: isSet(object.params) ? SystemParams.fromJSON(object.params) : undefined };
  },

  toJSON(message: GetSystemParamsResponse): unknown {
    const obj: any = {};
    message.params !== undefined && (obj.params = message.params ? SystemParams.toJSON(message.params) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetSystemParamsResponse>, I>>(object: I): GetSystemParamsResponse {
    const message = createBaseGetSystemParamsResponse();
    message.params = (object.params !== undefined && object.params !== null)
      ? SystemParams.fromPartial(object.params)
      : undefined;
    return message;
  },
};

function createBaseSetSystemParamRequest(): SetSystemParamRequest {
  return { param: "", value: undefined };
}

export const SetSystemParamRequest = {
  fromJSON(object: any): SetSystemParamRequest {
    return {
      param: isSet(object.param) ? String(object.param) : "",
      value: isSet(object.value) ? String(object.value) : undefined,
    };
  },

  toJSON(message: SetSystemParamRequest): unknown {
    const obj: any = {};
    message.param !== undefined && (obj.param = message.param);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SetSystemParamRequest>, I>>(object: I): SetSystemParamRequest {
    const message = createBaseSetSystemParamRequest();
    message.param = object.param ?? "";
    message.value = object.value ?? undefined;
    return message;
  },
};

function createBaseSetSystemParamResponse(): SetSystemParamResponse {
  return {};
}

export const SetSystemParamResponse = {
  fromJSON(_: any): SetSystemParamResponse {
    return {};
  },

  toJSON(_: SetSystemParamResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SetSystemParamResponse>, I>>(_: I): SetSystemParamResponse {
    const message = createBaseSetSystemParamResponse();
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
