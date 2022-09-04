/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Status } from "./common";

export const protobufPackage = "hummock";

export enum LevelType {
  UNSPECIFIED = 0,
  NONOVERLAPPING = 1,
  OVERLAPPING = 2,
  UNRECOGNIZED = -1,
}

export function levelTypeFromJSON(object: any): LevelType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return LevelType.UNSPECIFIED;
    case 1:
    case "NONOVERLAPPING":
      return LevelType.NONOVERLAPPING;
    case 2:
    case "OVERLAPPING":
      return LevelType.OVERLAPPING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return LevelType.UNRECOGNIZED;
  }
}

export function levelTypeToJSON(object: LevelType): string {
  switch (object) {
    case LevelType.UNSPECIFIED:
      return "UNSPECIFIED";
    case LevelType.NONOVERLAPPING:
      return "NONOVERLAPPING";
    case LevelType.OVERLAPPING:
      return "OVERLAPPING";
    case LevelType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface SstableInfo {
  id: number;
  keyRange: KeyRange | undefined;
  fileSize: number;
  tableIds: number[];
}

export interface OverlappingLevel {
  subLevels: Level[];
  totalFileSize: number;
}

export interface Level {
  levelIdx: number;
  levelType: LevelType;
  tableInfos: SstableInfo[];
  totalFileSize: number;
  subLevelId: number;
}

export interface InputLevel {
  levelIdx: number;
  levelType: LevelType;
  tableInfos: SstableInfo[];
}

export interface LevelDelta {
  levelIdx: number;
  l0SubLevelId: number;
  removedTableIds: number[];
  insertedTableInfos: SstableInfo[];
}

export interface UncommittedEpoch {
  epoch: number;
  tables: SstableInfo[];
}

export interface HummockVersion {
  id: number;
  /** Levels of each compaction group */
  levels: { [key: number]: HummockVersion_Levels };
  maxCommittedEpoch: number;
  /**
   * Snapshots with epoch less than the safe epoch have been GCed.
   * Reads against such an epoch will fail.
   */
  safeEpoch: number;
  maxCurrentEpoch: number;
}

export interface HummockVersion_Levels {
  levels: Level[];
  l0: OverlappingLevel | undefined;
}

export interface HummockVersion_LevelsEntry {
  key: number;
  value: HummockVersion_Levels | undefined;
}

export interface HummockVersionDelta {
  id: number;
  prevId: number;
  /** Levels of each compaction group */
  levelDeltas: { [key: number]: HummockVersionDelta_LevelDeltas };
  maxCommittedEpoch: number;
  /**
   * Snapshots with epoch less than the safe epoch have been GCed.
   * Reads against such an epoch will fail.
   */
  safeEpoch: number;
  trivialMove: boolean;
  maxCurrentEpoch: number;
}

export interface HummockVersionDelta_LevelDeltas {
  levelDeltas: LevelDelta[];
}

export interface HummockVersionDelta_LevelDeltasEntry {
  key: number;
  value: HummockVersionDelta_LevelDeltas | undefined;
}

export interface HummockVersionDeltas {
  versionDeltas: HummockVersionDelta[];
}

/** We will have two epoch after decouple */
export interface HummockSnapshot {
  /** Epoch with checkpoint, we will read durable data with it. */
  committedEpoch: number;
  /** Epoch without checkpoint, we will read real-time data with it. But it may be rolled back. */
  currentEpoch: number;
}

export interface PinVersionRequest {
  contextId: number;
  lastPinned: number;
}

export interface PinVersionResponse {
  status: Status | undefined;
  payload?: { $case: "versionDeltas"; versionDeltas: PinVersionResponse_HummockVersionDeltas } | {
    $case: "pinnedVersion";
    pinnedVersion: HummockVersion;
  };
}

export interface PinVersionResponse_HummockVersionDeltas {
  delta: HummockVersionDelta[];
}

export interface UnpinVersionBeforeRequest {
  contextId: number;
  unpinVersionBefore: number;
}

export interface UnpinVersionBeforeResponse {
  status: Status | undefined;
}

export interface UnpinVersionRequest {
  contextId: number;
}

export interface UnpinVersionResponse {
  status: Status | undefined;
}

export interface PinSnapshotRequest {
  contextId: number;
}

export interface PinSnapshotResponse {
  status: Status | undefined;
  snapshot: HummockSnapshot | undefined;
}

export interface GetEpochRequest {
}

export interface GetEpochResponse {
  status: Status | undefined;
  snapshot: HummockSnapshot | undefined;
}

export interface UnpinSnapshotRequest {
  contextId: number;
}

export interface UnpinSnapshotResponse {
  status: Status | undefined;
}

export interface UnpinSnapshotBeforeRequest {
  contextId: number;
  minSnapshot: HummockSnapshot | undefined;
}

export interface UnpinSnapshotBeforeResponse {
  status: Status | undefined;
}

export interface KeyRange {
  left: Uint8Array;
  right: Uint8Array;
  inf: boolean;
}

export interface TableOption {
  retentionSeconds: number;
}

export interface CompactTask {
  /** SSTs to be compacted, which will be removed from LSM after compaction */
  inputSsts: InputLevel[];
  /**
   * In ideal case, the compaction will generate `splits.len()` tables which have key range
   * corresponding to that in [`splits`], respectively
   */
  splits: KeyRange[];
  /** low watermark in 'ts-aware compaction' */
  watermark: number;
  /** compaction output, which will be added to [`target_level`] of LSM after compaction */
  sortedOutputSsts: SstableInfo[];
  /** task id assigned by hummock storage service */
  taskId: number;
  /** compaction output will be added to [`target_level`] of LSM after compaction */
  targetLevel: number;
  gcDeleteKeys: boolean;
  taskStatus: CompactTask_TaskStatus;
  /** compaction group the task belongs to */
  compactionGroupId: number;
  /** existing_table_ids for compaction drop key */
  existingTableIds: number[];
  compressionAlgorithm: number;
  targetFileSize: number;
  compactionFilterMask: number;
  tableOptions: { [key: number]: TableOption };
  currentEpochTime: number;
  targetSubLevelId: number;
}

export enum CompactTask_TaskStatus {
  PENDING = 0,
  SUCCESS = 1,
  FAILED = 2,
  CANCELED = 3,
  UNRECOGNIZED = -1,
}

export function compactTask_TaskStatusFromJSON(object: any): CompactTask_TaskStatus {
  switch (object) {
    case 0:
    case "PENDING":
      return CompactTask_TaskStatus.PENDING;
    case 1:
    case "SUCCESS":
      return CompactTask_TaskStatus.SUCCESS;
    case 2:
    case "FAILED":
      return CompactTask_TaskStatus.FAILED;
    case 3:
    case "CANCELED":
      return CompactTask_TaskStatus.CANCELED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return CompactTask_TaskStatus.UNRECOGNIZED;
  }
}

export function compactTask_TaskStatusToJSON(object: CompactTask_TaskStatus): string {
  switch (object) {
    case CompactTask_TaskStatus.PENDING:
      return "PENDING";
    case CompactTask_TaskStatus.SUCCESS:
      return "SUCCESS";
    case CompactTask_TaskStatus.FAILED:
      return "FAILED";
    case CompactTask_TaskStatus.CANCELED:
      return "CANCELED";
    case CompactTask_TaskStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface CompactTask_TableOptionsEntry {
  key: number;
  value: TableOption | undefined;
}

export interface LevelHandler {
  level: number;
  tasks: LevelHandler_RunningCompactTask[];
}

export interface LevelHandler_RunningCompactTask {
  taskId: number;
  ssts: number[];
  totalFileSize: number;
  targetLevel: number;
}

export interface CompactStatus {
  compactionGroupId: number;
  levelHandlers: LevelHandler[];
}

export interface CompactionGroup {
  id: number;
  memberTableIds: number[];
  compactionConfig: CompactionConfig | undefined;
  tableIdToOptions: { [key: number]: TableOption };
}

export interface CompactionGroup_TableIdToOptionsEntry {
  key: number;
  value: TableOption | undefined;
}

export interface CompactTaskAssignment {
  compactTask: CompactTask | undefined;
  contextId: number;
}

export interface GetCompactionTasksRequest {
}

export interface GetCompactionTasksResponse {
  status: Status | undefined;
  compactTask: CompactTask | undefined;
}

export interface ReportCompactionTasksRequest {
  contextId: number;
  compactTask: CompactTask | undefined;
}

export interface ReportCompactionTasksResponse {
  status: Status | undefined;
}

export interface HummockPinnedVersion {
  contextId: number;
  minPinnedId: number;
}

export interface HummockPinnedSnapshot {
  contextId: number;
  minimalPinnedSnapshot: number;
}

export interface GetNewSstIdsRequest {
  number: number;
}

export interface GetNewSstIdsResponse {
  status:
    | Status
    | undefined;
  /** inclusive */
  startId: number;
  /** exclusive */
  endId: number;
}

export interface SubscribeCompactTasksRequest {
  contextId: number;
  maxConcurrentTaskNumber: number;
}

export interface ValidationTask {
  sstIds: number[];
  sstIdToWorkerId: { [key: number]: number };
  epoch: number;
}

export interface ValidationTask_SstIdToWorkerIdEntry {
  key: number;
  value: number;
}

export interface SubscribeCompactTasksResponse {
  task?: { $case: "compactTask"; compactTask: CompactTask } | { $case: "vacuumTask"; vacuumTask: VacuumTask } | {
    $case: "fullScanTask";
    fullScanTask: FullScanTask;
  } | { $case: "validationTask"; validationTask: ValidationTask };
}

/** Delete SSTs in object store */
export interface VacuumTask {
  sstableIds: number[];
}

/** Scan object store to get candidate orphan SSTs. */
export interface FullScanTask {
  sstRetentionTimeSec: number;
}

export interface ReportVacuumTaskRequest {
  vacuumTask: VacuumTask | undefined;
}

export interface ReportVacuumTaskResponse {
  status: Status | undefined;
}

export interface GetCompactionGroupsRequest {
}

export interface GetCompactionGroupsResponse {
  status: Status | undefined;
  compactionGroups: CompactionGroup[];
}

export interface TriggerManualCompactionRequest {
  compactionGroupId: number;
  keyRange: KeyRange | undefined;
  tableId: number;
  level: number;
  sstIds: number[];
}

export interface TriggerManualCompactionResponse {
  status: Status | undefined;
}

export interface ReportFullScanTaskRequest {
  sstIds: number[];
}

export interface ReportFullScanTaskResponse {
  status: Status | undefined;
}

export interface TriggerFullGCRequest {
  sstRetentionTimeSec: number;
}

export interface TriggerFullGCResponse {
  status: Status | undefined;
}

export interface CompactionConfig {
  maxBytesForLevelBase: number;
  maxLevel: number;
  maxBytesForLevelMultiplier: number;
  maxCompactionBytes: number;
  subLevelMaxCompactionBytes: number;
  level0TriggerFileNumber: number;
  level0TierCompactFileNumber: number;
  compactionMode: CompactionConfig_CompactionMode;
  compressionAlgorithm: string[];
  targetFileSizeBase: number;
  compactionFilterMask: number;
  maxSubCompaction: number;
}

export enum CompactionConfig_CompactionMode {
  UNSPECIFIED = 0,
  RANGE = 1,
  CONSISTENT_HASH = 2,
  UNRECOGNIZED = -1,
}

export function compactionConfig_CompactionModeFromJSON(object: any): CompactionConfig_CompactionMode {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return CompactionConfig_CompactionMode.UNSPECIFIED;
    case 1:
    case "RANGE":
      return CompactionConfig_CompactionMode.RANGE;
    case 2:
    case "CONSISTENT_HASH":
      return CompactionConfig_CompactionMode.CONSISTENT_HASH;
    case -1:
    case "UNRECOGNIZED":
    default:
      return CompactionConfig_CompactionMode.UNRECOGNIZED;
  }
}

export function compactionConfig_CompactionModeToJSON(object: CompactionConfig_CompactionMode): string {
  switch (object) {
    case CompactionConfig_CompactionMode.UNSPECIFIED:
      return "UNSPECIFIED";
    case CompactionConfig_CompactionMode.RANGE:
      return "RANGE";
    case CompactionConfig_CompactionMode.CONSISTENT_HASH:
      return "CONSISTENT_HASH";
    case CompactionConfig_CompactionMode.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

function createBaseSstableInfo(): SstableInfo {
  return { id: 0, keyRange: undefined, fileSize: 0, tableIds: [] };
}

export const SstableInfo = {
  encode(message: SstableInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint64(message.id);
    }
    if (message.keyRange !== undefined) {
      KeyRange.encode(message.keyRange, writer.uint32(18).fork()).ldelim();
    }
    if (message.fileSize !== 0) {
      writer.uint32(24).uint64(message.fileSize);
    }
    writer.uint32(34).fork();
    for (const v of message.tableIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SstableInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSstableInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.keyRange = KeyRange.decode(reader, reader.uint32());
          break;
        case 3:
          message.fileSize = longToNumber(reader.uint64() as Long);
          break;
        case 4:
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

  fromJSON(object: any): SstableInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      keyRange: isSet(object.keyRange) ? KeyRange.fromJSON(object.keyRange) : undefined,
      fileSize: isSet(object.fileSize) ? Number(object.fileSize) : 0,
      tableIds: Array.isArray(object?.tableIds) ? object.tableIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: SstableInfo): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.keyRange !== undefined && (obj.keyRange = message.keyRange ? KeyRange.toJSON(message.keyRange) : undefined);
    message.fileSize !== undefined && (obj.fileSize = Math.round(message.fileSize));
    if (message.tableIds) {
      obj.tableIds = message.tableIds.map((e) => Math.round(e));
    } else {
      obj.tableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SstableInfo>, I>>(object: I): SstableInfo {
    const message = createBaseSstableInfo();
    message.id = object.id ?? 0;
    message.keyRange = (object.keyRange !== undefined && object.keyRange !== null)
      ? KeyRange.fromPartial(object.keyRange)
      : undefined;
    message.fileSize = object.fileSize ?? 0;
    message.tableIds = object.tableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseOverlappingLevel(): OverlappingLevel {
  return { subLevels: [], totalFileSize: 0 };
}

export const OverlappingLevel = {
  encode(message: OverlappingLevel, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.subLevels) {
      Level.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    if (message.totalFileSize !== 0) {
      writer.uint32(16).uint64(message.totalFileSize);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): OverlappingLevel {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseOverlappingLevel();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.subLevels.push(Level.decode(reader, reader.uint32()));
          break;
        case 2:
          message.totalFileSize = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): OverlappingLevel {
    return {
      subLevels: Array.isArray(object?.subLevels) ? object.subLevels.map((e: any) => Level.fromJSON(e)) : [],
      totalFileSize: isSet(object.totalFileSize) ? Number(object.totalFileSize) : 0,
    };
  },

  toJSON(message: OverlappingLevel): unknown {
    const obj: any = {};
    if (message.subLevels) {
      obj.subLevels = message.subLevels.map((e) => e ? Level.toJSON(e) : undefined);
    } else {
      obj.subLevels = [];
    }
    message.totalFileSize !== undefined && (obj.totalFileSize = Math.round(message.totalFileSize));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<OverlappingLevel>, I>>(object: I): OverlappingLevel {
    const message = createBaseOverlappingLevel();
    message.subLevels = object.subLevels?.map((e) => Level.fromPartial(e)) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    return message;
  },
};

function createBaseLevel(): Level {
  return { levelIdx: 0, levelType: 0, tableInfos: [], totalFileSize: 0, subLevelId: 0 };
}

export const Level = {
  encode(message: Level, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.levelIdx !== 0) {
      writer.uint32(8).uint32(message.levelIdx);
    }
    if (message.levelType !== 0) {
      writer.uint32(16).int32(message.levelType);
    }
    for (const v of message.tableInfos) {
      SstableInfo.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    if (message.totalFileSize !== 0) {
      writer.uint32(32).uint64(message.totalFileSize);
    }
    if (message.subLevelId !== 0) {
      writer.uint32(40).uint64(message.subLevelId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Level {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLevel();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.levelIdx = reader.uint32();
          break;
        case 2:
          message.levelType = reader.int32() as any;
          break;
        case 3:
          message.tableInfos.push(SstableInfo.decode(reader, reader.uint32()));
          break;
        case 4:
          message.totalFileSize = longToNumber(reader.uint64() as Long);
          break;
        case 5:
          message.subLevelId = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): Level {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      levelType: isSet(object.levelType) ? levelTypeFromJSON(object.levelType) : 0,
      tableInfos: Array.isArray(object?.tableInfos) ? object.tableInfos.map((e: any) => SstableInfo.fromJSON(e)) : [],
      totalFileSize: isSet(object.totalFileSize) ? Number(object.totalFileSize) : 0,
      subLevelId: isSet(object.subLevelId) ? Number(object.subLevelId) : 0,
    };
  },

  toJSON(message: Level): unknown {
    const obj: any = {};
    message.levelIdx !== undefined && (obj.levelIdx = Math.round(message.levelIdx));
    message.levelType !== undefined && (obj.levelType = levelTypeToJSON(message.levelType));
    if (message.tableInfos) {
      obj.tableInfos = message.tableInfos.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.tableInfos = [];
    }
    message.totalFileSize !== undefined && (obj.totalFileSize = Math.round(message.totalFileSize));
    message.subLevelId !== undefined && (obj.subLevelId = Math.round(message.subLevelId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Level>, I>>(object: I): Level {
    const message = createBaseLevel();
    message.levelIdx = object.levelIdx ?? 0;
    message.levelType = object.levelType ?? 0;
    message.tableInfos = object.tableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    message.subLevelId = object.subLevelId ?? 0;
    return message;
  },
};

function createBaseInputLevel(): InputLevel {
  return { levelIdx: 0, levelType: 0, tableInfos: [] };
}

export const InputLevel = {
  encode(message: InputLevel, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.levelIdx !== 0) {
      writer.uint32(8).uint32(message.levelIdx);
    }
    if (message.levelType !== 0) {
      writer.uint32(16).int32(message.levelType);
    }
    for (const v of message.tableInfos) {
      SstableInfo.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): InputLevel {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseInputLevel();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.levelIdx = reader.uint32();
          break;
        case 2:
          message.levelType = reader.int32() as any;
          break;
        case 3:
          message.tableInfos.push(SstableInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): InputLevel {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      levelType: isSet(object.levelType) ? levelTypeFromJSON(object.levelType) : 0,
      tableInfos: Array.isArray(object?.tableInfos) ? object.tableInfos.map((e: any) => SstableInfo.fromJSON(e)) : [],
    };
  },

  toJSON(message: InputLevel): unknown {
    const obj: any = {};
    message.levelIdx !== undefined && (obj.levelIdx = Math.round(message.levelIdx));
    message.levelType !== undefined && (obj.levelType = levelTypeToJSON(message.levelType));
    if (message.tableInfos) {
      obj.tableInfos = message.tableInfos.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.tableInfos = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InputLevel>, I>>(object: I): InputLevel {
    const message = createBaseInputLevel();
    message.levelIdx = object.levelIdx ?? 0;
    message.levelType = object.levelType ?? 0;
    message.tableInfos = object.tableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseLevelDelta(): LevelDelta {
  return { levelIdx: 0, l0SubLevelId: 0, removedTableIds: [], insertedTableInfos: [] };
}

export const LevelDelta = {
  encode(message: LevelDelta, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.levelIdx !== 0) {
      writer.uint32(8).uint32(message.levelIdx);
    }
    if (message.l0SubLevelId !== 0) {
      writer.uint32(16).uint64(message.l0SubLevelId);
    }
    writer.uint32(26).fork();
    for (const v of message.removedTableIds) {
      writer.uint64(v);
    }
    writer.ldelim();
    for (const v of message.insertedTableInfos) {
      SstableInfo.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LevelDelta {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLevelDelta();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.levelIdx = reader.uint32();
          break;
        case 2:
          message.l0SubLevelId = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.removedTableIds.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.removedTableIds.push(longToNumber(reader.uint64() as Long));
          }
          break;
        case 4:
          message.insertedTableInfos.push(SstableInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LevelDelta {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      l0SubLevelId: isSet(object.l0SubLevelId) ? Number(object.l0SubLevelId) : 0,
      removedTableIds: Array.isArray(object?.removedTableIds) ? object.removedTableIds.map((e: any) => Number(e)) : [],
      insertedTableInfos: Array.isArray(object?.insertedTableInfos)
        ? object.insertedTableInfos.map((e: any) => SstableInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: LevelDelta): unknown {
    const obj: any = {};
    message.levelIdx !== undefined && (obj.levelIdx = Math.round(message.levelIdx));
    message.l0SubLevelId !== undefined && (obj.l0SubLevelId = Math.round(message.l0SubLevelId));
    if (message.removedTableIds) {
      obj.removedTableIds = message.removedTableIds.map((e) => Math.round(e));
    } else {
      obj.removedTableIds = [];
    }
    if (message.insertedTableInfos) {
      obj.insertedTableInfos = message.insertedTableInfos.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.insertedTableInfos = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LevelDelta>, I>>(object: I): LevelDelta {
    const message = createBaseLevelDelta();
    message.levelIdx = object.levelIdx ?? 0;
    message.l0SubLevelId = object.l0SubLevelId ?? 0;
    message.removedTableIds = object.removedTableIds?.map((e) => e) || [];
    message.insertedTableInfos = object.insertedTableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUncommittedEpoch(): UncommittedEpoch {
  return { epoch: 0, tables: [] };
}

export const UncommittedEpoch = {
  encode(message: UncommittedEpoch, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.epoch !== 0) {
      writer.uint32(8).uint64(message.epoch);
    }
    for (const v of message.tables) {
      SstableInfo.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UncommittedEpoch {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUncommittedEpoch();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.epoch = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.tables.push(SstableInfo.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UncommittedEpoch {
    return {
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
      tables: Array.isArray(object?.tables) ? object.tables.map((e: any) => SstableInfo.fromJSON(e)) : [],
    };
  },

  toJSON(message: UncommittedEpoch): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    if (message.tables) {
      obj.tables = message.tables.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.tables = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UncommittedEpoch>, I>>(object: I): UncommittedEpoch {
    const message = createBaseUncommittedEpoch();
    message.epoch = object.epoch ?? 0;
    message.tables = object.tables?.map((e) => SstableInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHummockVersion(): HummockVersion {
  return { id: 0, levels: {}, maxCommittedEpoch: 0, safeEpoch: 0, maxCurrentEpoch: 0 };
}

export const HummockVersion = {
  encode(message: HummockVersion, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint64(message.id);
    }
    Object.entries(message.levels).forEach(([key, value]) => {
      HummockVersion_LevelsEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    if (message.maxCommittedEpoch !== 0) {
      writer.uint32(32).uint64(message.maxCommittedEpoch);
    }
    if (message.safeEpoch !== 0) {
      writer.uint32(40).uint64(message.safeEpoch);
    }
    if (message.maxCurrentEpoch !== 0) {
      writer.uint32(48).uint64(message.maxCurrentEpoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersion {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersion();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          const entry2 = HummockVersion_LevelsEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.levels[entry2.key] = entry2.value;
          }
          break;
        case 4:
          message.maxCommittedEpoch = longToNumber(reader.uint64() as Long);
          break;
        case 5:
          message.safeEpoch = longToNumber(reader.uint64() as Long);
          break;
        case 6:
          message.maxCurrentEpoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersion {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      levels: isObject(object.levels)
        ? Object.entries(object.levels).reduce<{ [key: number]: HummockVersion_Levels }>((acc, [key, value]) => {
          acc[Number(key)] = HummockVersion_Levels.fromJSON(value);
          return acc;
        }, {})
        : {},
      maxCommittedEpoch: isSet(object.maxCommittedEpoch) ? Number(object.maxCommittedEpoch) : 0,
      safeEpoch: isSet(object.safeEpoch) ? Number(object.safeEpoch) : 0,
      maxCurrentEpoch: isSet(object.maxCurrentEpoch) ? Number(object.maxCurrentEpoch) : 0,
    };
  },

  toJSON(message: HummockVersion): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    obj.levels = {};
    if (message.levels) {
      Object.entries(message.levels).forEach(([k, v]) => {
        obj.levels[k] = HummockVersion_Levels.toJSON(v);
      });
    }
    message.maxCommittedEpoch !== undefined && (obj.maxCommittedEpoch = Math.round(message.maxCommittedEpoch));
    message.safeEpoch !== undefined && (obj.safeEpoch = Math.round(message.safeEpoch));
    message.maxCurrentEpoch !== undefined && (obj.maxCurrentEpoch = Math.round(message.maxCurrentEpoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersion>, I>>(object: I): HummockVersion {
    const message = createBaseHummockVersion();
    message.id = object.id ?? 0;
    message.levels = Object.entries(object.levels ?? {}).reduce<{ [key: number]: HummockVersion_Levels }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = HummockVersion_Levels.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.maxCommittedEpoch = object.maxCommittedEpoch ?? 0;
    message.safeEpoch = object.safeEpoch ?? 0;
    message.maxCurrentEpoch = object.maxCurrentEpoch ?? 0;
    return message;
  },
};

function createBaseHummockVersion_Levels(): HummockVersion_Levels {
  return { levels: [], l0: undefined };
}

export const HummockVersion_Levels = {
  encode(message: HummockVersion_Levels, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.levels) {
      Level.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    if (message.l0 !== undefined) {
      OverlappingLevel.encode(message.l0, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersion_Levels {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersion_Levels();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.levels.push(Level.decode(reader, reader.uint32()));
          break;
        case 2:
          message.l0 = OverlappingLevel.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersion_Levels {
    return {
      levels: Array.isArray(object?.levels) ? object.levels.map((e: any) => Level.fromJSON(e)) : [],
      l0: isSet(object.l0) ? OverlappingLevel.fromJSON(object.l0) : undefined,
    };
  },

  toJSON(message: HummockVersion_Levels): unknown {
    const obj: any = {};
    if (message.levels) {
      obj.levels = message.levels.map((e) => e ? Level.toJSON(e) : undefined);
    } else {
      obj.levels = [];
    }
    message.l0 !== undefined && (obj.l0 = message.l0 ? OverlappingLevel.toJSON(message.l0) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersion_Levels>, I>>(object: I): HummockVersion_Levels {
    const message = createBaseHummockVersion_Levels();
    message.levels = object.levels?.map((e) => Level.fromPartial(e)) || [];
    message.l0 = (object.l0 !== undefined && object.l0 !== null) ? OverlappingLevel.fromPartial(object.l0) : undefined;
    return message;
  },
};

function createBaseHummockVersion_LevelsEntry(): HummockVersion_LevelsEntry {
  return { key: 0, value: undefined };
}

export const HummockVersion_LevelsEntry = {
  encode(message: HummockVersion_LevelsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint64(message.key);
    }
    if (message.value !== undefined) {
      HummockVersion_Levels.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersion_LevelsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersion_LevelsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.value = HummockVersion_Levels.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersion_LevelsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? HummockVersion_Levels.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: HummockVersion_LevelsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? HummockVersion_Levels.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersion_LevelsEntry>, I>>(object: I): HummockVersion_LevelsEntry {
    const message = createBaseHummockVersion_LevelsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? HummockVersion_Levels.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseHummockVersionDelta(): HummockVersionDelta {
  return {
    id: 0,
    prevId: 0,
    levelDeltas: {},
    maxCommittedEpoch: 0,
    safeEpoch: 0,
    trivialMove: false,
    maxCurrentEpoch: 0,
  };
}

export const HummockVersionDelta = {
  encode(message: HummockVersionDelta, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint64(message.id);
    }
    if (message.prevId !== 0) {
      writer.uint32(16).uint64(message.prevId);
    }
    Object.entries(message.levelDeltas).forEach(([key, value]) => {
      HummockVersionDelta_LevelDeltasEntry.encode({ key: key as any, value }, writer.uint32(26).fork()).ldelim();
    });
    if (message.maxCommittedEpoch !== 0) {
      writer.uint32(32).uint64(message.maxCommittedEpoch);
    }
    if (message.safeEpoch !== 0) {
      writer.uint32(40).uint64(message.safeEpoch);
    }
    if (message.trivialMove === true) {
      writer.uint32(48).bool(message.trivialMove);
    }
    if (message.maxCurrentEpoch !== 0) {
      writer.uint32(56).uint64(message.maxCurrentEpoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersionDelta {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersionDelta();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.prevId = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          const entry3 = HummockVersionDelta_LevelDeltasEntry.decode(reader, reader.uint32());
          if (entry3.value !== undefined) {
            message.levelDeltas[entry3.key] = entry3.value;
          }
          break;
        case 4:
          message.maxCommittedEpoch = longToNumber(reader.uint64() as Long);
          break;
        case 5:
          message.safeEpoch = longToNumber(reader.uint64() as Long);
          break;
        case 6:
          message.trivialMove = reader.bool();
          break;
        case 7:
          message.maxCurrentEpoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersionDelta {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      prevId: isSet(object.prevId) ? Number(object.prevId) : 0,
      levelDeltas: isObject(object.levelDeltas)
        ? Object.entries(object.levelDeltas).reduce<{ [key: number]: HummockVersionDelta_LevelDeltas }>(
          (acc, [key, value]) => {
            acc[Number(key)] = HummockVersionDelta_LevelDeltas.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
      maxCommittedEpoch: isSet(object.maxCommittedEpoch) ? Number(object.maxCommittedEpoch) : 0,
      safeEpoch: isSet(object.safeEpoch) ? Number(object.safeEpoch) : 0,
      trivialMove: isSet(object.trivialMove) ? Boolean(object.trivialMove) : false,
      maxCurrentEpoch: isSet(object.maxCurrentEpoch) ? Number(object.maxCurrentEpoch) : 0,
    };
  },

  toJSON(message: HummockVersionDelta): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.prevId !== undefined && (obj.prevId = Math.round(message.prevId));
    obj.levelDeltas = {};
    if (message.levelDeltas) {
      Object.entries(message.levelDeltas).forEach(([k, v]) => {
        obj.levelDeltas[k] = HummockVersionDelta_LevelDeltas.toJSON(v);
      });
    }
    message.maxCommittedEpoch !== undefined && (obj.maxCommittedEpoch = Math.round(message.maxCommittedEpoch));
    message.safeEpoch !== undefined && (obj.safeEpoch = Math.round(message.safeEpoch));
    message.trivialMove !== undefined && (obj.trivialMove = message.trivialMove);
    message.maxCurrentEpoch !== undefined && (obj.maxCurrentEpoch = Math.round(message.maxCurrentEpoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta>, I>>(object: I): HummockVersionDelta {
    const message = createBaseHummockVersionDelta();
    message.id = object.id ?? 0;
    message.prevId = object.prevId ?? 0;
    message.levelDeltas = Object.entries(object.levelDeltas ?? {}).reduce<
      { [key: number]: HummockVersionDelta_LevelDeltas }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = HummockVersionDelta_LevelDeltas.fromPartial(value);
      }
      return acc;
    }, {});
    message.maxCommittedEpoch = object.maxCommittedEpoch ?? 0;
    message.safeEpoch = object.safeEpoch ?? 0;
    message.trivialMove = object.trivialMove ?? false;
    message.maxCurrentEpoch = object.maxCurrentEpoch ?? 0;
    return message;
  },
};

function createBaseHummockVersionDelta_LevelDeltas(): HummockVersionDelta_LevelDeltas {
  return { levelDeltas: [] };
}

export const HummockVersionDelta_LevelDeltas = {
  encode(message: HummockVersionDelta_LevelDeltas, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.levelDeltas) {
      LevelDelta.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersionDelta_LevelDeltas {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersionDelta_LevelDeltas();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.levelDeltas.push(LevelDelta.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersionDelta_LevelDeltas {
    return {
      levelDeltas: Array.isArray(object?.levelDeltas) ? object.levelDeltas.map((e: any) => LevelDelta.fromJSON(e)) : [],
    };
  },

  toJSON(message: HummockVersionDelta_LevelDeltas): unknown {
    const obj: any = {};
    if (message.levelDeltas) {
      obj.levelDeltas = message.levelDeltas.map((e) => e ? LevelDelta.toJSON(e) : undefined);
    } else {
      obj.levelDeltas = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta_LevelDeltas>, I>>(
    object: I,
  ): HummockVersionDelta_LevelDeltas {
    const message = createBaseHummockVersionDelta_LevelDeltas();
    message.levelDeltas = object.levelDeltas?.map((e) => LevelDelta.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHummockVersionDelta_LevelDeltasEntry(): HummockVersionDelta_LevelDeltasEntry {
  return { key: 0, value: undefined };
}

export const HummockVersionDelta_LevelDeltasEntry = {
  encode(message: HummockVersionDelta_LevelDeltasEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint64(message.key);
    }
    if (message.value !== undefined) {
      HummockVersionDelta_LevelDeltas.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersionDelta_LevelDeltasEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersionDelta_LevelDeltasEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.value = HummockVersionDelta_LevelDeltas.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersionDelta_LevelDeltasEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? HummockVersionDelta_LevelDeltas.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: HummockVersionDelta_LevelDeltasEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? HummockVersionDelta_LevelDeltas.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta_LevelDeltasEntry>, I>>(
    object: I,
  ): HummockVersionDelta_LevelDeltasEntry {
    const message = createBaseHummockVersionDelta_LevelDeltasEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? HummockVersionDelta_LevelDeltas.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseHummockVersionDeltas(): HummockVersionDeltas {
  return { versionDeltas: [] };
}

export const HummockVersionDeltas = {
  encode(message: HummockVersionDeltas, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.versionDeltas) {
      HummockVersionDelta.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockVersionDeltas {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockVersionDeltas();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.versionDeltas.push(HummockVersionDelta.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockVersionDeltas {
    return {
      versionDeltas: Array.isArray(object?.versionDeltas)
        ? object.versionDeltas.map((e: any) => HummockVersionDelta.fromJSON(e))
        : [],
    };
  },

  toJSON(message: HummockVersionDeltas): unknown {
    const obj: any = {};
    if (message.versionDeltas) {
      obj.versionDeltas = message.versionDeltas.map((e) => e ? HummockVersionDelta.toJSON(e) : undefined);
    } else {
      obj.versionDeltas = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDeltas>, I>>(object: I): HummockVersionDeltas {
    const message = createBaseHummockVersionDeltas();
    message.versionDeltas = object.versionDeltas?.map((e) => HummockVersionDelta.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHummockSnapshot(): HummockSnapshot {
  return { committedEpoch: 0, currentEpoch: 0 };
}

export const HummockSnapshot = {
  encode(message: HummockSnapshot, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.committedEpoch !== 0) {
      writer.uint32(8).uint64(message.committedEpoch);
    }
    if (message.currentEpoch !== 0) {
      writer.uint32(16).uint64(message.currentEpoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockSnapshot {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockSnapshot();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.committedEpoch = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.currentEpoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockSnapshot {
    return {
      committedEpoch: isSet(object.committedEpoch) ? Number(object.committedEpoch) : 0,
      currentEpoch: isSet(object.currentEpoch) ? Number(object.currentEpoch) : 0,
    };
  },

  toJSON(message: HummockSnapshot): unknown {
    const obj: any = {};
    message.committedEpoch !== undefined && (obj.committedEpoch = Math.round(message.committedEpoch));
    message.currentEpoch !== undefined && (obj.currentEpoch = Math.round(message.currentEpoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockSnapshot>, I>>(object: I): HummockSnapshot {
    const message = createBaseHummockSnapshot();
    message.committedEpoch = object.committedEpoch ?? 0;
    message.currentEpoch = object.currentEpoch ?? 0;
    return message;
  },
};

function createBasePinVersionRequest(): PinVersionRequest {
  return { contextId: 0, lastPinned: 0 };
}

export const PinVersionRequest = {
  encode(message: PinVersionRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.lastPinned !== 0) {
      writer.uint32(16).uint64(message.lastPinned);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PinVersionRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePinVersionRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.lastPinned = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PinVersionRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      lastPinned: isSet(object.lastPinned) ? Number(object.lastPinned) : 0,
    };
  },

  toJSON(message: PinVersionRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.lastPinned !== undefined && (obj.lastPinned = Math.round(message.lastPinned));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinVersionRequest>, I>>(object: I): PinVersionRequest {
    const message = createBasePinVersionRequest();
    message.contextId = object.contextId ?? 0;
    message.lastPinned = object.lastPinned ?? 0;
    return message;
  },
};

function createBasePinVersionResponse(): PinVersionResponse {
  return { status: undefined, payload: undefined };
}

export const PinVersionResponse = {
  encode(message: PinVersionResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.payload?.$case === "versionDeltas") {
      PinVersionResponse_HummockVersionDeltas.encode(message.payload.versionDeltas, writer.uint32(18).fork()).ldelim();
    }
    if (message.payload?.$case === "pinnedVersion") {
      HummockVersion.encode(message.payload.pinnedVersion, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PinVersionResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePinVersionResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.payload = {
            $case: "versionDeltas",
            versionDeltas: PinVersionResponse_HummockVersionDeltas.decode(reader, reader.uint32()),
          };
          break;
        case 3:
          message.payload = { $case: "pinnedVersion", pinnedVersion: HummockVersion.decode(reader, reader.uint32()) };
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PinVersionResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      payload: isSet(object.versionDeltas)
        ? {
          $case: "versionDeltas",
          versionDeltas: PinVersionResponse_HummockVersionDeltas.fromJSON(object.versionDeltas),
        }
        : isSet(object.pinnedVersion)
        ? { $case: "pinnedVersion", pinnedVersion: HummockVersion.fromJSON(object.pinnedVersion) }
        : undefined,
    };
  },

  toJSON(message: PinVersionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.payload?.$case === "versionDeltas" && (obj.versionDeltas = message.payload?.versionDeltas
      ? PinVersionResponse_HummockVersionDeltas.toJSON(message.payload?.versionDeltas)
      : undefined);
    message.payload?.$case === "pinnedVersion" && (obj.pinnedVersion = message.payload?.pinnedVersion
      ? HummockVersion.toJSON(message.payload?.pinnedVersion)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinVersionResponse>, I>>(object: I): PinVersionResponse {
    const message = createBasePinVersionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    if (
      object.payload?.$case === "versionDeltas" &&
      object.payload?.versionDeltas !== undefined &&
      object.payload?.versionDeltas !== null
    ) {
      message.payload = {
        $case: "versionDeltas",
        versionDeltas: PinVersionResponse_HummockVersionDeltas.fromPartial(object.payload.versionDeltas),
      };
    }
    if (
      object.payload?.$case === "pinnedVersion" &&
      object.payload?.pinnedVersion !== undefined &&
      object.payload?.pinnedVersion !== null
    ) {
      message.payload = {
        $case: "pinnedVersion",
        pinnedVersion: HummockVersion.fromPartial(object.payload.pinnedVersion),
      };
    }
    return message;
  },
};

function createBasePinVersionResponse_HummockVersionDeltas(): PinVersionResponse_HummockVersionDeltas {
  return { delta: [] };
}

export const PinVersionResponse_HummockVersionDeltas = {
  encode(message: PinVersionResponse_HummockVersionDeltas, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.delta) {
      HummockVersionDelta.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PinVersionResponse_HummockVersionDeltas {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePinVersionResponse_HummockVersionDeltas();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.delta.push(HummockVersionDelta.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PinVersionResponse_HummockVersionDeltas {
    return { delta: Array.isArray(object?.delta) ? object.delta.map((e: any) => HummockVersionDelta.fromJSON(e)) : [] };
  },

  toJSON(message: PinVersionResponse_HummockVersionDeltas): unknown {
    const obj: any = {};
    if (message.delta) {
      obj.delta = message.delta.map((e) => e ? HummockVersionDelta.toJSON(e) : undefined);
    } else {
      obj.delta = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinVersionResponse_HummockVersionDeltas>, I>>(
    object: I,
  ): PinVersionResponse_HummockVersionDeltas {
    const message = createBasePinVersionResponse_HummockVersionDeltas();
    message.delta = object.delta?.map((e) => HummockVersionDelta.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUnpinVersionBeforeRequest(): UnpinVersionBeforeRequest {
  return { contextId: 0, unpinVersionBefore: 0 };
}

export const UnpinVersionBeforeRequest = {
  encode(message: UnpinVersionBeforeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.unpinVersionBefore !== 0) {
      writer.uint32(16).uint64(message.unpinVersionBefore);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinVersionBeforeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinVersionBeforeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.unpinVersionBefore = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UnpinVersionBeforeRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      unpinVersionBefore: isSet(object.unpinVersionBefore) ? Number(object.unpinVersionBefore) : 0,
    };
  },

  toJSON(message: UnpinVersionBeforeRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.unpinVersionBefore !== undefined && (obj.unpinVersionBefore = Math.round(message.unpinVersionBefore));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinVersionBeforeRequest>, I>>(object: I): UnpinVersionBeforeRequest {
    const message = createBaseUnpinVersionBeforeRequest();
    message.contextId = object.contextId ?? 0;
    message.unpinVersionBefore = object.unpinVersionBefore ?? 0;
    return message;
  },
};

function createBaseUnpinVersionBeforeResponse(): UnpinVersionBeforeResponse {
  return { status: undefined };
}

export const UnpinVersionBeforeResponse = {
  encode(message: UnpinVersionBeforeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinVersionBeforeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinVersionBeforeResponse();
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

  fromJSON(object: any): UnpinVersionBeforeResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: UnpinVersionBeforeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinVersionBeforeResponse>, I>>(object: I): UnpinVersionBeforeResponse {
    const message = createBaseUnpinVersionBeforeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseUnpinVersionRequest(): UnpinVersionRequest {
  return { contextId: 0 };
}

export const UnpinVersionRequest = {
  encode(message: UnpinVersionRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinVersionRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinVersionRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UnpinVersionRequest {
    return { contextId: isSet(object.contextId) ? Number(object.contextId) : 0 };
  },

  toJSON(message: UnpinVersionRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinVersionRequest>, I>>(object: I): UnpinVersionRequest {
    const message = createBaseUnpinVersionRequest();
    message.contextId = object.contextId ?? 0;
    return message;
  },
};

function createBaseUnpinVersionResponse(): UnpinVersionResponse {
  return { status: undefined };
}

export const UnpinVersionResponse = {
  encode(message: UnpinVersionResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinVersionResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinVersionResponse();
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

  fromJSON(object: any): UnpinVersionResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: UnpinVersionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinVersionResponse>, I>>(object: I): UnpinVersionResponse {
    const message = createBaseUnpinVersionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBasePinSnapshotRequest(): PinSnapshotRequest {
  return { contextId: 0 };
}

export const PinSnapshotRequest = {
  encode(message: PinSnapshotRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PinSnapshotRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePinSnapshotRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PinSnapshotRequest {
    return { contextId: isSet(object.contextId) ? Number(object.contextId) : 0 };
  },

  toJSON(message: PinSnapshotRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinSnapshotRequest>, I>>(object: I): PinSnapshotRequest {
    const message = createBasePinSnapshotRequest();
    message.contextId = object.contextId ?? 0;
    return message;
  },
};

function createBasePinSnapshotResponse(): PinSnapshotResponse {
  return { status: undefined, snapshot: undefined };
}

export const PinSnapshotResponse = {
  encode(message: PinSnapshotResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.snapshot !== undefined) {
      HummockSnapshot.encode(message.snapshot, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PinSnapshotResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePinSnapshotResponse();
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

  fromJSON(object: any): PinSnapshotResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      snapshot: isSet(object.snapshot) ? HummockSnapshot.fromJSON(object.snapshot) : undefined,
    };
  },

  toJSON(message: PinSnapshotResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.snapshot !== undefined &&
      (obj.snapshot = message.snapshot ? HummockSnapshot.toJSON(message.snapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinSnapshotResponse>, I>>(object: I): PinSnapshotResponse {
    const message = createBasePinSnapshotResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.snapshot = (object.snapshot !== undefined && object.snapshot !== null)
      ? HummockSnapshot.fromPartial(object.snapshot)
      : undefined;
    return message;
  },
};

function createBaseGetEpochRequest(): GetEpochRequest {
  return {};
}

export const GetEpochRequest = {
  encode(_: GetEpochRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetEpochRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetEpochRequest();
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

  fromJSON(_: any): GetEpochRequest {
    return {};
  },

  toJSON(_: GetEpochRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetEpochRequest>, I>>(_: I): GetEpochRequest {
    const message = createBaseGetEpochRequest();
    return message;
  },
};

function createBaseGetEpochResponse(): GetEpochResponse {
  return { status: undefined, snapshot: undefined };
}

export const GetEpochResponse = {
  encode(message: GetEpochResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.snapshot !== undefined) {
      HummockSnapshot.encode(message.snapshot, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetEpochResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetEpochResponse();
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

  fromJSON(object: any): GetEpochResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      snapshot: isSet(object.snapshot) ? HummockSnapshot.fromJSON(object.snapshot) : undefined,
    };
  },

  toJSON(message: GetEpochResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.snapshot !== undefined &&
      (obj.snapshot = message.snapshot ? HummockSnapshot.toJSON(message.snapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetEpochResponse>, I>>(object: I): GetEpochResponse {
    const message = createBaseGetEpochResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.snapshot = (object.snapshot !== undefined && object.snapshot !== null)
      ? HummockSnapshot.fromPartial(object.snapshot)
      : undefined;
    return message;
  },
};

function createBaseUnpinSnapshotRequest(): UnpinSnapshotRequest {
  return { contextId: 0 };
}

export const UnpinSnapshotRequest = {
  encode(message: UnpinSnapshotRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinSnapshotRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinSnapshotRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UnpinSnapshotRequest {
    return { contextId: isSet(object.contextId) ? Number(object.contextId) : 0 };
  },

  toJSON(message: UnpinSnapshotRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinSnapshotRequest>, I>>(object: I): UnpinSnapshotRequest {
    const message = createBaseUnpinSnapshotRequest();
    message.contextId = object.contextId ?? 0;
    return message;
  },
};

function createBaseUnpinSnapshotResponse(): UnpinSnapshotResponse {
  return { status: undefined };
}

export const UnpinSnapshotResponse = {
  encode(message: UnpinSnapshotResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinSnapshotResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinSnapshotResponse();
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

  fromJSON(object: any): UnpinSnapshotResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: UnpinSnapshotResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinSnapshotResponse>, I>>(object: I): UnpinSnapshotResponse {
    const message = createBaseUnpinSnapshotResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseUnpinSnapshotBeforeRequest(): UnpinSnapshotBeforeRequest {
  return { contextId: 0, minSnapshot: undefined };
}

export const UnpinSnapshotBeforeRequest = {
  encode(message: UnpinSnapshotBeforeRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.minSnapshot !== undefined) {
      HummockSnapshot.encode(message.minSnapshot, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinSnapshotBeforeRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinSnapshotBeforeRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 3:
          message.minSnapshot = HummockSnapshot.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UnpinSnapshotBeforeRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      minSnapshot: isSet(object.minSnapshot) ? HummockSnapshot.fromJSON(object.minSnapshot) : undefined,
    };
  },

  toJSON(message: UnpinSnapshotBeforeRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.minSnapshot !== undefined &&
      (obj.minSnapshot = message.minSnapshot ? HummockSnapshot.toJSON(message.minSnapshot) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinSnapshotBeforeRequest>, I>>(object: I): UnpinSnapshotBeforeRequest {
    const message = createBaseUnpinSnapshotBeforeRequest();
    message.contextId = object.contextId ?? 0;
    message.minSnapshot = (object.minSnapshot !== undefined && object.minSnapshot !== null)
      ? HummockSnapshot.fromPartial(object.minSnapshot)
      : undefined;
    return message;
  },
};

function createBaseUnpinSnapshotBeforeResponse(): UnpinSnapshotBeforeResponse {
  return { status: undefined };
}

export const UnpinSnapshotBeforeResponse = {
  encode(message: UnpinSnapshotBeforeResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnpinSnapshotBeforeResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnpinSnapshotBeforeResponse();
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

  fromJSON(object: any): UnpinSnapshotBeforeResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: UnpinSnapshotBeforeResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnpinSnapshotBeforeResponse>, I>>(object: I): UnpinSnapshotBeforeResponse {
    const message = createBaseUnpinSnapshotBeforeResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseKeyRange(): KeyRange {
  return { left: new Uint8Array(), right: new Uint8Array(), inf: false };
}

export const KeyRange = {
  encode(message: KeyRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.left.length !== 0) {
      writer.uint32(10).bytes(message.left);
    }
    if (message.right.length !== 0) {
      writer.uint32(18).bytes(message.right);
    }
    if (message.inf === true) {
      writer.uint32(24).bool(message.inf);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): KeyRange {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseKeyRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.left = reader.bytes();
          break;
        case 2:
          message.right = reader.bytes();
          break;
        case 3:
          message.inf = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): KeyRange {
    return {
      left: isSet(object.left) ? bytesFromBase64(object.left) : new Uint8Array(),
      right: isSet(object.right) ? bytesFromBase64(object.right) : new Uint8Array(),
      inf: isSet(object.inf) ? Boolean(object.inf) : false,
    };
  },

  toJSON(message: KeyRange): unknown {
    const obj: any = {};
    message.left !== undefined &&
      (obj.left = base64FromBytes(message.left !== undefined ? message.left : new Uint8Array()));
    message.right !== undefined &&
      (obj.right = base64FromBytes(message.right !== undefined ? message.right : new Uint8Array()));
    message.inf !== undefined && (obj.inf = message.inf);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<KeyRange>, I>>(object: I): KeyRange {
    const message = createBaseKeyRange();
    message.left = object.left ?? new Uint8Array();
    message.right = object.right ?? new Uint8Array();
    message.inf = object.inf ?? false;
    return message;
  },
};

function createBaseTableOption(): TableOption {
  return { retentionSeconds: 0 };
}

export const TableOption = {
  encode(message: TableOption, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.retentionSeconds !== 0) {
      writer.uint32(8).uint32(message.retentionSeconds);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableOption {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableOption();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.retentionSeconds = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableOption {
    return { retentionSeconds: isSet(object.retentionSeconds) ? Number(object.retentionSeconds) : 0 };
  },

  toJSON(message: TableOption): unknown {
    const obj: any = {};
    message.retentionSeconds !== undefined && (obj.retentionSeconds = Math.round(message.retentionSeconds));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableOption>, I>>(object: I): TableOption {
    const message = createBaseTableOption();
    message.retentionSeconds = object.retentionSeconds ?? 0;
    return message;
  },
};

function createBaseCompactTask(): CompactTask {
  return {
    inputSsts: [],
    splits: [],
    watermark: 0,
    sortedOutputSsts: [],
    taskId: 0,
    targetLevel: 0,
    gcDeleteKeys: false,
    taskStatus: 0,
    compactionGroupId: 0,
    existingTableIds: [],
    compressionAlgorithm: 0,
    targetFileSize: 0,
    compactionFilterMask: 0,
    tableOptions: {},
    currentEpochTime: 0,
    targetSubLevelId: 0,
  };
}

export const CompactTask = {
  encode(message: CompactTask, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.inputSsts) {
      InputLevel.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.splits) {
      KeyRange.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.watermark !== 0) {
      writer.uint32(24).uint64(message.watermark);
    }
    for (const v of message.sortedOutputSsts) {
      SstableInfo.encode(v!, writer.uint32(34).fork()).ldelim();
    }
    if (message.taskId !== 0) {
      writer.uint32(40).uint64(message.taskId);
    }
    if (message.targetLevel !== 0) {
      writer.uint32(48).uint32(message.targetLevel);
    }
    if (message.gcDeleteKeys === true) {
      writer.uint32(56).bool(message.gcDeleteKeys);
    }
    if (message.taskStatus !== 0) {
      writer.uint32(72).int32(message.taskStatus);
    }
    if (message.compactionGroupId !== 0) {
      writer.uint32(96).uint64(message.compactionGroupId);
    }
    writer.uint32(106).fork();
    for (const v of message.existingTableIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    if (message.compressionAlgorithm !== 0) {
      writer.uint32(112).uint32(message.compressionAlgorithm);
    }
    if (message.targetFileSize !== 0) {
      writer.uint32(120).uint64(message.targetFileSize);
    }
    if (message.compactionFilterMask !== 0) {
      writer.uint32(128).uint32(message.compactionFilterMask);
    }
    Object.entries(message.tableOptions).forEach(([key, value]) => {
      CompactTask_TableOptionsEntry.encode({ key: key as any, value }, writer.uint32(138).fork()).ldelim();
    });
    if (message.currentEpochTime !== 0) {
      writer.uint32(144).uint64(message.currentEpochTime);
    }
    if (message.targetSubLevelId !== 0) {
      writer.uint32(152).uint64(message.targetSubLevelId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactTask {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactTask();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.inputSsts.push(InputLevel.decode(reader, reader.uint32()));
          break;
        case 2:
          message.splits.push(KeyRange.decode(reader, reader.uint32()));
          break;
        case 3:
          message.watermark = longToNumber(reader.uint64() as Long);
          break;
        case 4:
          message.sortedOutputSsts.push(SstableInfo.decode(reader, reader.uint32()));
          break;
        case 5:
          message.taskId = longToNumber(reader.uint64() as Long);
          break;
        case 6:
          message.targetLevel = reader.uint32();
          break;
        case 7:
          message.gcDeleteKeys = reader.bool();
          break;
        case 9:
          message.taskStatus = reader.int32() as any;
          break;
        case 12:
          message.compactionGroupId = longToNumber(reader.uint64() as Long);
          break;
        case 13:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.existingTableIds.push(reader.uint32());
            }
          } else {
            message.existingTableIds.push(reader.uint32());
          }
          break;
        case 14:
          message.compressionAlgorithm = reader.uint32();
          break;
        case 15:
          message.targetFileSize = longToNumber(reader.uint64() as Long);
          break;
        case 16:
          message.compactionFilterMask = reader.uint32();
          break;
        case 17:
          const entry17 = CompactTask_TableOptionsEntry.decode(reader, reader.uint32());
          if (entry17.value !== undefined) {
            message.tableOptions[entry17.key] = entry17.value;
          }
          break;
        case 18:
          message.currentEpochTime = longToNumber(reader.uint64() as Long);
          break;
        case 19:
          message.targetSubLevelId = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactTask {
    return {
      inputSsts: Array.isArray(object?.inputSsts) ? object.inputSsts.map((e: any) => InputLevel.fromJSON(e)) : [],
      splits: Array.isArray(object?.splits) ? object.splits.map((e: any) => KeyRange.fromJSON(e)) : [],
      watermark: isSet(object.watermark) ? Number(object.watermark) : 0,
      sortedOutputSsts: Array.isArray(object?.sortedOutputSsts)
        ? object.sortedOutputSsts.map((e: any) => SstableInfo.fromJSON(e))
        : [],
      taskId: isSet(object.taskId) ? Number(object.taskId) : 0,
      targetLevel: isSet(object.targetLevel) ? Number(object.targetLevel) : 0,
      gcDeleteKeys: isSet(object.gcDeleteKeys) ? Boolean(object.gcDeleteKeys) : false,
      taskStatus: isSet(object.taskStatus) ? compactTask_TaskStatusFromJSON(object.taskStatus) : 0,
      compactionGroupId: isSet(object.compactionGroupId) ? Number(object.compactionGroupId) : 0,
      existingTableIds: Array.isArray(object?.existingTableIds)
        ? object.existingTableIds.map((e: any) => Number(e))
        : [],
      compressionAlgorithm: isSet(object.compressionAlgorithm) ? Number(object.compressionAlgorithm) : 0,
      targetFileSize: isSet(object.targetFileSize) ? Number(object.targetFileSize) : 0,
      compactionFilterMask: isSet(object.compactionFilterMask) ? Number(object.compactionFilterMask) : 0,
      tableOptions: isObject(object.tableOptions)
        ? Object.entries(object.tableOptions).reduce<{ [key: number]: TableOption }>((acc, [key, value]) => {
          acc[Number(key)] = TableOption.fromJSON(value);
          return acc;
        }, {})
        : {},
      currentEpochTime: isSet(object.currentEpochTime) ? Number(object.currentEpochTime) : 0,
      targetSubLevelId: isSet(object.targetSubLevelId) ? Number(object.targetSubLevelId) : 0,
    };
  },

  toJSON(message: CompactTask): unknown {
    const obj: any = {};
    if (message.inputSsts) {
      obj.inputSsts = message.inputSsts.map((e) => e ? InputLevel.toJSON(e) : undefined);
    } else {
      obj.inputSsts = [];
    }
    if (message.splits) {
      obj.splits = message.splits.map((e) => e ? KeyRange.toJSON(e) : undefined);
    } else {
      obj.splits = [];
    }
    message.watermark !== undefined && (obj.watermark = Math.round(message.watermark));
    if (message.sortedOutputSsts) {
      obj.sortedOutputSsts = message.sortedOutputSsts.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.sortedOutputSsts = [];
    }
    message.taskId !== undefined && (obj.taskId = Math.round(message.taskId));
    message.targetLevel !== undefined && (obj.targetLevel = Math.round(message.targetLevel));
    message.gcDeleteKeys !== undefined && (obj.gcDeleteKeys = message.gcDeleteKeys);
    message.taskStatus !== undefined && (obj.taskStatus = compactTask_TaskStatusToJSON(message.taskStatus));
    message.compactionGroupId !== undefined && (obj.compactionGroupId = Math.round(message.compactionGroupId));
    if (message.existingTableIds) {
      obj.existingTableIds = message.existingTableIds.map((e) => Math.round(e));
    } else {
      obj.existingTableIds = [];
    }
    message.compressionAlgorithm !== undefined && (obj.compressionAlgorithm = Math.round(message.compressionAlgorithm));
    message.targetFileSize !== undefined && (obj.targetFileSize = Math.round(message.targetFileSize));
    message.compactionFilterMask !== undefined && (obj.compactionFilterMask = Math.round(message.compactionFilterMask));
    obj.tableOptions = {};
    if (message.tableOptions) {
      Object.entries(message.tableOptions).forEach(([k, v]) => {
        obj.tableOptions[k] = TableOption.toJSON(v);
      });
    }
    message.currentEpochTime !== undefined && (obj.currentEpochTime = Math.round(message.currentEpochTime));
    message.targetSubLevelId !== undefined && (obj.targetSubLevelId = Math.round(message.targetSubLevelId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactTask>, I>>(object: I): CompactTask {
    const message = createBaseCompactTask();
    message.inputSsts = object.inputSsts?.map((e) => InputLevel.fromPartial(e)) || [];
    message.splits = object.splits?.map((e) => KeyRange.fromPartial(e)) || [];
    message.watermark = object.watermark ?? 0;
    message.sortedOutputSsts = object.sortedOutputSsts?.map((e) => SstableInfo.fromPartial(e)) || [];
    message.taskId = object.taskId ?? 0;
    message.targetLevel = object.targetLevel ?? 0;
    message.gcDeleteKeys = object.gcDeleteKeys ?? false;
    message.taskStatus = object.taskStatus ?? 0;
    message.compactionGroupId = object.compactionGroupId ?? 0;
    message.existingTableIds = object.existingTableIds?.map((e) => e) || [];
    message.compressionAlgorithm = object.compressionAlgorithm ?? 0;
    message.targetFileSize = object.targetFileSize ?? 0;
    message.compactionFilterMask = object.compactionFilterMask ?? 0;
    message.tableOptions = Object.entries(object.tableOptions ?? {}).reduce<{ [key: number]: TableOption }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = TableOption.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.currentEpochTime = object.currentEpochTime ?? 0;
    message.targetSubLevelId = object.targetSubLevelId ?? 0;
    return message;
  },
};

function createBaseCompactTask_TableOptionsEntry(): CompactTask_TableOptionsEntry {
  return { key: 0, value: undefined };
}

export const CompactTask_TableOptionsEntry = {
  encode(message: CompactTask_TableOptionsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      TableOption.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactTask_TableOptionsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactTask_TableOptionsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = TableOption.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactTask_TableOptionsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableOption.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: CompactTask_TableOptionsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? TableOption.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactTask_TableOptionsEntry>, I>>(
    object: I,
  ): CompactTask_TableOptionsEntry {
    const message = createBaseCompactTask_TableOptionsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableOption.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseLevelHandler(): LevelHandler {
  return { level: 0, tasks: [] };
}

export const LevelHandler = {
  encode(message: LevelHandler, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.level !== 0) {
      writer.uint32(8).uint32(message.level);
    }
    for (const v of message.tasks) {
      LevelHandler_RunningCompactTask.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LevelHandler {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLevelHandler();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.level = reader.uint32();
          break;
        case 3:
          message.tasks.push(LevelHandler_RunningCompactTask.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LevelHandler {
    return {
      level: isSet(object.level) ? Number(object.level) : 0,
      tasks: Array.isArray(object?.tasks)
        ? object.tasks.map((e: any) => LevelHandler_RunningCompactTask.fromJSON(e))
        : [],
    };
  },

  toJSON(message: LevelHandler): unknown {
    const obj: any = {};
    message.level !== undefined && (obj.level = Math.round(message.level));
    if (message.tasks) {
      obj.tasks = message.tasks.map((e) => e ? LevelHandler_RunningCompactTask.toJSON(e) : undefined);
    } else {
      obj.tasks = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LevelHandler>, I>>(object: I): LevelHandler {
    const message = createBaseLevelHandler();
    message.level = object.level ?? 0;
    message.tasks = object.tasks?.map((e) => LevelHandler_RunningCompactTask.fromPartial(e)) || [];
    return message;
  },
};

function createBaseLevelHandler_RunningCompactTask(): LevelHandler_RunningCompactTask {
  return { taskId: 0, ssts: [], totalFileSize: 0, targetLevel: 0 };
}

export const LevelHandler_RunningCompactTask = {
  encode(message: LevelHandler_RunningCompactTask, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== 0) {
      writer.uint32(8).uint64(message.taskId);
    }
    writer.uint32(18).fork();
    for (const v of message.ssts) {
      writer.uint64(v);
    }
    writer.ldelim();
    if (message.totalFileSize !== 0) {
      writer.uint32(24).uint64(message.totalFileSize);
    }
    if (message.targetLevel !== 0) {
      writer.uint32(32).uint32(message.targetLevel);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LevelHandler_RunningCompactTask {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLevelHandler_RunningCompactTask();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.ssts.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.ssts.push(longToNumber(reader.uint64() as Long));
          }
          break;
        case 3:
          message.totalFileSize = longToNumber(reader.uint64() as Long);
          break;
        case 4:
          message.targetLevel = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LevelHandler_RunningCompactTask {
    return {
      taskId: isSet(object.taskId) ? Number(object.taskId) : 0,
      ssts: Array.isArray(object?.ssts) ? object.ssts.map((e: any) => Number(e)) : [],
      totalFileSize: isSet(object.totalFileSize) ? Number(object.totalFileSize) : 0,
      targetLevel: isSet(object.targetLevel) ? Number(object.targetLevel) : 0,
    };
  },

  toJSON(message: LevelHandler_RunningCompactTask): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = Math.round(message.taskId));
    if (message.ssts) {
      obj.ssts = message.ssts.map((e) => Math.round(e));
    } else {
      obj.ssts = [];
    }
    message.totalFileSize !== undefined && (obj.totalFileSize = Math.round(message.totalFileSize));
    message.targetLevel !== undefined && (obj.targetLevel = Math.round(message.targetLevel));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LevelHandler_RunningCompactTask>, I>>(
    object: I,
  ): LevelHandler_RunningCompactTask {
    const message = createBaseLevelHandler_RunningCompactTask();
    message.taskId = object.taskId ?? 0;
    message.ssts = object.ssts?.map((e) => e) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    message.targetLevel = object.targetLevel ?? 0;
    return message;
  },
};

function createBaseCompactStatus(): CompactStatus {
  return { compactionGroupId: 0, levelHandlers: [] };
}

export const CompactStatus = {
  encode(message: CompactStatus, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.compactionGroupId !== 0) {
      writer.uint32(8).uint64(message.compactionGroupId);
    }
    for (const v of message.levelHandlers) {
      LevelHandler.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactStatus {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactStatus();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.compactionGroupId = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.levelHandlers.push(LevelHandler.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactStatus {
    return {
      compactionGroupId: isSet(object.compactionGroupId) ? Number(object.compactionGroupId) : 0,
      levelHandlers: Array.isArray(object?.levelHandlers)
        ? object.levelHandlers.map((e: any) => LevelHandler.fromJSON(e))
        : [],
    };
  },

  toJSON(message: CompactStatus): unknown {
    const obj: any = {};
    message.compactionGroupId !== undefined && (obj.compactionGroupId = Math.round(message.compactionGroupId));
    if (message.levelHandlers) {
      obj.levelHandlers = message.levelHandlers.map((e) => e ? LevelHandler.toJSON(e) : undefined);
    } else {
      obj.levelHandlers = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactStatus>, I>>(object: I): CompactStatus {
    const message = createBaseCompactStatus();
    message.compactionGroupId = object.compactionGroupId ?? 0;
    message.levelHandlers = object.levelHandlers?.map((e) => LevelHandler.fromPartial(e)) || [];
    return message;
  },
};

function createBaseCompactionGroup(): CompactionGroup {
  return { id: 0, memberTableIds: [], compactionConfig: undefined, tableIdToOptions: {} };
}

export const CompactionGroup = {
  encode(message: CompactionGroup, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.id !== 0) {
      writer.uint32(8).uint64(message.id);
    }
    writer.uint32(18).fork();
    for (const v of message.memberTableIds) {
      writer.uint32(v);
    }
    writer.ldelim();
    if (message.compactionConfig !== undefined) {
      CompactionConfig.encode(message.compactionConfig, writer.uint32(26).fork()).ldelim();
    }
    Object.entries(message.tableIdToOptions).forEach(([key, value]) => {
      CompactionGroup_TableIdToOptionsEntry.encode({ key: key as any, value }, writer.uint32(34).fork()).ldelim();
    });
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactionGroup {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactionGroup();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.id = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.memberTableIds.push(reader.uint32());
            }
          } else {
            message.memberTableIds.push(reader.uint32());
          }
          break;
        case 3:
          message.compactionConfig = CompactionConfig.decode(reader, reader.uint32());
          break;
        case 4:
          const entry4 = CompactionGroup_TableIdToOptionsEntry.decode(reader, reader.uint32());
          if (entry4.value !== undefined) {
            message.tableIdToOptions[entry4.key] = entry4.value;
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactionGroup {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      memberTableIds: Array.isArray(object?.memberTableIds) ? object.memberTableIds.map((e: any) => Number(e)) : [],
      compactionConfig: isSet(object.compactionConfig) ? CompactionConfig.fromJSON(object.compactionConfig) : undefined,
      tableIdToOptions: isObject(object.tableIdToOptions)
        ? Object.entries(object.tableIdToOptions).reduce<{ [key: number]: TableOption }>((acc, [key, value]) => {
          acc[Number(key)] = TableOption.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: CompactionGroup): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    if (message.memberTableIds) {
      obj.memberTableIds = message.memberTableIds.map((e) => Math.round(e));
    } else {
      obj.memberTableIds = [];
    }
    message.compactionConfig !== undefined &&
      (obj.compactionConfig = message.compactionConfig ? CompactionConfig.toJSON(message.compactionConfig) : undefined);
    obj.tableIdToOptions = {};
    if (message.tableIdToOptions) {
      Object.entries(message.tableIdToOptions).forEach(([k, v]) => {
        obj.tableIdToOptions[k] = TableOption.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionGroup>, I>>(object: I): CompactionGroup {
    const message = createBaseCompactionGroup();
    message.id = object.id ?? 0;
    message.memberTableIds = object.memberTableIds?.map((e) => e) || [];
    message.compactionConfig = (object.compactionConfig !== undefined && object.compactionConfig !== null)
      ? CompactionConfig.fromPartial(object.compactionConfig)
      : undefined;
    message.tableIdToOptions = Object.entries(object.tableIdToOptions ?? {}).reduce<{ [key: number]: TableOption }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = TableOption.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseCompactionGroup_TableIdToOptionsEntry(): CompactionGroup_TableIdToOptionsEntry {
  return { key: 0, value: undefined };
}

export const CompactionGroup_TableIdToOptionsEntry = {
  encode(message: CompactionGroup_TableIdToOptionsEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint32(message.key);
    }
    if (message.value !== undefined) {
      TableOption.encode(message.value, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactionGroup_TableIdToOptionsEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactionGroup_TableIdToOptionsEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = reader.uint32();
          break;
        case 2:
          message.value = TableOption.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactionGroup_TableIdToOptionsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableOption.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: CompactionGroup_TableIdToOptionsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? TableOption.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionGroup_TableIdToOptionsEntry>, I>>(
    object: I,
  ): CompactionGroup_TableIdToOptionsEntry {
    const message = createBaseCompactionGroup_TableIdToOptionsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableOption.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseCompactTaskAssignment(): CompactTaskAssignment {
  return { compactTask: undefined, contextId: 0 };
}

export const CompactTaskAssignment = {
  encode(message: CompactTaskAssignment, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.compactTask !== undefined) {
      CompactTask.encode(message.compactTask, writer.uint32(10).fork()).ldelim();
    }
    if (message.contextId !== 0) {
      writer.uint32(16).uint32(message.contextId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactTaskAssignment {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactTaskAssignment();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.compactTask = CompactTask.decode(reader, reader.uint32());
          break;
        case 2:
          message.contextId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactTaskAssignment {
    return {
      compactTask: isSet(object.compactTask) ? CompactTask.fromJSON(object.compactTask) : undefined,
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
    };
  },

  toJSON(message: CompactTaskAssignment): unknown {
    const obj: any = {};
    message.compactTask !== undefined &&
      (obj.compactTask = message.compactTask ? CompactTask.toJSON(message.compactTask) : undefined);
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactTaskAssignment>, I>>(object: I): CompactTaskAssignment {
    const message = createBaseCompactTaskAssignment();
    message.compactTask = (object.compactTask !== undefined && object.compactTask !== null)
      ? CompactTask.fromPartial(object.compactTask)
      : undefined;
    message.contextId = object.contextId ?? 0;
    return message;
  },
};

function createBaseGetCompactionTasksRequest(): GetCompactionTasksRequest {
  return {};
}

export const GetCompactionTasksRequest = {
  encode(_: GetCompactionTasksRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetCompactionTasksRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetCompactionTasksRequest();
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

  fromJSON(_: any): GetCompactionTasksRequest {
    return {};
  },

  toJSON(_: GetCompactionTasksRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCompactionTasksRequest>, I>>(_: I): GetCompactionTasksRequest {
    const message = createBaseGetCompactionTasksRequest();
    return message;
  },
};

function createBaseGetCompactionTasksResponse(): GetCompactionTasksResponse {
  return { status: undefined, compactTask: undefined };
}

export const GetCompactionTasksResponse = {
  encode(message: GetCompactionTasksResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.compactTask !== undefined) {
      CompactTask.encode(message.compactTask, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetCompactionTasksResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetCompactionTasksResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.compactTask = CompactTask.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetCompactionTasksResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      compactTask: isSet(object.compactTask) ? CompactTask.fromJSON(object.compactTask) : undefined,
    };
  },

  toJSON(message: GetCompactionTasksResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.compactTask !== undefined &&
      (obj.compactTask = message.compactTask ? CompactTask.toJSON(message.compactTask) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCompactionTasksResponse>, I>>(object: I): GetCompactionTasksResponse {
    const message = createBaseGetCompactionTasksResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.compactTask = (object.compactTask !== undefined && object.compactTask !== null)
      ? CompactTask.fromPartial(object.compactTask)
      : undefined;
    return message;
  },
};

function createBaseReportCompactionTasksRequest(): ReportCompactionTasksRequest {
  return { contextId: 0, compactTask: undefined };
}

export const ReportCompactionTasksRequest = {
  encode(message: ReportCompactionTasksRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.compactTask !== undefined) {
      CompactTask.encode(message.compactTask, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportCompactionTasksRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportCompactionTasksRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.compactTask = CompactTask.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ReportCompactionTasksRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      compactTask: isSet(object.compactTask) ? CompactTask.fromJSON(object.compactTask) : undefined,
    };
  },

  toJSON(message: ReportCompactionTasksRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.compactTask !== undefined &&
      (obj.compactTask = message.compactTask ? CompactTask.toJSON(message.compactTask) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTasksRequest>, I>>(object: I): ReportCompactionTasksRequest {
    const message = createBaseReportCompactionTasksRequest();
    message.contextId = object.contextId ?? 0;
    message.compactTask = (object.compactTask !== undefined && object.compactTask !== null)
      ? CompactTask.fromPartial(object.compactTask)
      : undefined;
    return message;
  },
};

function createBaseReportCompactionTasksResponse(): ReportCompactionTasksResponse {
  return { status: undefined };
}

export const ReportCompactionTasksResponse = {
  encode(message: ReportCompactionTasksResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportCompactionTasksResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportCompactionTasksResponse();
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

  fromJSON(object: any): ReportCompactionTasksResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: ReportCompactionTasksResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTasksResponse>, I>>(
    object: I,
  ): ReportCompactionTasksResponse {
    const message = createBaseReportCompactionTasksResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseHummockPinnedVersion(): HummockPinnedVersion {
  return { contextId: 0, minPinnedId: 0 };
}

export const HummockPinnedVersion = {
  encode(message: HummockPinnedVersion, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.minPinnedId !== 0) {
      writer.uint32(16).uint64(message.minPinnedId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockPinnedVersion {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockPinnedVersion();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.minPinnedId = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockPinnedVersion {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      minPinnedId: isSet(object.minPinnedId) ? Number(object.minPinnedId) : 0,
    };
  },

  toJSON(message: HummockPinnedVersion): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.minPinnedId !== undefined && (obj.minPinnedId = Math.round(message.minPinnedId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockPinnedVersion>, I>>(object: I): HummockPinnedVersion {
    const message = createBaseHummockPinnedVersion();
    message.contextId = object.contextId ?? 0;
    message.minPinnedId = object.minPinnedId ?? 0;
    return message;
  },
};

function createBaseHummockPinnedSnapshot(): HummockPinnedSnapshot {
  return { contextId: 0, minimalPinnedSnapshot: 0 };
}

export const HummockPinnedSnapshot = {
  encode(message: HummockPinnedSnapshot, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.minimalPinnedSnapshot !== 0) {
      writer.uint32(16).uint64(message.minimalPinnedSnapshot);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HummockPinnedSnapshot {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHummockPinnedSnapshot();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.minimalPinnedSnapshot = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HummockPinnedSnapshot {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      minimalPinnedSnapshot: isSet(object.minimalPinnedSnapshot) ? Number(object.minimalPinnedSnapshot) : 0,
    };
  },

  toJSON(message: HummockPinnedSnapshot): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.minimalPinnedSnapshot !== undefined &&
      (obj.minimalPinnedSnapshot = Math.round(message.minimalPinnedSnapshot));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockPinnedSnapshot>, I>>(object: I): HummockPinnedSnapshot {
    const message = createBaseHummockPinnedSnapshot();
    message.contextId = object.contextId ?? 0;
    message.minimalPinnedSnapshot = object.minimalPinnedSnapshot ?? 0;
    return message;
  },
};

function createBaseGetNewSstIdsRequest(): GetNewSstIdsRequest {
  return { number: 0 };
}

export const GetNewSstIdsRequest = {
  encode(message: GetNewSstIdsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.number !== 0) {
      writer.uint32(8).uint32(message.number);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetNewSstIdsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetNewSstIdsRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.number = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetNewSstIdsRequest {
    return { number: isSet(object.number) ? Number(object.number) : 0 };
  },

  toJSON(message: GetNewSstIdsRequest): unknown {
    const obj: any = {};
    message.number !== undefined && (obj.number = Math.round(message.number));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetNewSstIdsRequest>, I>>(object: I): GetNewSstIdsRequest {
    const message = createBaseGetNewSstIdsRequest();
    message.number = object.number ?? 0;
    return message;
  },
};

function createBaseGetNewSstIdsResponse(): GetNewSstIdsResponse {
  return { status: undefined, startId: 0, endId: 0 };
}

export const GetNewSstIdsResponse = {
  encode(message: GetNewSstIdsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    if (message.startId !== 0) {
      writer.uint32(16).uint64(message.startId);
    }
    if (message.endId !== 0) {
      writer.uint32(24).uint64(message.endId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetNewSstIdsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetNewSstIdsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.startId = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          message.endId = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetNewSstIdsResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      startId: isSet(object.startId) ? Number(object.startId) : 0,
      endId: isSet(object.endId) ? Number(object.endId) : 0,
    };
  },

  toJSON(message: GetNewSstIdsResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.startId !== undefined && (obj.startId = Math.round(message.startId));
    message.endId !== undefined && (obj.endId = Math.round(message.endId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetNewSstIdsResponse>, I>>(object: I): GetNewSstIdsResponse {
    const message = createBaseGetNewSstIdsResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.startId = object.startId ?? 0;
    message.endId = object.endId ?? 0;
    return message;
  },
};

function createBaseSubscribeCompactTasksRequest(): SubscribeCompactTasksRequest {
  return { contextId: 0, maxConcurrentTaskNumber: 0 };
}

export const SubscribeCompactTasksRequest = {
  encode(message: SubscribeCompactTasksRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.contextId !== 0) {
      writer.uint32(8).uint32(message.contextId);
    }
    if (message.maxConcurrentTaskNumber !== 0) {
      writer.uint32(16).uint64(message.maxConcurrentTaskNumber);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SubscribeCompactTasksRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSubscribeCompactTasksRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.contextId = reader.uint32();
          break;
        case 2:
          message.maxConcurrentTaskNumber = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SubscribeCompactTasksRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      maxConcurrentTaskNumber: isSet(object.maxConcurrentTaskNumber) ? Number(object.maxConcurrentTaskNumber) : 0,
    };
  },

  toJSON(message: SubscribeCompactTasksRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.maxConcurrentTaskNumber !== undefined &&
      (obj.maxConcurrentTaskNumber = Math.round(message.maxConcurrentTaskNumber));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeCompactTasksRequest>, I>>(object: I): SubscribeCompactTasksRequest {
    const message = createBaseSubscribeCompactTasksRequest();
    message.contextId = object.contextId ?? 0;
    message.maxConcurrentTaskNumber = object.maxConcurrentTaskNumber ?? 0;
    return message;
  },
};

function createBaseValidationTask(): ValidationTask {
  return { sstIds: [], sstIdToWorkerId: {}, epoch: 0 };
}

export const ValidationTask = {
  encode(message: ValidationTask, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.sstIds) {
      writer.uint64(v);
    }
    writer.ldelim();
    Object.entries(message.sstIdToWorkerId).forEach(([key, value]) => {
      ValidationTask_SstIdToWorkerIdEntry.encode({ key: key as any, value }, writer.uint32(18).fork()).ldelim();
    });
    if (message.epoch !== 0) {
      writer.uint32(24).uint64(message.epoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValidationTask {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValidationTask();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.sstIds.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.sstIds.push(longToNumber(reader.uint64() as Long));
          }
          break;
        case 2:
          const entry2 = ValidationTask_SstIdToWorkerIdEntry.decode(reader, reader.uint32());
          if (entry2.value !== undefined) {
            message.sstIdToWorkerId[entry2.key] = entry2.value;
          }
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

  fromJSON(object: any): ValidationTask {
    return {
      sstIds: Array.isArray(object?.sstIds) ? object.sstIds.map((e: any) => Number(e)) : [],
      sstIdToWorkerId: isObject(object.sstIdToWorkerId)
        ? Object.entries(object.sstIdToWorkerId).reduce<{ [key: number]: number }>((acc, [key, value]) => {
          acc[Number(key)] = Number(value);
          return acc;
        }, {})
        : {},
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: ValidationTask): unknown {
    const obj: any = {};
    if (message.sstIds) {
      obj.sstIds = message.sstIds.map((e) => Math.round(e));
    } else {
      obj.sstIds = [];
    }
    obj.sstIdToWorkerId = {};
    if (message.sstIdToWorkerId) {
      Object.entries(message.sstIdToWorkerId).forEach(([k, v]) => {
        obj.sstIdToWorkerId[k] = Math.round(v);
      });
    }
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ValidationTask>, I>>(object: I): ValidationTask {
    const message = createBaseValidationTask();
    message.sstIds = object.sstIds?.map((e) => e) || [];
    message.sstIdToWorkerId = Object.entries(object.sstIdToWorkerId ?? {}).reduce<{ [key: number]: number }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = Number(value);
        }
        return acc;
      },
      {},
    );
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseValidationTask_SstIdToWorkerIdEntry(): ValidationTask_SstIdToWorkerIdEntry {
  return { key: 0, value: 0 };
}

export const ValidationTask_SstIdToWorkerIdEntry = {
  encode(message: ValidationTask_SstIdToWorkerIdEntry, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.key !== 0) {
      writer.uint32(8).uint64(message.key);
    }
    if (message.value !== 0) {
      writer.uint32(16).uint32(message.value);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValidationTask_SstIdToWorkerIdEntry {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValidationTask_SstIdToWorkerIdEntry();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.key = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.value = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ValidationTask_SstIdToWorkerIdEntry {
    return { key: isSet(object.key) ? Number(object.key) : 0, value: isSet(object.value) ? Number(object.value) : 0 };
  },

  toJSON(message: ValidationTask_SstIdToWorkerIdEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = Math.round(message.value));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ValidationTask_SstIdToWorkerIdEntry>, I>>(
    object: I,
  ): ValidationTask_SstIdToWorkerIdEntry {
    const message = createBaseValidationTask_SstIdToWorkerIdEntry();
    message.key = object.key ?? 0;
    message.value = object.value ?? 0;
    return message;
  },
};

function createBaseSubscribeCompactTasksResponse(): SubscribeCompactTasksResponse {
  return { task: undefined };
}

export const SubscribeCompactTasksResponse = {
  encode(message: SubscribeCompactTasksResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.task?.$case === "compactTask") {
      CompactTask.encode(message.task.compactTask, writer.uint32(10).fork()).ldelim();
    }
    if (message.task?.$case === "vacuumTask") {
      VacuumTask.encode(message.task.vacuumTask, writer.uint32(18).fork()).ldelim();
    }
    if (message.task?.$case === "fullScanTask") {
      FullScanTask.encode(message.task.fullScanTask, writer.uint32(26).fork()).ldelim();
    }
    if (message.task?.$case === "validationTask") {
      ValidationTask.encode(message.task.validationTask, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SubscribeCompactTasksResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSubscribeCompactTasksResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.task = { $case: "compactTask", compactTask: CompactTask.decode(reader, reader.uint32()) };
          break;
        case 2:
          message.task = { $case: "vacuumTask", vacuumTask: VacuumTask.decode(reader, reader.uint32()) };
          break;
        case 3:
          message.task = { $case: "fullScanTask", fullScanTask: FullScanTask.decode(reader, reader.uint32()) };
          break;
        case 4:
          message.task = { $case: "validationTask", validationTask: ValidationTask.decode(reader, reader.uint32()) };
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SubscribeCompactTasksResponse {
    return {
      task: isSet(object.compactTask)
        ? { $case: "compactTask", compactTask: CompactTask.fromJSON(object.compactTask) }
        : isSet(object.vacuumTask)
        ? { $case: "vacuumTask", vacuumTask: VacuumTask.fromJSON(object.vacuumTask) }
        : isSet(object.fullScanTask)
        ? { $case: "fullScanTask", fullScanTask: FullScanTask.fromJSON(object.fullScanTask) }
        : isSet(object.validationTask)
        ? { $case: "validationTask", validationTask: ValidationTask.fromJSON(object.validationTask) }
        : undefined,
    };
  },

  toJSON(message: SubscribeCompactTasksResponse): unknown {
    const obj: any = {};
    message.task?.$case === "compactTask" &&
      (obj.compactTask = message.task?.compactTask ? CompactTask.toJSON(message.task?.compactTask) : undefined);
    message.task?.$case === "vacuumTask" &&
      (obj.vacuumTask = message.task?.vacuumTask ? VacuumTask.toJSON(message.task?.vacuumTask) : undefined);
    message.task?.$case === "fullScanTask" &&
      (obj.fullScanTask = message.task?.fullScanTask ? FullScanTask.toJSON(message.task?.fullScanTask) : undefined);
    message.task?.$case === "validationTask" && (obj.validationTask = message.task?.validationTask
      ? ValidationTask.toJSON(message.task?.validationTask)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SubscribeCompactTasksResponse>, I>>(
    object: I,
  ): SubscribeCompactTasksResponse {
    const message = createBaseSubscribeCompactTasksResponse();
    if (
      object.task?.$case === "compactTask" &&
      object.task?.compactTask !== undefined &&
      object.task?.compactTask !== null
    ) {
      message.task = { $case: "compactTask", compactTask: CompactTask.fromPartial(object.task.compactTask) };
    }
    if (
      object.task?.$case === "vacuumTask" && object.task?.vacuumTask !== undefined && object.task?.vacuumTask !== null
    ) {
      message.task = { $case: "vacuumTask", vacuumTask: VacuumTask.fromPartial(object.task.vacuumTask) };
    }
    if (
      object.task?.$case === "fullScanTask" &&
      object.task?.fullScanTask !== undefined &&
      object.task?.fullScanTask !== null
    ) {
      message.task = { $case: "fullScanTask", fullScanTask: FullScanTask.fromPartial(object.task.fullScanTask) };
    }
    if (
      object.task?.$case === "validationTask" &&
      object.task?.validationTask !== undefined &&
      object.task?.validationTask !== null
    ) {
      message.task = {
        $case: "validationTask",
        validationTask: ValidationTask.fromPartial(object.task.validationTask),
      };
    }
    return message;
  },
};

function createBaseVacuumTask(): VacuumTask {
  return { sstableIds: [] };
}

export const VacuumTask = {
  encode(message: VacuumTask, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.sstableIds) {
      writer.uint64(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): VacuumTask {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseVacuumTask();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.sstableIds.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.sstableIds.push(longToNumber(reader.uint64() as Long));
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): VacuumTask {
    return { sstableIds: Array.isArray(object?.sstableIds) ? object.sstableIds.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: VacuumTask): unknown {
    const obj: any = {};
    if (message.sstableIds) {
      obj.sstableIds = message.sstableIds.map((e) => Math.round(e));
    } else {
      obj.sstableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<VacuumTask>, I>>(object: I): VacuumTask {
    const message = createBaseVacuumTask();
    message.sstableIds = object.sstableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseFullScanTask(): FullScanTask {
  return { sstRetentionTimeSec: 0 };
}

export const FullScanTask = {
  encode(message: FullScanTask, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sstRetentionTimeSec !== 0) {
      writer.uint32(8).uint64(message.sstRetentionTimeSec);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FullScanTask {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFullScanTask();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sstRetentionTimeSec = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): FullScanTask {
    return { sstRetentionTimeSec: isSet(object.sstRetentionTimeSec) ? Number(object.sstRetentionTimeSec) : 0 };
  },

  toJSON(message: FullScanTask): unknown {
    const obj: any = {};
    message.sstRetentionTimeSec !== undefined && (obj.sstRetentionTimeSec = Math.round(message.sstRetentionTimeSec));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FullScanTask>, I>>(object: I): FullScanTask {
    const message = createBaseFullScanTask();
    message.sstRetentionTimeSec = object.sstRetentionTimeSec ?? 0;
    return message;
  },
};

function createBaseReportVacuumTaskRequest(): ReportVacuumTaskRequest {
  return { vacuumTask: undefined };
}

export const ReportVacuumTaskRequest = {
  encode(message: ReportVacuumTaskRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.vacuumTask !== undefined) {
      VacuumTask.encode(message.vacuumTask, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportVacuumTaskRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportVacuumTaskRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.vacuumTask = VacuumTask.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ReportVacuumTaskRequest {
    return { vacuumTask: isSet(object.vacuumTask) ? VacuumTask.fromJSON(object.vacuumTask) : undefined };
  },

  toJSON(message: ReportVacuumTaskRequest): unknown {
    const obj: any = {};
    message.vacuumTask !== undefined &&
      (obj.vacuumTask = message.vacuumTask ? VacuumTask.toJSON(message.vacuumTask) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportVacuumTaskRequest>, I>>(object: I): ReportVacuumTaskRequest {
    const message = createBaseReportVacuumTaskRequest();
    message.vacuumTask = (object.vacuumTask !== undefined && object.vacuumTask !== null)
      ? VacuumTask.fromPartial(object.vacuumTask)
      : undefined;
    return message;
  },
};

function createBaseReportVacuumTaskResponse(): ReportVacuumTaskResponse {
  return { status: undefined };
}

export const ReportVacuumTaskResponse = {
  encode(message: ReportVacuumTaskResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportVacuumTaskResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportVacuumTaskResponse();
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

  fromJSON(object: any): ReportVacuumTaskResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: ReportVacuumTaskResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportVacuumTaskResponse>, I>>(object: I): ReportVacuumTaskResponse {
    const message = createBaseReportVacuumTaskResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseGetCompactionGroupsRequest(): GetCompactionGroupsRequest {
  return {};
}

export const GetCompactionGroupsRequest = {
  encode(_: GetCompactionGroupsRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetCompactionGroupsRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetCompactionGroupsRequest();
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

  fromJSON(_: any): GetCompactionGroupsRequest {
    return {};
  },

  toJSON(_: GetCompactionGroupsRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCompactionGroupsRequest>, I>>(_: I): GetCompactionGroupsRequest {
    const message = createBaseGetCompactionGroupsRequest();
    return message;
  },
};

function createBaseGetCompactionGroupsResponse(): GetCompactionGroupsResponse {
  return { status: undefined, compactionGroups: [] };
}

export const GetCompactionGroupsResponse = {
  encode(message: GetCompactionGroupsResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.compactionGroups) {
      CompactionGroup.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): GetCompactionGroupsResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseGetCompactionGroupsResponse();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.status = Status.decode(reader, reader.uint32());
          break;
        case 2:
          message.compactionGroups.push(CompactionGroup.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): GetCompactionGroupsResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      compactionGroups: Array.isArray(object?.compactionGroups)
        ? object.compactionGroups.map((e: any) => CompactionGroup.fromJSON(e))
        : [],
    };
  },

  toJSON(message: GetCompactionGroupsResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    if (message.compactionGroups) {
      obj.compactionGroups = message.compactionGroups.map((e) => e ? CompactionGroup.toJSON(e) : undefined);
    } else {
      obj.compactionGroups = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCompactionGroupsResponse>, I>>(object: I): GetCompactionGroupsResponse {
    const message = createBaseGetCompactionGroupsResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.compactionGroups = object.compactionGroups?.map((e) => CompactionGroup.fromPartial(e)) || [];
    return message;
  },
};

function createBaseTriggerManualCompactionRequest(): TriggerManualCompactionRequest {
  return { compactionGroupId: 0, keyRange: undefined, tableId: 0, level: 0, sstIds: [] };
}

export const TriggerManualCompactionRequest = {
  encode(message: TriggerManualCompactionRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.compactionGroupId !== 0) {
      writer.uint32(8).uint64(message.compactionGroupId);
    }
    if (message.keyRange !== undefined) {
      KeyRange.encode(message.keyRange, writer.uint32(18).fork()).ldelim();
    }
    if (message.tableId !== 0) {
      writer.uint32(24).uint32(message.tableId);
    }
    if (message.level !== 0) {
      writer.uint32(32).uint32(message.level);
    }
    writer.uint32(42).fork();
    for (const v of message.sstIds) {
      writer.uint64(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TriggerManualCompactionRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTriggerManualCompactionRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.compactionGroupId = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.keyRange = KeyRange.decode(reader, reader.uint32());
          break;
        case 3:
          message.tableId = reader.uint32();
          break;
        case 4:
          message.level = reader.uint32();
          break;
        case 5:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.sstIds.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.sstIds.push(longToNumber(reader.uint64() as Long));
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TriggerManualCompactionRequest {
    return {
      compactionGroupId: isSet(object.compactionGroupId) ? Number(object.compactionGroupId) : 0,
      keyRange: isSet(object.keyRange) ? KeyRange.fromJSON(object.keyRange) : undefined,
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      level: isSet(object.level) ? Number(object.level) : 0,
      sstIds: Array.isArray(object?.sstIds) ? object.sstIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: TriggerManualCompactionRequest): unknown {
    const obj: any = {};
    message.compactionGroupId !== undefined && (obj.compactionGroupId = Math.round(message.compactionGroupId));
    message.keyRange !== undefined && (obj.keyRange = message.keyRange ? KeyRange.toJSON(message.keyRange) : undefined);
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.level !== undefined && (obj.level = Math.round(message.level));
    if (message.sstIds) {
      obj.sstIds = message.sstIds.map((e) => Math.round(e));
    } else {
      obj.sstIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerManualCompactionRequest>, I>>(
    object: I,
  ): TriggerManualCompactionRequest {
    const message = createBaseTriggerManualCompactionRequest();
    message.compactionGroupId = object.compactionGroupId ?? 0;
    message.keyRange = (object.keyRange !== undefined && object.keyRange !== null)
      ? KeyRange.fromPartial(object.keyRange)
      : undefined;
    message.tableId = object.tableId ?? 0;
    message.level = object.level ?? 0;
    message.sstIds = object.sstIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseTriggerManualCompactionResponse(): TriggerManualCompactionResponse {
  return { status: undefined };
}

export const TriggerManualCompactionResponse = {
  encode(message: TriggerManualCompactionResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TriggerManualCompactionResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTriggerManualCompactionResponse();
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

  fromJSON(object: any): TriggerManualCompactionResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: TriggerManualCompactionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerManualCompactionResponse>, I>>(
    object: I,
  ): TriggerManualCompactionResponse {
    const message = createBaseTriggerManualCompactionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseReportFullScanTaskRequest(): ReportFullScanTaskRequest {
  return { sstIds: [] };
}

export const ReportFullScanTaskRequest = {
  encode(message: ReportFullScanTaskRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.sstIds) {
      writer.uint64(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportFullScanTaskRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportFullScanTaskRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.sstIds.push(longToNumber(reader.uint64() as Long));
            }
          } else {
            message.sstIds.push(longToNumber(reader.uint64() as Long));
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ReportFullScanTaskRequest {
    return { sstIds: Array.isArray(object?.sstIds) ? object.sstIds.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: ReportFullScanTaskRequest): unknown {
    const obj: any = {};
    if (message.sstIds) {
      obj.sstIds = message.sstIds.map((e) => Math.round(e));
    } else {
      obj.sstIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportFullScanTaskRequest>, I>>(object: I): ReportFullScanTaskRequest {
    const message = createBaseReportFullScanTaskRequest();
    message.sstIds = object.sstIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseReportFullScanTaskResponse(): ReportFullScanTaskResponse {
  return { status: undefined };
}

export const ReportFullScanTaskResponse = {
  encode(message: ReportFullScanTaskResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ReportFullScanTaskResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseReportFullScanTaskResponse();
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

  fromJSON(object: any): ReportFullScanTaskResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: ReportFullScanTaskResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportFullScanTaskResponse>, I>>(object: I): ReportFullScanTaskResponse {
    const message = createBaseReportFullScanTaskResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseTriggerFullGCRequest(): TriggerFullGCRequest {
  return { sstRetentionTimeSec: 0 };
}

export const TriggerFullGCRequest = {
  encode(message: TriggerFullGCRequest, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.sstRetentionTimeSec !== 0) {
      writer.uint32(8).uint64(message.sstRetentionTimeSec);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TriggerFullGCRequest {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTriggerFullGCRequest();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sstRetentionTimeSec = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TriggerFullGCRequest {
    return { sstRetentionTimeSec: isSet(object.sstRetentionTimeSec) ? Number(object.sstRetentionTimeSec) : 0 };
  },

  toJSON(message: TriggerFullGCRequest): unknown {
    const obj: any = {};
    message.sstRetentionTimeSec !== undefined && (obj.sstRetentionTimeSec = Math.round(message.sstRetentionTimeSec));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerFullGCRequest>, I>>(object: I): TriggerFullGCRequest {
    const message = createBaseTriggerFullGCRequest();
    message.sstRetentionTimeSec = object.sstRetentionTimeSec ?? 0;
    return message;
  },
};

function createBaseTriggerFullGCResponse(): TriggerFullGCResponse {
  return { status: undefined };
}

export const TriggerFullGCResponse = {
  encode(message: TriggerFullGCResponse, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.status !== undefined) {
      Status.encode(message.status, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TriggerFullGCResponse {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTriggerFullGCResponse();
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

  fromJSON(object: any): TriggerFullGCResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: TriggerFullGCResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerFullGCResponse>, I>>(object: I): TriggerFullGCResponse {
    const message = createBaseTriggerFullGCResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseCompactionConfig(): CompactionConfig {
  return {
    maxBytesForLevelBase: 0,
    maxLevel: 0,
    maxBytesForLevelMultiplier: 0,
    maxCompactionBytes: 0,
    subLevelMaxCompactionBytes: 0,
    level0TriggerFileNumber: 0,
    level0TierCompactFileNumber: 0,
    compactionMode: 0,
    compressionAlgorithm: [],
    targetFileSizeBase: 0,
    compactionFilterMask: 0,
    maxSubCompaction: 0,
  };
}

export const CompactionConfig = {
  encode(message: CompactionConfig, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.maxBytesForLevelBase !== 0) {
      writer.uint32(8).uint64(message.maxBytesForLevelBase);
    }
    if (message.maxLevel !== 0) {
      writer.uint32(16).uint64(message.maxLevel);
    }
    if (message.maxBytesForLevelMultiplier !== 0) {
      writer.uint32(24).uint64(message.maxBytesForLevelMultiplier);
    }
    if (message.maxCompactionBytes !== 0) {
      writer.uint32(32).uint64(message.maxCompactionBytes);
    }
    if (message.subLevelMaxCompactionBytes !== 0) {
      writer.uint32(40).uint64(message.subLevelMaxCompactionBytes);
    }
    if (message.level0TriggerFileNumber !== 0) {
      writer.uint32(48).uint64(message.level0TriggerFileNumber);
    }
    if (message.level0TierCompactFileNumber !== 0) {
      writer.uint32(56).uint64(message.level0TierCompactFileNumber);
    }
    if (message.compactionMode !== 0) {
      writer.uint32(64).int32(message.compactionMode);
    }
    for (const v of message.compressionAlgorithm) {
      writer.uint32(74).string(v!);
    }
    if (message.targetFileSizeBase !== 0) {
      writer.uint32(80).uint64(message.targetFileSizeBase);
    }
    if (message.compactionFilterMask !== 0) {
      writer.uint32(88).uint32(message.compactionFilterMask);
    }
    if (message.maxSubCompaction !== 0) {
      writer.uint32(96).uint32(message.maxSubCompaction);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): CompactionConfig {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseCompactionConfig();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.maxBytesForLevelBase = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.maxLevel = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          message.maxBytesForLevelMultiplier = longToNumber(reader.uint64() as Long);
          break;
        case 4:
          message.maxCompactionBytes = longToNumber(reader.uint64() as Long);
          break;
        case 5:
          message.subLevelMaxCompactionBytes = longToNumber(reader.uint64() as Long);
          break;
        case 6:
          message.level0TriggerFileNumber = longToNumber(reader.uint64() as Long);
          break;
        case 7:
          message.level0TierCompactFileNumber = longToNumber(reader.uint64() as Long);
          break;
        case 8:
          message.compactionMode = reader.int32() as any;
          break;
        case 9:
          message.compressionAlgorithm.push(reader.string());
          break;
        case 10:
          message.targetFileSizeBase = longToNumber(reader.uint64() as Long);
          break;
        case 11:
          message.compactionFilterMask = reader.uint32();
          break;
        case 12:
          message.maxSubCompaction = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): CompactionConfig {
    return {
      maxBytesForLevelBase: isSet(object.maxBytesForLevelBase) ? Number(object.maxBytesForLevelBase) : 0,
      maxLevel: isSet(object.maxLevel) ? Number(object.maxLevel) : 0,
      maxBytesForLevelMultiplier: isSet(object.maxBytesForLevelMultiplier)
        ? Number(object.maxBytesForLevelMultiplier)
        : 0,
      maxCompactionBytes: isSet(object.maxCompactionBytes) ? Number(object.maxCompactionBytes) : 0,
      subLevelMaxCompactionBytes: isSet(object.subLevelMaxCompactionBytes)
        ? Number(object.subLevelMaxCompactionBytes)
        : 0,
      level0TriggerFileNumber: isSet(object.level0TriggerFileNumber) ? Number(object.level0TriggerFileNumber) : 0,
      level0TierCompactFileNumber: isSet(object.level0TierCompactFileNumber)
        ? Number(object.level0TierCompactFileNumber)
        : 0,
      compactionMode: isSet(object.compactionMode) ? compactionConfig_CompactionModeFromJSON(object.compactionMode) : 0,
      compressionAlgorithm: Array.isArray(object?.compressionAlgorithm)
        ? object.compressionAlgorithm.map((e: any) => String(e))
        : [],
      targetFileSizeBase: isSet(object.targetFileSizeBase) ? Number(object.targetFileSizeBase) : 0,
      compactionFilterMask: isSet(object.compactionFilterMask) ? Number(object.compactionFilterMask) : 0,
      maxSubCompaction: isSet(object.maxSubCompaction) ? Number(object.maxSubCompaction) : 0,
    };
  },

  toJSON(message: CompactionConfig): unknown {
    const obj: any = {};
    message.maxBytesForLevelBase !== undefined && (obj.maxBytesForLevelBase = Math.round(message.maxBytesForLevelBase));
    message.maxLevel !== undefined && (obj.maxLevel = Math.round(message.maxLevel));
    message.maxBytesForLevelMultiplier !== undefined &&
      (obj.maxBytesForLevelMultiplier = Math.round(message.maxBytesForLevelMultiplier));
    message.maxCompactionBytes !== undefined && (obj.maxCompactionBytes = Math.round(message.maxCompactionBytes));
    message.subLevelMaxCompactionBytes !== undefined &&
      (obj.subLevelMaxCompactionBytes = Math.round(message.subLevelMaxCompactionBytes));
    message.level0TriggerFileNumber !== undefined &&
      (obj.level0TriggerFileNumber = Math.round(message.level0TriggerFileNumber));
    message.level0TierCompactFileNumber !== undefined &&
      (obj.level0TierCompactFileNumber = Math.round(message.level0TierCompactFileNumber));
    message.compactionMode !== undefined &&
      (obj.compactionMode = compactionConfig_CompactionModeToJSON(message.compactionMode));
    if (message.compressionAlgorithm) {
      obj.compressionAlgorithm = message.compressionAlgorithm.map((e) => e);
    } else {
      obj.compressionAlgorithm = [];
    }
    message.targetFileSizeBase !== undefined && (obj.targetFileSizeBase = Math.round(message.targetFileSizeBase));
    message.compactionFilterMask !== undefined && (obj.compactionFilterMask = Math.round(message.compactionFilterMask));
    message.maxSubCompaction !== undefined && (obj.maxSubCompaction = Math.round(message.maxSubCompaction));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionConfig>, I>>(object: I): CompactionConfig {
    const message = createBaseCompactionConfig();
    message.maxBytesForLevelBase = object.maxBytesForLevelBase ?? 0;
    message.maxLevel = object.maxLevel ?? 0;
    message.maxBytesForLevelMultiplier = object.maxBytesForLevelMultiplier ?? 0;
    message.maxCompactionBytes = object.maxCompactionBytes ?? 0;
    message.subLevelMaxCompactionBytes = object.subLevelMaxCompactionBytes ?? 0;
    message.level0TriggerFileNumber = object.level0TriggerFileNumber ?? 0;
    message.level0TierCompactFileNumber = object.level0TierCompactFileNumber ?? 0;
    message.compactionMode = object.compactionMode ?? 0;
    message.compressionAlgorithm = object.compressionAlgorithm?.map((e) => e) || [];
    message.targetFileSizeBase = object.targetFileSizeBase ?? 0;
    message.compactionFilterMask = object.compactionFilterMask ?? 0;
    message.maxSubCompaction = object.maxSubCompaction ?? 0;
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

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
