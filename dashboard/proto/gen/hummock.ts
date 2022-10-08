/* eslint-disable */
import { Status, WorkerNode } from "./common";

export const protobufPackage = "hummock";

export const LevelType = {
  UNSPECIFIED: "UNSPECIFIED",
  NONOVERLAPPING: "NONOVERLAPPING",
  OVERLAPPING: "OVERLAPPING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type LevelType = typeof LevelType[keyof typeof LevelType];

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
  metaOffset: number;
  staleKeyCount: number;
  totalKeyCount: number;
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

export interface GetCurrentVersionRequest {
}

export interface GetCurrentVersionResponse {
  status: Status | undefined;
  currentVersion: HummockVersion | undefined;
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

export const CompactTask_TaskStatus = {
  UNSPECIFIED: "UNSPECIFIED",
  PENDING: "PENDING",
  SUCCESS: "SUCCESS",
  HEARTBEAT_CANCELED: "HEARTBEAT_CANCELED",
  NO_AVAIL_CANCELED: "NO_AVAIL_CANCELED",
  ASSIGN_FAIL_CANCELED: "ASSIGN_FAIL_CANCELED",
  SEND_FAIL_CANCELED: "SEND_FAIL_CANCELED",
  MANUAL_CANCELED: "MANUAL_CANCELED",
  EXECUTE_FAILED: "EXECUTE_FAILED",
  JOIN_HANDLE_FAILED: "JOIN_HANDLE_FAILED",
  TRACK_SST_ID_FAILED: "TRACK_SST_ID_FAILED",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type CompactTask_TaskStatus = typeof CompactTask_TaskStatus[keyof typeof CompactTask_TaskStatus];

export function compactTask_TaskStatusFromJSON(object: any): CompactTask_TaskStatus {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return CompactTask_TaskStatus.UNSPECIFIED;
    case 1:
    case "PENDING":
      return CompactTask_TaskStatus.PENDING;
    case 2:
    case "SUCCESS":
      return CompactTask_TaskStatus.SUCCESS;
    case 3:
    case "HEARTBEAT_CANCELED":
      return CompactTask_TaskStatus.HEARTBEAT_CANCELED;
    case 4:
    case "NO_AVAIL_CANCELED":
      return CompactTask_TaskStatus.NO_AVAIL_CANCELED;
    case 5:
    case "ASSIGN_FAIL_CANCELED":
      return CompactTask_TaskStatus.ASSIGN_FAIL_CANCELED;
    case 6:
    case "SEND_FAIL_CANCELED":
      return CompactTask_TaskStatus.SEND_FAIL_CANCELED;
    case 7:
    case "MANUAL_CANCELED":
      return CompactTask_TaskStatus.MANUAL_CANCELED;
    case 8:
    case "EXECUTE_FAILED":
      return CompactTask_TaskStatus.EXECUTE_FAILED;
    case 9:
    case "JOIN_HANDLE_FAILED":
      return CompactTask_TaskStatus.JOIN_HANDLE_FAILED;
    case 10:
    case "TRACK_SST_ID_FAILED":
      return CompactTask_TaskStatus.TRACK_SST_ID_FAILED;
    case -1:
    case "UNRECOGNIZED":
    default:
      return CompactTask_TaskStatus.UNRECOGNIZED;
  }
}

export function compactTask_TaskStatusToJSON(object: CompactTask_TaskStatus): string {
  switch (object) {
    case CompactTask_TaskStatus.UNSPECIFIED:
      return "UNSPECIFIED";
    case CompactTask_TaskStatus.PENDING:
      return "PENDING";
    case CompactTask_TaskStatus.SUCCESS:
      return "SUCCESS";
    case CompactTask_TaskStatus.HEARTBEAT_CANCELED:
      return "HEARTBEAT_CANCELED";
    case CompactTask_TaskStatus.NO_AVAIL_CANCELED:
      return "NO_AVAIL_CANCELED";
    case CompactTask_TaskStatus.ASSIGN_FAIL_CANCELED:
      return "ASSIGN_FAIL_CANCELED";
    case CompactTask_TaskStatus.SEND_FAIL_CANCELED:
      return "SEND_FAIL_CANCELED";
    case CompactTask_TaskStatus.MANUAL_CANCELED:
      return "MANUAL_CANCELED";
    case CompactTask_TaskStatus.EXECUTE_FAILED:
      return "EXECUTE_FAILED";
    case CompactTask_TaskStatus.JOIN_HANDLE_FAILED:
      return "JOIN_HANDLE_FAILED";
    case CompactTask_TaskStatus.TRACK_SST_ID_FAILED:
      return "TRACK_SST_ID_FAILED";
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

/**
 * This is a heartbeat message. Task will be considered dead if
 * `CompactTaskProgress` is not received for a timeout
 * or `num_ssts_sealed`/`num_ssts_uploaded` do not increase for a timeout.
 */
export interface CompactTaskProgress {
  taskId: number;
  numSstsSealed: number;
  numSstsUploaded: number;
}

export interface ReportCompactionTaskProgressRequest {
  contextId: number;
  progress: CompactTaskProgress[];
}

export interface ReportCompactionTaskProgressResponse {
  status: Status | undefined;
}

export interface SubscribeCompactTasksRequest {
  contextId: number;
  maxConcurrentTaskNumber: number;
}

export interface ValidationTask {
  sstInfos: SstableInfo[];
  sstIdToWorkerId: { [key: number]: number };
  epoch: number;
}

export interface ValidationTask_SstIdToWorkerIdEntry {
  key: number;
  value: number;
}

export interface SubscribeCompactTasksResponse {
  task?:
    | { $case: "compactTask"; compactTask: CompactTask }
    | { $case: "vacuumTask"; vacuumTask: VacuumTask }
    | { $case: "fullScanTask"; fullScanTask: FullScanTask }
    | { $case: "validationTask"; validationTask: ValidationTask }
    | { $case: "cancelCompactTask"; cancelCompactTask: CancelCompactTask };
}

/** Delete SSTs in object store */
export interface VacuumTask {
  sstableIds: number[];
}

/** Scan object store to get candidate orphan SSTs. */
export interface FullScanTask {
  sstRetentionTimeSec: number;
}

/** Cancel compact task */
export interface CancelCompactTask {
  contextId: number;
  taskId: number;
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

export interface GetVersionDeltasRequest {
  startId: number;
  numEpochs: number;
}

export interface GetVersionDeltasResponse {
  versionDeltas: HummockVersionDeltas | undefined;
}

export interface PinnedVersionsSummary {
  pinnedVersions: HummockPinnedVersion[];
  workers: { [key: number]: WorkerNode };
}

export interface PinnedVersionsSummary_WorkersEntry {
  key: number;
  value: WorkerNode | undefined;
}

export interface PinnedSnapshotsSummary {
  pinnedSnapshots: HummockPinnedSnapshot[];
  workers: { [key: number]: WorkerNode };
}

export interface PinnedSnapshotsSummary_WorkersEntry {
  key: number;
  value: WorkerNode | undefined;
}

export interface RiseCtlGetPinnedVersionsSummaryRequest {
}

export interface RiseCtlGetPinnedVersionsSummaryResponse {
  summary: PinnedVersionsSummary | undefined;
}

export interface RiseCtlGetPinnedSnapshotsSummaryRequest {
}

export interface RiseCtlGetPinnedSnapshotsSummaryResponse {
  summary: PinnedSnapshotsSummary | undefined;
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

export const CompactionConfig_CompactionMode = {
  UNSPECIFIED: "UNSPECIFIED",
  RANGE: "RANGE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type CompactionConfig_CompactionMode =
  typeof CompactionConfig_CompactionMode[keyof typeof CompactionConfig_CompactionMode];

export function compactionConfig_CompactionModeFromJSON(object: any): CompactionConfig_CompactionMode {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return CompactionConfig_CompactionMode.UNSPECIFIED;
    case 1:
    case "RANGE":
      return CompactionConfig_CompactionMode.RANGE;
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
    case CompactionConfig_CompactionMode.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

function createBaseSstableInfo(): SstableInfo {
  return { id: 0, keyRange: undefined, fileSize: 0, tableIds: [], metaOffset: 0, staleKeyCount: 0, totalKeyCount: 0 };
}

export const SstableInfo = {
  fromJSON(object: any): SstableInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      keyRange: isSet(object.keyRange) ? KeyRange.fromJSON(object.keyRange) : undefined,
      fileSize: isSet(object.fileSize) ? Number(object.fileSize) : 0,
      tableIds: Array.isArray(object?.tableIds) ? object.tableIds.map((e: any) => Number(e)) : [],
      metaOffset: isSet(object.metaOffset) ? Number(object.metaOffset) : 0,
      staleKeyCount: isSet(object.staleKeyCount) ? Number(object.staleKeyCount) : 0,
      totalKeyCount: isSet(object.totalKeyCount) ? Number(object.totalKeyCount) : 0,
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
    message.metaOffset !== undefined && (obj.metaOffset = Math.round(message.metaOffset));
    message.staleKeyCount !== undefined && (obj.staleKeyCount = Math.round(message.staleKeyCount));
    message.totalKeyCount !== undefined && (obj.totalKeyCount = Math.round(message.totalKeyCount));
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
    message.metaOffset = object.metaOffset ?? 0;
    message.staleKeyCount = object.staleKeyCount ?? 0;
    message.totalKeyCount = object.totalKeyCount ?? 0;
    return message;
  },
};

function createBaseOverlappingLevel(): OverlappingLevel {
  return { subLevels: [], totalFileSize: 0 };
}

export const OverlappingLevel = {
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
  return { levelIdx: 0, levelType: LevelType.UNSPECIFIED, tableInfos: [], totalFileSize: 0, subLevelId: 0 };
}

export const Level = {
  fromJSON(object: any): Level {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      levelType: isSet(object.levelType) ? levelTypeFromJSON(object.levelType) : LevelType.UNSPECIFIED,
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
    message.levelType = object.levelType ?? LevelType.UNSPECIFIED;
    message.tableInfos = object.tableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    message.subLevelId = object.subLevelId ?? 0;
    return message;
  },
};

function createBaseInputLevel(): InputLevel {
  return { levelIdx: 0, levelType: LevelType.UNSPECIFIED, tableInfos: [] };
}

export const InputLevel = {
  fromJSON(object: any): InputLevel {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      levelType: isSet(object.levelType) ? levelTypeFromJSON(object.levelType) : LevelType.UNSPECIFIED,
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
    message.levelType = object.levelType ?? LevelType.UNSPECIFIED;
    message.tableInfos = object.tableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseLevelDelta(): LevelDelta {
  return { levelIdx: 0, l0SubLevelId: 0, removedTableIds: [], insertedTableInfos: [] };
}

export const LevelDelta = {
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
  return { id: 0, levels: {}, maxCommittedEpoch: 0, safeEpoch: 0 };
}

export const HummockVersion = {
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
    return message;
  },
};

function createBaseHummockVersion_Levels(): HummockVersion_Levels {
  return { levels: [], l0: undefined };
}

export const HummockVersion_Levels = {
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
  return { id: 0, prevId: 0, levelDeltas: {}, maxCommittedEpoch: 0, safeEpoch: 0, trivialMove: false };
}

export const HummockVersionDelta = {
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
    return message;
  },
};

function createBaseHummockVersionDelta_LevelDeltas(): HummockVersionDelta_LevelDeltas {
  return { levelDeltas: [] };
}

export const HummockVersionDelta_LevelDeltas = {
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

function createBaseGetCurrentVersionRequest(): GetCurrentVersionRequest {
  return {};
}

export const GetCurrentVersionRequest = {
  fromJSON(_: any): GetCurrentVersionRequest {
    return {};
  },

  toJSON(_: GetCurrentVersionRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCurrentVersionRequest>, I>>(_: I): GetCurrentVersionRequest {
    const message = createBaseGetCurrentVersionRequest();
    return message;
  },
};

function createBaseGetCurrentVersionResponse(): GetCurrentVersionResponse {
  return { status: undefined, currentVersion: undefined };
}

export const GetCurrentVersionResponse = {
  fromJSON(object: any): GetCurrentVersionResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      currentVersion: isSet(object.currentVersion) ? HummockVersion.fromJSON(object.currentVersion) : undefined,
    };
  },

  toJSON(message: GetCurrentVersionResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    message.currentVersion !== undefined &&
      (obj.currentVersion = message.currentVersion ? HummockVersion.toJSON(message.currentVersion) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetCurrentVersionResponse>, I>>(object: I): GetCurrentVersionResponse {
    const message = createBaseGetCurrentVersionResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.currentVersion = (object.currentVersion !== undefined && object.currentVersion !== null)
      ? HummockVersion.fromPartial(object.currentVersion)
      : undefined;
    return message;
  },
};

function createBaseUnpinVersionRequest(): UnpinVersionRequest {
  return { contextId: 0 };
}

export const UnpinVersionRequest = {
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
    taskStatus: CompactTask_TaskStatus.UNSPECIFIED,
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
      taskStatus: isSet(object.taskStatus)
        ? compactTask_TaskStatusFromJSON(object.taskStatus)
        : CompactTask_TaskStatus.UNSPECIFIED,
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
    message.taskStatus = object.taskStatus ?? CompactTask_TaskStatus.UNSPECIFIED;
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

function createBaseCompactTaskProgress(): CompactTaskProgress {
  return { taskId: 0, numSstsSealed: 0, numSstsUploaded: 0 };
}

export const CompactTaskProgress = {
  fromJSON(object: any): CompactTaskProgress {
    return {
      taskId: isSet(object.taskId) ? Number(object.taskId) : 0,
      numSstsSealed: isSet(object.numSstsSealed) ? Number(object.numSstsSealed) : 0,
      numSstsUploaded: isSet(object.numSstsUploaded) ? Number(object.numSstsUploaded) : 0,
    };
  },

  toJSON(message: CompactTaskProgress): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = Math.round(message.taskId));
    message.numSstsSealed !== undefined && (obj.numSstsSealed = Math.round(message.numSstsSealed));
    message.numSstsUploaded !== undefined && (obj.numSstsUploaded = Math.round(message.numSstsUploaded));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactTaskProgress>, I>>(object: I): CompactTaskProgress {
    const message = createBaseCompactTaskProgress();
    message.taskId = object.taskId ?? 0;
    message.numSstsSealed = object.numSstsSealed ?? 0;
    message.numSstsUploaded = object.numSstsUploaded ?? 0;
    return message;
  },
};

function createBaseReportCompactionTaskProgressRequest(): ReportCompactionTaskProgressRequest {
  return { contextId: 0, progress: [] };
}

export const ReportCompactionTaskProgressRequest = {
  fromJSON(object: any): ReportCompactionTaskProgressRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      progress: Array.isArray(object?.progress) ? object.progress.map((e: any) => CompactTaskProgress.fromJSON(e)) : [],
    };
  },

  toJSON(message: ReportCompactionTaskProgressRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    if (message.progress) {
      obj.progress = message.progress.map((e) => e ? CompactTaskProgress.toJSON(e) : undefined);
    } else {
      obj.progress = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTaskProgressRequest>, I>>(
    object: I,
  ): ReportCompactionTaskProgressRequest {
    const message = createBaseReportCompactionTaskProgressRequest();
    message.contextId = object.contextId ?? 0;
    message.progress = object.progress?.map((e) => CompactTaskProgress.fromPartial(e)) || [];
    return message;
  },
};

function createBaseReportCompactionTaskProgressResponse(): ReportCompactionTaskProgressResponse {
  return { status: undefined };
}

export const ReportCompactionTaskProgressResponse = {
  fromJSON(object: any): ReportCompactionTaskProgressResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: ReportCompactionTaskProgressResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTaskProgressResponse>, I>>(
    object: I,
  ): ReportCompactionTaskProgressResponse {
    const message = createBaseReportCompactionTaskProgressResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseSubscribeCompactTasksRequest(): SubscribeCompactTasksRequest {
  return { contextId: 0, maxConcurrentTaskNumber: 0 };
}

export const SubscribeCompactTasksRequest = {
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
  return { sstInfos: [], sstIdToWorkerId: {}, epoch: 0 };
}

export const ValidationTask = {
  fromJSON(object: any): ValidationTask {
    return {
      sstInfos: Array.isArray(object?.sstInfos) ? object.sstInfos.map((e: any) => SstableInfo.fromJSON(e)) : [],
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
    if (message.sstInfos) {
      obj.sstInfos = message.sstInfos.map((e) => e ? SstableInfo.toJSON(e) : undefined);
    } else {
      obj.sstInfos = [];
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
    message.sstInfos = object.sstInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
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
        : isSet(object.cancelCompactTask)
        ? { $case: "cancelCompactTask", cancelCompactTask: CancelCompactTask.fromJSON(object.cancelCompactTask) }
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
    message.task?.$case === "cancelCompactTask" && (obj.cancelCompactTask = message.task?.cancelCompactTask
      ? CancelCompactTask.toJSON(message.task?.cancelCompactTask)
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
    if (
      object.task?.$case === "cancelCompactTask" &&
      object.task?.cancelCompactTask !== undefined &&
      object.task?.cancelCompactTask !== null
    ) {
      message.task = {
        $case: "cancelCompactTask",
        cancelCompactTask: CancelCompactTask.fromPartial(object.task.cancelCompactTask),
      };
    }
    return message;
  },
};

function createBaseVacuumTask(): VacuumTask {
  return { sstableIds: [] };
}

export const VacuumTask = {
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

function createBaseCancelCompactTask(): CancelCompactTask {
  return { contextId: 0, taskId: 0 };
}

export const CancelCompactTask = {
  fromJSON(object: any): CancelCompactTask {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      taskId: isSet(object.taskId) ? Number(object.taskId) : 0,
    };
  },

  toJSON(message: CancelCompactTask): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.taskId !== undefined && (obj.taskId = Math.round(message.taskId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CancelCompactTask>, I>>(object: I): CancelCompactTask {
    const message = createBaseCancelCompactTask();
    message.contextId = object.contextId ?? 0;
    message.taskId = object.taskId ?? 0;
    return message;
  },
};

function createBaseReportVacuumTaskRequest(): ReportVacuumTaskRequest {
  return { vacuumTask: undefined };
}

export const ReportVacuumTaskRequest = {
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

function createBaseGetVersionDeltasRequest(): GetVersionDeltasRequest {
  return { startId: 0, numEpochs: 0 };
}

export const GetVersionDeltasRequest = {
  fromJSON(object: any): GetVersionDeltasRequest {
    return {
      startId: isSet(object.startId) ? Number(object.startId) : 0,
      numEpochs: isSet(object.numEpochs) ? Number(object.numEpochs) : 0,
    };
  },

  toJSON(message: GetVersionDeltasRequest): unknown {
    const obj: any = {};
    message.startId !== undefined && (obj.startId = Math.round(message.startId));
    message.numEpochs !== undefined && (obj.numEpochs = Math.round(message.numEpochs));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetVersionDeltasRequest>, I>>(object: I): GetVersionDeltasRequest {
    const message = createBaseGetVersionDeltasRequest();
    message.startId = object.startId ?? 0;
    message.numEpochs = object.numEpochs ?? 0;
    return message;
  },
};

function createBaseGetVersionDeltasResponse(): GetVersionDeltasResponse {
  return { versionDeltas: undefined };
}

export const GetVersionDeltasResponse = {
  fromJSON(object: any): GetVersionDeltasResponse {
    return {
      versionDeltas: isSet(object.versionDeltas) ? HummockVersionDeltas.fromJSON(object.versionDeltas) : undefined,
    };
  },

  toJSON(message: GetVersionDeltasResponse): unknown {
    const obj: any = {};
    message.versionDeltas !== undefined &&
      (obj.versionDeltas = message.versionDeltas ? HummockVersionDeltas.toJSON(message.versionDeltas) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetVersionDeltasResponse>, I>>(object: I): GetVersionDeltasResponse {
    const message = createBaseGetVersionDeltasResponse();
    message.versionDeltas = (object.versionDeltas !== undefined && object.versionDeltas !== null)
      ? HummockVersionDeltas.fromPartial(object.versionDeltas)
      : undefined;
    return message;
  },
};

function createBasePinnedVersionsSummary(): PinnedVersionsSummary {
  return { pinnedVersions: [], workers: {} };
}

export const PinnedVersionsSummary = {
  fromJSON(object: any): PinnedVersionsSummary {
    return {
      pinnedVersions: Array.isArray(object?.pinnedVersions)
        ? object.pinnedVersions.map((e: any) => HummockPinnedVersion.fromJSON(e))
        : [],
      workers: isObject(object.workers)
        ? Object.entries(object.workers).reduce<{ [key: number]: WorkerNode }>((acc, [key, value]) => {
          acc[Number(key)] = WorkerNode.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: PinnedVersionsSummary): unknown {
    const obj: any = {};
    if (message.pinnedVersions) {
      obj.pinnedVersions = message.pinnedVersions.map((e) => e ? HummockPinnedVersion.toJSON(e) : undefined);
    } else {
      obj.pinnedVersions = [];
    }
    obj.workers = {};
    if (message.workers) {
      Object.entries(message.workers).forEach(([k, v]) => {
        obj.workers[k] = WorkerNode.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinnedVersionsSummary>, I>>(object: I): PinnedVersionsSummary {
    const message = createBasePinnedVersionsSummary();
    message.pinnedVersions = object.pinnedVersions?.map((e) => HummockPinnedVersion.fromPartial(e)) || [];
    message.workers = Object.entries(object.workers ?? {}).reduce<{ [key: number]: WorkerNode }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = WorkerNode.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBasePinnedVersionsSummary_WorkersEntry(): PinnedVersionsSummary_WorkersEntry {
  return { key: 0, value: undefined };
}

export const PinnedVersionsSummary_WorkersEntry = {
  fromJSON(object: any): PinnedVersionsSummary_WorkersEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? WorkerNode.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: PinnedVersionsSummary_WorkersEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? WorkerNode.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinnedVersionsSummary_WorkersEntry>, I>>(
    object: I,
  ): PinnedVersionsSummary_WorkersEntry {
    const message = createBasePinnedVersionsSummary_WorkersEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? WorkerNode.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBasePinnedSnapshotsSummary(): PinnedSnapshotsSummary {
  return { pinnedSnapshots: [], workers: {} };
}

export const PinnedSnapshotsSummary = {
  fromJSON(object: any): PinnedSnapshotsSummary {
    return {
      pinnedSnapshots: Array.isArray(object?.pinnedSnapshots)
        ? object.pinnedSnapshots.map((e: any) => HummockPinnedSnapshot.fromJSON(e))
        : [],
      workers: isObject(object.workers)
        ? Object.entries(object.workers).reduce<{ [key: number]: WorkerNode }>((acc, [key, value]) => {
          acc[Number(key)] = WorkerNode.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: PinnedSnapshotsSummary): unknown {
    const obj: any = {};
    if (message.pinnedSnapshots) {
      obj.pinnedSnapshots = message.pinnedSnapshots.map((e) => e ? HummockPinnedSnapshot.toJSON(e) : undefined);
    } else {
      obj.pinnedSnapshots = [];
    }
    obj.workers = {};
    if (message.workers) {
      Object.entries(message.workers).forEach(([k, v]) => {
        obj.workers[k] = WorkerNode.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinnedSnapshotsSummary>, I>>(object: I): PinnedSnapshotsSummary {
    const message = createBasePinnedSnapshotsSummary();
    message.pinnedSnapshots = object.pinnedSnapshots?.map((e) => HummockPinnedSnapshot.fromPartial(e)) || [];
    message.workers = Object.entries(object.workers ?? {}).reduce<{ [key: number]: WorkerNode }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = WorkerNode.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBasePinnedSnapshotsSummary_WorkersEntry(): PinnedSnapshotsSummary_WorkersEntry {
  return { key: 0, value: undefined };
}

export const PinnedSnapshotsSummary_WorkersEntry = {
  fromJSON(object: any): PinnedSnapshotsSummary_WorkersEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? WorkerNode.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: PinnedSnapshotsSummary_WorkersEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? WorkerNode.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinnedSnapshotsSummary_WorkersEntry>, I>>(
    object: I,
  ): PinnedSnapshotsSummary_WorkersEntry {
    const message = createBasePinnedSnapshotsSummary_WorkersEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? WorkerNode.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseRiseCtlGetPinnedVersionsSummaryRequest(): RiseCtlGetPinnedVersionsSummaryRequest {
  return {};
}

export const RiseCtlGetPinnedVersionsSummaryRequest = {
  fromJSON(_: any): RiseCtlGetPinnedVersionsSummaryRequest {
    return {};
  },

  toJSON(_: RiseCtlGetPinnedVersionsSummaryRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlGetPinnedVersionsSummaryRequest>, I>>(
    _: I,
  ): RiseCtlGetPinnedVersionsSummaryRequest {
    const message = createBaseRiseCtlGetPinnedVersionsSummaryRequest();
    return message;
  },
};

function createBaseRiseCtlGetPinnedVersionsSummaryResponse(): RiseCtlGetPinnedVersionsSummaryResponse {
  return { summary: undefined };
}

export const RiseCtlGetPinnedVersionsSummaryResponse = {
  fromJSON(object: any): RiseCtlGetPinnedVersionsSummaryResponse {
    return { summary: isSet(object.summary) ? PinnedVersionsSummary.fromJSON(object.summary) : undefined };
  },

  toJSON(message: RiseCtlGetPinnedVersionsSummaryResponse): unknown {
    const obj: any = {};
    message.summary !== undefined &&
      (obj.summary = message.summary ? PinnedVersionsSummary.toJSON(message.summary) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlGetPinnedVersionsSummaryResponse>, I>>(
    object: I,
  ): RiseCtlGetPinnedVersionsSummaryResponse {
    const message = createBaseRiseCtlGetPinnedVersionsSummaryResponse();
    message.summary = (object.summary !== undefined && object.summary !== null)
      ? PinnedVersionsSummary.fromPartial(object.summary)
      : undefined;
    return message;
  },
};

function createBaseRiseCtlGetPinnedSnapshotsSummaryRequest(): RiseCtlGetPinnedSnapshotsSummaryRequest {
  return {};
}

export const RiseCtlGetPinnedSnapshotsSummaryRequest = {
  fromJSON(_: any): RiseCtlGetPinnedSnapshotsSummaryRequest {
    return {};
  },

  toJSON(_: RiseCtlGetPinnedSnapshotsSummaryRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlGetPinnedSnapshotsSummaryRequest>, I>>(
    _: I,
  ): RiseCtlGetPinnedSnapshotsSummaryRequest {
    const message = createBaseRiseCtlGetPinnedSnapshotsSummaryRequest();
    return message;
  },
};

function createBaseRiseCtlGetPinnedSnapshotsSummaryResponse(): RiseCtlGetPinnedSnapshotsSummaryResponse {
  return { summary: undefined };
}

export const RiseCtlGetPinnedSnapshotsSummaryResponse = {
  fromJSON(object: any): RiseCtlGetPinnedSnapshotsSummaryResponse {
    return { summary: isSet(object.summary) ? PinnedSnapshotsSummary.fromJSON(object.summary) : undefined };
  },

  toJSON(message: RiseCtlGetPinnedSnapshotsSummaryResponse): unknown {
    const obj: any = {};
    message.summary !== undefined &&
      (obj.summary = message.summary ? PinnedSnapshotsSummary.toJSON(message.summary) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlGetPinnedSnapshotsSummaryResponse>, I>>(
    object: I,
  ): RiseCtlGetPinnedSnapshotsSummaryResponse {
    const message = createBaseRiseCtlGetPinnedSnapshotsSummaryResponse();
    message.summary = (object.summary !== undefined && object.summary !== null)
      ? PinnedSnapshotsSummary.fromPartial(object.summary)
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
    compactionMode: CompactionConfig_CompactionMode.UNSPECIFIED,
    compressionAlgorithm: [],
    targetFileSizeBase: 0,
    compactionFilterMask: 0,
    maxSubCompaction: 0,
  };
}

export const CompactionConfig = {
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
      compactionMode: isSet(object.compactionMode)
        ? compactionConfig_CompactionModeFromJSON(object.compactionMode)
        : CompactionConfig_CompactionMode.UNSPECIFIED,
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
    message.compactionMode = object.compactionMode ?? CompactionConfig_CompactionMode.UNSPECIFIED;
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

function isObject(value: any): boolean {
  return typeof value === "object" && value !== null;
}

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
