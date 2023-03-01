/* eslint-disable */
import { Table } from "./catalog";
import { Status, WorkerNode } from "./common";
import { CompactorRuntimeConfig } from "./compactor";

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
  /** When a SST is divided, its divide_version will increase one. */
  divideVersion: number;
  minEpoch: number;
  maxEpoch: number;
  uncompressedFileSize: number;
}

export interface OverlappingLevel {
  subLevels: Level[];
  totalFileSize: number;
  uncompressedFileSize: number;
}

export interface Level {
  levelIdx: number;
  levelType: LevelType;
  tableInfos: SstableInfo[];
  totalFileSize: number;
  subLevelId: number;
  uncompressedFileSize: number;
}

export interface InputLevel {
  levelIdx: number;
  levelType: LevelType;
  tableInfos: SstableInfo[];
}

export interface IntraLevelDelta {
  levelIdx: number;
  l0SubLevelId: number;
  removedTableIds: number[];
  insertedTableInfos: SstableInfo[];
}

export interface GroupConstruct {
  groupConfig:
    | CompactionConfig
    | undefined;
  /** If `parent_group_id` is not 0, it means `parent_group_id` splits into `parent_group_id` and this group, so this group is not empty initially. */
  parentGroupId: number;
  tableIds: number[];
  groupId: number;
}

export interface GroupMetaChange {
  tableIdsAdd: number[];
  tableIdsRemove: number[];
}

export interface GroupDestroy {
}

export interface GroupDelta {
  deltaType?:
    | { $case: "intraLevel"; intraLevel: IntraLevelDelta }
    | { $case: "groupConstruct"; groupConstruct: GroupConstruct }
    | { $case: "groupDestroy"; groupDestroy: GroupDestroy }
    | { $case: "groupMetaChange"; groupMetaChange: GroupMetaChange };
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
  groupId: number;
  parentGroupId: number;
  memberTableIds: number[];
}

export interface HummockVersion_LevelsEntry {
  key: number;
  value: HummockVersion_Levels | undefined;
}

export interface HummockVersionDelta {
  id: number;
  prevId: number;
  /** Levels of each compaction group */
  groupDeltas: { [key: number]: HummockVersionDelta_GroupDeltas };
  maxCommittedEpoch: number;
  /**
   * Snapshots with epoch less than the safe epoch have been GCed.
   * Reads against such an epoch will fail.
   */
  safeEpoch: number;
  trivialMove: boolean;
  gcSstIds: number[];
}

export interface HummockVersionDelta_GroupDeltas {
  groupDeltas: GroupDelta[];
}

export interface HummockVersionDelta_GroupDeltasEntry {
  key: number;
  value: HummockVersionDelta_GroupDeltas | undefined;
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

export interface VersionUpdatePayload {
  payload?: { $case: "versionDeltas"; versionDeltas: HummockVersionDeltas } | {
    $case: "pinnedVersion";
    pinnedVersion: HummockVersion;
  };
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

export interface PinSpecificSnapshotRequest {
  contextId: number;
  epoch: number;
}

export interface GetAssignedCompactTaskNumRequest {
}

export interface GetAssignedCompactTaskNumResponse {
  numTasks: number;
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

/**
 * When `right_exclusive=false`, it represents [left, right], of which both boundary are open. When `right_exclusive=true`,
 * it represents [left, right), of which right is exclusive.
 */
export interface KeyRange {
  left: Uint8Array;
  right: Uint8Array;
  rightExclusive: boolean;
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
  /** Identifies whether the task is space_reclaim, if the compact_task_type increases, it will be refactored to enum */
  taskType: CompactTask_TaskType;
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
  INVALID_GROUP_CANCELED: "INVALID_GROUP_CANCELED",
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
    case "INVALID_GROUP_CANCELED":
      return CompactTask_TaskStatus.INVALID_GROUP_CANCELED;
    case 9:
    case "EXECUTE_FAILED":
      return CompactTask_TaskStatus.EXECUTE_FAILED;
    case 10:
    case "JOIN_HANDLE_FAILED":
      return CompactTask_TaskStatus.JOIN_HANDLE_FAILED;
    case 11:
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
    case CompactTask_TaskStatus.INVALID_GROUP_CANCELED:
      return "INVALID_GROUP_CANCELED";
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

export const CompactTask_TaskType = {
  TYPE_UNSPECIFIED: "TYPE_UNSPECIFIED",
  DYNAMIC: "DYNAMIC",
  SPACE_RECLAIM: "SPACE_RECLAIM",
  MANUAL: "MANUAL",
  SHARED_BUFFER: "SHARED_BUFFER",
  TTL: "TTL",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type CompactTask_TaskType = typeof CompactTask_TaskType[keyof typeof CompactTask_TaskType];

export function compactTask_TaskTypeFromJSON(object: any): CompactTask_TaskType {
  switch (object) {
    case 0:
    case "TYPE_UNSPECIFIED":
      return CompactTask_TaskType.TYPE_UNSPECIFIED;
    case 1:
    case "DYNAMIC":
      return CompactTask_TaskType.DYNAMIC;
    case 2:
    case "SPACE_RECLAIM":
      return CompactTask_TaskType.SPACE_RECLAIM;
    case 3:
    case "MANUAL":
      return CompactTask_TaskType.MANUAL;
    case 4:
    case "SHARED_BUFFER":
      return CompactTask_TaskType.SHARED_BUFFER;
    case 5:
    case "TTL":
      return CompactTask_TaskType.TTL;
    case -1:
    case "UNRECOGNIZED":
    default:
      return CompactTask_TaskType.UNRECOGNIZED;
  }
}

export function compactTask_TaskTypeToJSON(object: CompactTask_TaskType): string {
  switch (object) {
    case CompactTask_TaskType.TYPE_UNSPECIFIED:
      return "TYPE_UNSPECIFIED";
    case CompactTask_TaskType.DYNAMIC:
      return "DYNAMIC";
    case CompactTask_TaskType.SPACE_RECLAIM:
      return "SPACE_RECLAIM";
    case CompactTask_TaskType.MANUAL:
      return "MANUAL";
    case CompactTask_TaskType.SHARED_BUFFER:
      return "SHARED_BUFFER";
    case CompactTask_TaskType.TTL:
      return "TTL";
    case CompactTask_TaskType.UNRECOGNIZED:
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

/** Config info of compaction group. */
export interface CompactionGroup {
  id: number;
  compactionConfig: CompactionConfig | undefined;
}

/**
 * Complete info of compaction group.
 * The info is the aggregate of `HummockVersion` and `CompactionGroupConfig`
 */
export interface CompactionGroupInfo {
  id: number;
  parentId: number;
  memberTableIds: number[];
  compactionConfig: CompactionConfig | undefined;
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
  tableStatsChange: { [key: number]: TableStats };
}

export interface ReportCompactionTasksRequest_TableStatsChangeEntry {
  key: number;
  value: TableStats | undefined;
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

/** The measurement of the workload on a compactor to determine whether it is idle. */
export interface CompactorWorkload {
  cpu: number;
}

export interface CompactorHeartbeatRequest {
  contextId: number;
  progress: CompactTaskProgress[];
  workload: CompactorWorkload | undefined;
}

export interface CompactorHeartbeatResponse {
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

export interface ListVersionDeltasRequest {
  startId: number;
  numLimit: number;
  committedEpochLimit: number;
}

export interface ListVersionDeltasResponse {
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

export interface InitMetadataForReplayRequest {
  tables: Table[];
  compactionGroups: CompactionGroupInfo[];
}

export interface InitMetadataForReplayResponse {
}

export interface ReplayVersionDeltaRequest {
  versionDelta: HummockVersionDelta | undefined;
}

export interface ReplayVersionDeltaResponse {
  version: HummockVersion | undefined;
  modifiedCompactionGroups: number[];
}

export interface TriggerCompactionDeterministicRequest {
  versionId: number;
  compactionGroups: number[];
}

export interface TriggerCompactionDeterministicResponse {
}

export interface DisableCommitEpochRequest {
}

export interface DisableCommitEpochResponse {
  currentVersion: HummockVersion | undefined;
}

export interface RiseCtlListCompactionGroupRequest {
}

export interface RiseCtlListCompactionGroupResponse {
  status: Status | undefined;
  compactionGroups: CompactionGroupInfo[];
}

export interface RiseCtlUpdateCompactionConfigRequest {
  compactionGroupIds: number[];
  configs: RiseCtlUpdateCompactionConfigRequest_MutableConfig[];
}

export interface RiseCtlUpdateCompactionConfigRequest_MutableConfig {
  mutableConfig?:
    | { $case: "maxBytesForLevelBase"; maxBytesForLevelBase: number }
    | { $case: "maxBytesForLevelMultiplier"; maxBytesForLevelMultiplier: number }
    | { $case: "maxCompactionBytes"; maxCompactionBytes: number }
    | { $case: "subLevelMaxCompactionBytes"; subLevelMaxCompactionBytes: number }
    | { $case: "level0TierCompactFileNumber"; level0TierCompactFileNumber: number }
    | { $case: "targetFileSizeBase"; targetFileSizeBase: number }
    | { $case: "compactionFilterMask"; compactionFilterMask: number }
    | { $case: "maxSubCompaction"; maxSubCompaction: number };
}

export interface RiseCtlUpdateCompactionConfigResponse {
  status: Status | undefined;
}

export interface SetCompactorRuntimeConfigRequest {
  contextId: number;
  config: CompactorRuntimeConfig | undefined;
}

export interface SetCompactorRuntimeConfigResponse {
}

export interface PinVersionRequest {
  contextId: number;
}

export interface PinVersionResponse {
  pinnedVersion: HummockVersion | undefined;
}

export interface CompactionConfig {
  maxBytesForLevelBase: number;
  maxLevel: number;
  maxBytesForLevelMultiplier: number;
  maxCompactionBytes: number;
  subLevelMaxCompactionBytes: number;
  level0TierCompactFileNumber: number;
  compactionMode: CompactionConfig_CompactionMode;
  compressionAlgorithm: string[];
  targetFileSizeBase: number;
  compactionFilterMask: number;
  maxSubCompaction: number;
  maxSpaceReclaimBytes: number;
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

export interface TableStats {
  totalKeySize: number;
  totalValueSize: number;
  totalKeyCount: number;
}

export interface HummockVersionStats {
  hummockVersionId: number;
  tableStats: { [key: number]: TableStats };
}

export interface HummockVersionStats_TableStatsEntry {
  key: number;
  value: TableStats | undefined;
}

export interface GetScaleCompactorRequest {
}

export interface GetScaleCompactorResponse {
  suggestCores: number;
  runningCores: number;
  totalCores: number;
  waitingCompactionBytes: number;
}

function createBaseSstableInfo(): SstableInfo {
  return {
    id: 0,
    keyRange: undefined,
    fileSize: 0,
    tableIds: [],
    metaOffset: 0,
    staleKeyCount: 0,
    totalKeyCount: 0,
    divideVersion: 0,
    minEpoch: 0,
    maxEpoch: 0,
    uncompressedFileSize: 0,
  };
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
      divideVersion: isSet(object.divideVersion) ? Number(object.divideVersion) : 0,
      minEpoch: isSet(object.minEpoch) ? Number(object.minEpoch) : 0,
      maxEpoch: isSet(object.maxEpoch) ? Number(object.maxEpoch) : 0,
      uncompressedFileSize: isSet(object.uncompressedFileSize) ? Number(object.uncompressedFileSize) : 0,
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
    message.divideVersion !== undefined && (obj.divideVersion = Math.round(message.divideVersion));
    message.minEpoch !== undefined && (obj.minEpoch = Math.round(message.minEpoch));
    message.maxEpoch !== undefined && (obj.maxEpoch = Math.round(message.maxEpoch));
    message.uncompressedFileSize !== undefined && (obj.uncompressedFileSize = Math.round(message.uncompressedFileSize));
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
    message.divideVersion = object.divideVersion ?? 0;
    message.minEpoch = object.minEpoch ?? 0;
    message.maxEpoch = object.maxEpoch ?? 0;
    message.uncompressedFileSize = object.uncompressedFileSize ?? 0;
    return message;
  },
};

function createBaseOverlappingLevel(): OverlappingLevel {
  return { subLevels: [], totalFileSize: 0, uncompressedFileSize: 0 };
}

export const OverlappingLevel = {
  fromJSON(object: any): OverlappingLevel {
    return {
      subLevels: Array.isArray(object?.subLevels) ? object.subLevels.map((e: any) => Level.fromJSON(e)) : [],
      totalFileSize: isSet(object.totalFileSize) ? Number(object.totalFileSize) : 0,
      uncompressedFileSize: isSet(object.uncompressedFileSize) ? Number(object.uncompressedFileSize) : 0,
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
    message.uncompressedFileSize !== undefined && (obj.uncompressedFileSize = Math.round(message.uncompressedFileSize));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<OverlappingLevel>, I>>(object: I): OverlappingLevel {
    const message = createBaseOverlappingLevel();
    message.subLevels = object.subLevels?.map((e) => Level.fromPartial(e)) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    message.uncompressedFileSize = object.uncompressedFileSize ?? 0;
    return message;
  },
};

function createBaseLevel(): Level {
  return {
    levelIdx: 0,
    levelType: LevelType.UNSPECIFIED,
    tableInfos: [],
    totalFileSize: 0,
    subLevelId: 0,
    uncompressedFileSize: 0,
  };
}

export const Level = {
  fromJSON(object: any): Level {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      levelType: isSet(object.levelType) ? levelTypeFromJSON(object.levelType) : LevelType.UNSPECIFIED,
      tableInfos: Array.isArray(object?.tableInfos) ? object.tableInfos.map((e: any) => SstableInfo.fromJSON(e)) : [],
      totalFileSize: isSet(object.totalFileSize) ? Number(object.totalFileSize) : 0,
      subLevelId: isSet(object.subLevelId) ? Number(object.subLevelId) : 0,
      uncompressedFileSize: isSet(object.uncompressedFileSize) ? Number(object.uncompressedFileSize) : 0,
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
    message.uncompressedFileSize !== undefined && (obj.uncompressedFileSize = Math.round(message.uncompressedFileSize));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Level>, I>>(object: I): Level {
    const message = createBaseLevel();
    message.levelIdx = object.levelIdx ?? 0;
    message.levelType = object.levelType ?? LevelType.UNSPECIFIED;
    message.tableInfos = object.tableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    message.totalFileSize = object.totalFileSize ?? 0;
    message.subLevelId = object.subLevelId ?? 0;
    message.uncompressedFileSize = object.uncompressedFileSize ?? 0;
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

function createBaseIntraLevelDelta(): IntraLevelDelta {
  return { levelIdx: 0, l0SubLevelId: 0, removedTableIds: [], insertedTableInfos: [] };
}

export const IntraLevelDelta = {
  fromJSON(object: any): IntraLevelDelta {
    return {
      levelIdx: isSet(object.levelIdx) ? Number(object.levelIdx) : 0,
      l0SubLevelId: isSet(object.l0SubLevelId) ? Number(object.l0SubLevelId) : 0,
      removedTableIds: Array.isArray(object?.removedTableIds) ? object.removedTableIds.map((e: any) => Number(e)) : [],
      insertedTableInfos: Array.isArray(object?.insertedTableInfos)
        ? object.insertedTableInfos.map((e: any) => SstableInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: IntraLevelDelta): unknown {
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

  fromPartial<I extends Exact<DeepPartial<IntraLevelDelta>, I>>(object: I): IntraLevelDelta {
    const message = createBaseIntraLevelDelta();
    message.levelIdx = object.levelIdx ?? 0;
    message.l0SubLevelId = object.l0SubLevelId ?? 0;
    message.removedTableIds = object.removedTableIds?.map((e) => e) || [];
    message.insertedTableInfos = object.insertedTableInfos?.map((e) => SstableInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseGroupConstruct(): GroupConstruct {
  return { groupConfig: undefined, parentGroupId: 0, tableIds: [], groupId: 0 };
}

export const GroupConstruct = {
  fromJSON(object: any): GroupConstruct {
    return {
      groupConfig: isSet(object.groupConfig) ? CompactionConfig.fromJSON(object.groupConfig) : undefined,
      parentGroupId: isSet(object.parentGroupId) ? Number(object.parentGroupId) : 0,
      tableIds: Array.isArray(object?.tableIds) ? object.tableIds.map((e: any) => Number(e)) : [],
      groupId: isSet(object.groupId) ? Number(object.groupId) : 0,
    };
  },

  toJSON(message: GroupConstruct): unknown {
    const obj: any = {};
    message.groupConfig !== undefined &&
      (obj.groupConfig = message.groupConfig ? CompactionConfig.toJSON(message.groupConfig) : undefined);
    message.parentGroupId !== undefined && (obj.parentGroupId = Math.round(message.parentGroupId));
    if (message.tableIds) {
      obj.tableIds = message.tableIds.map((e) => Math.round(e));
    } else {
      obj.tableIds = [];
    }
    message.groupId !== undefined && (obj.groupId = Math.round(message.groupId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupConstruct>, I>>(object: I): GroupConstruct {
    const message = createBaseGroupConstruct();
    message.groupConfig = (object.groupConfig !== undefined && object.groupConfig !== null)
      ? CompactionConfig.fromPartial(object.groupConfig)
      : undefined;
    message.parentGroupId = object.parentGroupId ?? 0;
    message.tableIds = object.tableIds?.map((e) => e) || [];
    message.groupId = object.groupId ?? 0;
    return message;
  },
};

function createBaseGroupMetaChange(): GroupMetaChange {
  return { tableIdsAdd: [], tableIdsRemove: [] };
}

export const GroupMetaChange = {
  fromJSON(object: any): GroupMetaChange {
    return {
      tableIdsAdd: Array.isArray(object?.tableIdsAdd) ? object.tableIdsAdd.map((e: any) => Number(e)) : [],
      tableIdsRemove: Array.isArray(object?.tableIdsRemove) ? object.tableIdsRemove.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: GroupMetaChange): unknown {
    const obj: any = {};
    if (message.tableIdsAdd) {
      obj.tableIdsAdd = message.tableIdsAdd.map((e) => Math.round(e));
    } else {
      obj.tableIdsAdd = [];
    }
    if (message.tableIdsRemove) {
      obj.tableIdsRemove = message.tableIdsRemove.map((e) => Math.round(e));
    } else {
      obj.tableIdsRemove = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupMetaChange>, I>>(object: I): GroupMetaChange {
    const message = createBaseGroupMetaChange();
    message.tableIdsAdd = object.tableIdsAdd?.map((e) => e) || [];
    message.tableIdsRemove = object.tableIdsRemove?.map((e) => e) || [];
    return message;
  },
};

function createBaseGroupDestroy(): GroupDestroy {
  return {};
}

export const GroupDestroy = {
  fromJSON(_: any): GroupDestroy {
    return {};
  },

  toJSON(_: GroupDestroy): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupDestroy>, I>>(_: I): GroupDestroy {
    const message = createBaseGroupDestroy();
    return message;
  },
};

function createBaseGroupDelta(): GroupDelta {
  return { deltaType: undefined };
}

export const GroupDelta = {
  fromJSON(object: any): GroupDelta {
    return {
      deltaType: isSet(object.intraLevel)
        ? { $case: "intraLevel", intraLevel: IntraLevelDelta.fromJSON(object.intraLevel) }
        : isSet(object.groupConstruct)
        ? { $case: "groupConstruct", groupConstruct: GroupConstruct.fromJSON(object.groupConstruct) }
        : isSet(object.groupDestroy)
        ? { $case: "groupDestroy", groupDestroy: GroupDestroy.fromJSON(object.groupDestroy) }
        : isSet(object.groupMetaChange)
        ? { $case: "groupMetaChange", groupMetaChange: GroupMetaChange.fromJSON(object.groupMetaChange) }
        : undefined,
    };
  },

  toJSON(message: GroupDelta): unknown {
    const obj: any = {};
    message.deltaType?.$case === "intraLevel" && (obj.intraLevel = message.deltaType?.intraLevel
      ? IntraLevelDelta.toJSON(message.deltaType?.intraLevel)
      : undefined);
    message.deltaType?.$case === "groupConstruct" && (obj.groupConstruct = message.deltaType?.groupConstruct
      ? GroupConstruct.toJSON(message.deltaType?.groupConstruct)
      : undefined);
    message.deltaType?.$case === "groupDestroy" && (obj.groupDestroy = message.deltaType?.groupDestroy
      ? GroupDestroy.toJSON(message.deltaType?.groupDestroy)
      : undefined);
    message.deltaType?.$case === "groupMetaChange" && (obj.groupMetaChange = message.deltaType?.groupMetaChange
      ? GroupMetaChange.toJSON(message.deltaType?.groupMetaChange)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupDelta>, I>>(object: I): GroupDelta {
    const message = createBaseGroupDelta();
    if (
      object.deltaType?.$case === "intraLevel" &&
      object.deltaType?.intraLevel !== undefined &&
      object.deltaType?.intraLevel !== null
    ) {
      message.deltaType = { $case: "intraLevel", intraLevel: IntraLevelDelta.fromPartial(object.deltaType.intraLevel) };
    }
    if (
      object.deltaType?.$case === "groupConstruct" &&
      object.deltaType?.groupConstruct !== undefined &&
      object.deltaType?.groupConstruct !== null
    ) {
      message.deltaType = {
        $case: "groupConstruct",
        groupConstruct: GroupConstruct.fromPartial(object.deltaType.groupConstruct),
      };
    }
    if (
      object.deltaType?.$case === "groupDestroy" &&
      object.deltaType?.groupDestroy !== undefined &&
      object.deltaType?.groupDestroy !== null
    ) {
      message.deltaType = {
        $case: "groupDestroy",
        groupDestroy: GroupDestroy.fromPartial(object.deltaType.groupDestroy),
      };
    }
    if (
      object.deltaType?.$case === "groupMetaChange" &&
      object.deltaType?.groupMetaChange !== undefined &&
      object.deltaType?.groupMetaChange !== null
    ) {
      message.deltaType = {
        $case: "groupMetaChange",
        groupMetaChange: GroupMetaChange.fromPartial(object.deltaType.groupMetaChange),
      };
    }
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
  return { levels: [], l0: undefined, groupId: 0, parentGroupId: 0, memberTableIds: [] };
}

export const HummockVersion_Levels = {
  fromJSON(object: any): HummockVersion_Levels {
    return {
      levels: Array.isArray(object?.levels) ? object.levels.map((e: any) => Level.fromJSON(e)) : [],
      l0: isSet(object.l0) ? OverlappingLevel.fromJSON(object.l0) : undefined,
      groupId: isSet(object.groupId) ? Number(object.groupId) : 0,
      parentGroupId: isSet(object.parentGroupId) ? Number(object.parentGroupId) : 0,
      memberTableIds: Array.isArray(object?.memberTableIds) ? object.memberTableIds.map((e: any) => Number(e)) : [],
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
    message.groupId !== undefined && (obj.groupId = Math.round(message.groupId));
    message.parentGroupId !== undefined && (obj.parentGroupId = Math.round(message.parentGroupId));
    if (message.memberTableIds) {
      obj.memberTableIds = message.memberTableIds.map((e) => Math.round(e));
    } else {
      obj.memberTableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersion_Levels>, I>>(object: I): HummockVersion_Levels {
    const message = createBaseHummockVersion_Levels();
    message.levels = object.levels?.map((e) => Level.fromPartial(e)) || [];
    message.l0 = (object.l0 !== undefined && object.l0 !== null) ? OverlappingLevel.fromPartial(object.l0) : undefined;
    message.groupId = object.groupId ?? 0;
    message.parentGroupId = object.parentGroupId ?? 0;
    message.memberTableIds = object.memberTableIds?.map((e) => e) || [];
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
  return { id: 0, prevId: 0, groupDeltas: {}, maxCommittedEpoch: 0, safeEpoch: 0, trivialMove: false, gcSstIds: [] };
}

export const HummockVersionDelta = {
  fromJSON(object: any): HummockVersionDelta {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      prevId: isSet(object.prevId) ? Number(object.prevId) : 0,
      groupDeltas: isObject(object.groupDeltas)
        ? Object.entries(object.groupDeltas).reduce<{ [key: number]: HummockVersionDelta_GroupDeltas }>(
          (acc, [key, value]) => {
            acc[Number(key)] = HummockVersionDelta_GroupDeltas.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
      maxCommittedEpoch: isSet(object.maxCommittedEpoch) ? Number(object.maxCommittedEpoch) : 0,
      safeEpoch: isSet(object.safeEpoch) ? Number(object.safeEpoch) : 0,
      trivialMove: isSet(object.trivialMove) ? Boolean(object.trivialMove) : false,
      gcSstIds: Array.isArray(object?.gcSstIds)
        ? object.gcSstIds.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: HummockVersionDelta): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.prevId !== undefined && (obj.prevId = Math.round(message.prevId));
    obj.groupDeltas = {};
    if (message.groupDeltas) {
      Object.entries(message.groupDeltas).forEach(([k, v]) => {
        obj.groupDeltas[k] = HummockVersionDelta_GroupDeltas.toJSON(v);
      });
    }
    message.maxCommittedEpoch !== undefined && (obj.maxCommittedEpoch = Math.round(message.maxCommittedEpoch));
    message.safeEpoch !== undefined && (obj.safeEpoch = Math.round(message.safeEpoch));
    message.trivialMove !== undefined && (obj.trivialMove = message.trivialMove);
    if (message.gcSstIds) {
      obj.gcSstIds = message.gcSstIds.map((e) => Math.round(e));
    } else {
      obj.gcSstIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta>, I>>(object: I): HummockVersionDelta {
    const message = createBaseHummockVersionDelta();
    message.id = object.id ?? 0;
    message.prevId = object.prevId ?? 0;
    message.groupDeltas = Object.entries(object.groupDeltas ?? {}).reduce<
      { [key: number]: HummockVersionDelta_GroupDeltas }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = HummockVersionDelta_GroupDeltas.fromPartial(value);
      }
      return acc;
    }, {});
    message.maxCommittedEpoch = object.maxCommittedEpoch ?? 0;
    message.safeEpoch = object.safeEpoch ?? 0;
    message.trivialMove = object.trivialMove ?? false;
    message.gcSstIds = object.gcSstIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseHummockVersionDelta_GroupDeltas(): HummockVersionDelta_GroupDeltas {
  return { groupDeltas: [] };
}

export const HummockVersionDelta_GroupDeltas = {
  fromJSON(object: any): HummockVersionDelta_GroupDeltas {
    return {
      groupDeltas: Array.isArray(object?.groupDeltas) ? object.groupDeltas.map((e: any) => GroupDelta.fromJSON(e)) : [],
    };
  },

  toJSON(message: HummockVersionDelta_GroupDeltas): unknown {
    const obj: any = {};
    if (message.groupDeltas) {
      obj.groupDeltas = message.groupDeltas.map((e) => e ? GroupDelta.toJSON(e) : undefined);
    } else {
      obj.groupDeltas = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta_GroupDeltas>, I>>(
    object: I,
  ): HummockVersionDelta_GroupDeltas {
    const message = createBaseHummockVersionDelta_GroupDeltas();
    message.groupDeltas = object.groupDeltas?.map((e) => GroupDelta.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHummockVersionDelta_GroupDeltasEntry(): HummockVersionDelta_GroupDeltasEntry {
  return { key: 0, value: undefined };
}

export const HummockVersionDelta_GroupDeltasEntry = {
  fromJSON(object: any): HummockVersionDelta_GroupDeltasEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? HummockVersionDelta_GroupDeltas.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: HummockVersionDelta_GroupDeltasEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? HummockVersionDelta_GroupDeltas.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionDelta_GroupDeltasEntry>, I>>(
    object: I,
  ): HummockVersionDelta_GroupDeltasEntry {
    const message = createBaseHummockVersionDelta_GroupDeltasEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? HummockVersionDelta_GroupDeltas.fromPartial(object.value)
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

function createBaseVersionUpdatePayload(): VersionUpdatePayload {
  return { payload: undefined };
}

export const VersionUpdatePayload = {
  fromJSON(object: any): VersionUpdatePayload {
    return {
      payload: isSet(object.versionDeltas)
        ? { $case: "versionDeltas", versionDeltas: HummockVersionDeltas.fromJSON(object.versionDeltas) }
        : isSet(object.pinnedVersion)
        ? { $case: "pinnedVersion", pinnedVersion: HummockVersion.fromJSON(object.pinnedVersion) }
        : undefined,
    };
  },

  toJSON(message: VersionUpdatePayload): unknown {
    const obj: any = {};
    message.payload?.$case === "versionDeltas" && (obj.versionDeltas = message.payload?.versionDeltas
      ? HummockVersionDeltas.toJSON(message.payload?.versionDeltas)
      : undefined);
    message.payload?.$case === "pinnedVersion" && (obj.pinnedVersion = message.payload?.pinnedVersion
      ? HummockVersion.toJSON(message.payload?.pinnedVersion)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<VersionUpdatePayload>, I>>(object: I): VersionUpdatePayload {
    const message = createBaseVersionUpdatePayload();
    if (
      object.payload?.$case === "versionDeltas" &&
      object.payload?.versionDeltas !== undefined &&
      object.payload?.versionDeltas !== null
    ) {
      message.payload = {
        $case: "versionDeltas",
        versionDeltas: HummockVersionDeltas.fromPartial(object.payload.versionDeltas),
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

function createBasePinSpecificSnapshotRequest(): PinSpecificSnapshotRequest {
  return { contextId: 0, epoch: 0 };
}

export const PinSpecificSnapshotRequest = {
  fromJSON(object: any): PinSpecificSnapshotRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: PinSpecificSnapshotRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinSpecificSnapshotRequest>, I>>(object: I): PinSpecificSnapshotRequest {
    const message = createBasePinSpecificSnapshotRequest();
    message.contextId = object.contextId ?? 0;
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseGetAssignedCompactTaskNumRequest(): GetAssignedCompactTaskNumRequest {
  return {};
}

export const GetAssignedCompactTaskNumRequest = {
  fromJSON(_: any): GetAssignedCompactTaskNumRequest {
    return {};
  },

  toJSON(_: GetAssignedCompactTaskNumRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetAssignedCompactTaskNumRequest>, I>>(
    _: I,
  ): GetAssignedCompactTaskNumRequest {
    const message = createBaseGetAssignedCompactTaskNumRequest();
    return message;
  },
};

function createBaseGetAssignedCompactTaskNumResponse(): GetAssignedCompactTaskNumResponse {
  return { numTasks: 0 };
}

export const GetAssignedCompactTaskNumResponse = {
  fromJSON(object: any): GetAssignedCompactTaskNumResponse {
    return { numTasks: isSet(object.numTasks) ? Number(object.numTasks) : 0 };
  },

  toJSON(message: GetAssignedCompactTaskNumResponse): unknown {
    const obj: any = {};
    message.numTasks !== undefined && (obj.numTasks = Math.round(message.numTasks));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetAssignedCompactTaskNumResponse>, I>>(
    object: I,
  ): GetAssignedCompactTaskNumResponse {
    const message = createBaseGetAssignedCompactTaskNumResponse();
    message.numTasks = object.numTasks ?? 0;
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
  return { left: new Uint8Array(), right: new Uint8Array(), rightExclusive: false };
}

export const KeyRange = {
  fromJSON(object: any): KeyRange {
    return {
      left: isSet(object.left) ? bytesFromBase64(object.left) : new Uint8Array(),
      right: isSet(object.right) ? bytesFromBase64(object.right) : new Uint8Array(),
      rightExclusive: isSet(object.rightExclusive) ? Boolean(object.rightExclusive) : false,
    };
  },

  toJSON(message: KeyRange): unknown {
    const obj: any = {};
    message.left !== undefined &&
      (obj.left = base64FromBytes(message.left !== undefined ? message.left : new Uint8Array()));
    message.right !== undefined &&
      (obj.right = base64FromBytes(message.right !== undefined ? message.right : new Uint8Array()));
    message.rightExclusive !== undefined && (obj.rightExclusive = message.rightExclusive);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<KeyRange>, I>>(object: I): KeyRange {
    const message = createBaseKeyRange();
    message.left = object.left ?? new Uint8Array();
    message.right = object.right ?? new Uint8Array();
    message.rightExclusive = object.rightExclusive ?? false;
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
    taskType: CompactTask_TaskType.TYPE_UNSPECIFIED,
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
      taskType: isSet(object.taskType)
        ? compactTask_TaskTypeFromJSON(object.taskType)
        : CompactTask_TaskType.TYPE_UNSPECIFIED,
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
    message.taskType !== undefined && (obj.taskType = compactTask_TaskTypeToJSON(message.taskType));
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
    message.taskType = object.taskType ?? CompactTask_TaskType.TYPE_UNSPECIFIED;
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
  return { id: 0, compactionConfig: undefined };
}

export const CompactionGroup = {
  fromJSON(object: any): CompactionGroup {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      compactionConfig: isSet(object.compactionConfig) ? CompactionConfig.fromJSON(object.compactionConfig) : undefined,
    };
  },

  toJSON(message: CompactionGroup): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.compactionConfig !== undefined &&
      (obj.compactionConfig = message.compactionConfig ? CompactionConfig.toJSON(message.compactionConfig) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionGroup>, I>>(object: I): CompactionGroup {
    const message = createBaseCompactionGroup();
    message.id = object.id ?? 0;
    message.compactionConfig = (object.compactionConfig !== undefined && object.compactionConfig !== null)
      ? CompactionConfig.fromPartial(object.compactionConfig)
      : undefined;
    return message;
  },
};

function createBaseCompactionGroupInfo(): CompactionGroupInfo {
  return { id: 0, parentId: 0, memberTableIds: [], compactionConfig: undefined };
}

export const CompactionGroupInfo = {
  fromJSON(object: any): CompactionGroupInfo {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      parentId: isSet(object.parentId) ? Number(object.parentId) : 0,
      memberTableIds: Array.isArray(object?.memberTableIds) ? object.memberTableIds.map((e: any) => Number(e)) : [],
      compactionConfig: isSet(object.compactionConfig) ? CompactionConfig.fromJSON(object.compactionConfig) : undefined,
    };
  },

  toJSON(message: CompactionGroupInfo): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.parentId !== undefined && (obj.parentId = Math.round(message.parentId));
    if (message.memberTableIds) {
      obj.memberTableIds = message.memberTableIds.map((e) => Math.round(e));
    } else {
      obj.memberTableIds = [];
    }
    message.compactionConfig !== undefined &&
      (obj.compactionConfig = message.compactionConfig ? CompactionConfig.toJSON(message.compactionConfig) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionGroupInfo>, I>>(object: I): CompactionGroupInfo {
    const message = createBaseCompactionGroupInfo();
    message.id = object.id ?? 0;
    message.parentId = object.parentId ?? 0;
    message.memberTableIds = object.memberTableIds?.map((e) => e) || [];
    message.compactionConfig = (object.compactionConfig !== undefined && object.compactionConfig !== null)
      ? CompactionConfig.fromPartial(object.compactionConfig)
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
  return { contextId: 0, compactTask: undefined, tableStatsChange: {} };
}

export const ReportCompactionTasksRequest = {
  fromJSON(object: any): ReportCompactionTasksRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      compactTask: isSet(object.compactTask) ? CompactTask.fromJSON(object.compactTask) : undefined,
      tableStatsChange: isObject(object.tableStatsChange)
        ? Object.entries(object.tableStatsChange).reduce<{ [key: number]: TableStats }>((acc, [key, value]) => {
          acc[Number(key)] = TableStats.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: ReportCompactionTasksRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.compactTask !== undefined &&
      (obj.compactTask = message.compactTask ? CompactTask.toJSON(message.compactTask) : undefined);
    obj.tableStatsChange = {};
    if (message.tableStatsChange) {
      Object.entries(message.tableStatsChange).forEach(([k, v]) => {
        obj.tableStatsChange[k] = TableStats.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTasksRequest>, I>>(object: I): ReportCompactionTasksRequest {
    const message = createBaseReportCompactionTasksRequest();
    message.contextId = object.contextId ?? 0;
    message.compactTask = (object.compactTask !== undefined && object.compactTask !== null)
      ? CompactTask.fromPartial(object.compactTask)
      : undefined;
    message.tableStatsChange = Object.entries(object.tableStatsChange ?? {}).reduce<{ [key: number]: TableStats }>(
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

function createBaseReportCompactionTasksRequest_TableStatsChangeEntry(): ReportCompactionTasksRequest_TableStatsChangeEntry {
  return { key: 0, value: undefined };
}

export const ReportCompactionTasksRequest_TableStatsChangeEntry = {
  fromJSON(object: any): ReportCompactionTasksRequest_TableStatsChangeEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableStats.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: ReportCompactionTasksRequest_TableStatsChangeEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? TableStats.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReportCompactionTasksRequest_TableStatsChangeEntry>, I>>(
    object: I,
  ): ReportCompactionTasksRequest_TableStatsChangeEntry {
    const message = createBaseReportCompactionTasksRequest_TableStatsChangeEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableStats.fromPartial(object.value)
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

function createBaseCompactorWorkload(): CompactorWorkload {
  return { cpu: 0 };
}

export const CompactorWorkload = {
  fromJSON(object: any): CompactorWorkload {
    return { cpu: isSet(object.cpu) ? Number(object.cpu) : 0 };
  },

  toJSON(message: CompactorWorkload): unknown {
    const obj: any = {};
    message.cpu !== undefined && (obj.cpu = Math.round(message.cpu));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactorWorkload>, I>>(object: I): CompactorWorkload {
    const message = createBaseCompactorWorkload();
    message.cpu = object.cpu ?? 0;
    return message;
  },
};

function createBaseCompactorHeartbeatRequest(): CompactorHeartbeatRequest {
  return { contextId: 0, progress: [], workload: undefined };
}

export const CompactorHeartbeatRequest = {
  fromJSON(object: any): CompactorHeartbeatRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      progress: Array.isArray(object?.progress) ? object.progress.map((e: any) => CompactTaskProgress.fromJSON(e)) : [],
      workload: isSet(object.workload) ? CompactorWorkload.fromJSON(object.workload) : undefined,
    };
  },

  toJSON(message: CompactorHeartbeatRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    if (message.progress) {
      obj.progress = message.progress.map((e) => e ? CompactTaskProgress.toJSON(e) : undefined);
    } else {
      obj.progress = [];
    }
    message.workload !== undefined &&
      (obj.workload = message.workload ? CompactorWorkload.toJSON(message.workload) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactorHeartbeatRequest>, I>>(object: I): CompactorHeartbeatRequest {
    const message = createBaseCompactorHeartbeatRequest();
    message.contextId = object.contextId ?? 0;
    message.progress = object.progress?.map((e) => CompactTaskProgress.fromPartial(e)) || [];
    message.workload = (object.workload !== undefined && object.workload !== null)
      ? CompactorWorkload.fromPartial(object.workload)
      : undefined;
    return message;
  },
};

function createBaseCompactorHeartbeatResponse(): CompactorHeartbeatResponse {
  return { status: undefined };
}

export const CompactorHeartbeatResponse = {
  fromJSON(object: any): CompactorHeartbeatResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: CompactorHeartbeatResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactorHeartbeatResponse>, I>>(object: I): CompactorHeartbeatResponse {
    const message = createBaseCompactorHeartbeatResponse();
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

function createBaseListVersionDeltasRequest(): ListVersionDeltasRequest {
  return { startId: 0, numLimit: 0, committedEpochLimit: 0 };
}

export const ListVersionDeltasRequest = {
  fromJSON(object: any): ListVersionDeltasRequest {
    return {
      startId: isSet(object.startId) ? Number(object.startId) : 0,
      numLimit: isSet(object.numLimit) ? Number(object.numLimit) : 0,
      committedEpochLimit: isSet(object.committedEpochLimit) ? Number(object.committedEpochLimit) : 0,
    };
  },

  toJSON(message: ListVersionDeltasRequest): unknown {
    const obj: any = {};
    message.startId !== undefined && (obj.startId = Math.round(message.startId));
    message.numLimit !== undefined && (obj.numLimit = Math.round(message.numLimit));
    message.committedEpochLimit !== undefined && (obj.committedEpochLimit = Math.round(message.committedEpochLimit));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListVersionDeltasRequest>, I>>(object: I): ListVersionDeltasRequest {
    const message = createBaseListVersionDeltasRequest();
    message.startId = object.startId ?? 0;
    message.numLimit = object.numLimit ?? 0;
    message.committedEpochLimit = object.committedEpochLimit ?? 0;
    return message;
  },
};

function createBaseListVersionDeltasResponse(): ListVersionDeltasResponse {
  return { versionDeltas: undefined };
}

export const ListVersionDeltasResponse = {
  fromJSON(object: any): ListVersionDeltasResponse {
    return {
      versionDeltas: isSet(object.versionDeltas) ? HummockVersionDeltas.fromJSON(object.versionDeltas) : undefined,
    };
  },

  toJSON(message: ListVersionDeltasResponse): unknown {
    const obj: any = {};
    message.versionDeltas !== undefined &&
      (obj.versionDeltas = message.versionDeltas ? HummockVersionDeltas.toJSON(message.versionDeltas) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListVersionDeltasResponse>, I>>(object: I): ListVersionDeltasResponse {
    const message = createBaseListVersionDeltasResponse();
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

function createBaseInitMetadataForReplayRequest(): InitMetadataForReplayRequest {
  return { tables: [], compactionGroups: [] };
}

export const InitMetadataForReplayRequest = {
  fromJSON(object: any): InitMetadataForReplayRequest {
    return {
      tables: Array.isArray(object?.tables) ? object.tables.map((e: any) => Table.fromJSON(e)) : [],
      compactionGroups: Array.isArray(object?.compactionGroups)
        ? object.compactionGroups.map((e: any) => CompactionGroupInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: InitMetadataForReplayRequest): unknown {
    const obj: any = {};
    if (message.tables) {
      obj.tables = message.tables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.tables = [];
    }
    if (message.compactionGroups) {
      obj.compactionGroups = message.compactionGroups.map((e) => e ? CompactionGroupInfo.toJSON(e) : undefined);
    } else {
      obj.compactionGroups = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InitMetadataForReplayRequest>, I>>(object: I): InitMetadataForReplayRequest {
    const message = createBaseInitMetadataForReplayRequest();
    message.tables = object.tables?.map((e) => Table.fromPartial(e)) || [];
    message.compactionGroups = object.compactionGroups?.map((e) => CompactionGroupInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseInitMetadataForReplayResponse(): InitMetadataForReplayResponse {
  return {};
}

export const InitMetadataForReplayResponse = {
  fromJSON(_: any): InitMetadataForReplayResponse {
    return {};
  },

  toJSON(_: InitMetadataForReplayResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InitMetadataForReplayResponse>, I>>(_: I): InitMetadataForReplayResponse {
    const message = createBaseInitMetadataForReplayResponse();
    return message;
  },
};

function createBaseReplayVersionDeltaRequest(): ReplayVersionDeltaRequest {
  return { versionDelta: undefined };
}

export const ReplayVersionDeltaRequest = {
  fromJSON(object: any): ReplayVersionDeltaRequest {
    return { versionDelta: isSet(object.versionDelta) ? HummockVersionDelta.fromJSON(object.versionDelta) : undefined };
  },

  toJSON(message: ReplayVersionDeltaRequest): unknown {
    const obj: any = {};
    message.versionDelta !== undefined &&
      (obj.versionDelta = message.versionDelta ? HummockVersionDelta.toJSON(message.versionDelta) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReplayVersionDeltaRequest>, I>>(object: I): ReplayVersionDeltaRequest {
    const message = createBaseReplayVersionDeltaRequest();
    message.versionDelta = (object.versionDelta !== undefined && object.versionDelta !== null)
      ? HummockVersionDelta.fromPartial(object.versionDelta)
      : undefined;
    return message;
  },
};

function createBaseReplayVersionDeltaResponse(): ReplayVersionDeltaResponse {
  return { version: undefined, modifiedCompactionGroups: [] };
}

export const ReplayVersionDeltaResponse = {
  fromJSON(object: any): ReplayVersionDeltaResponse {
    return {
      version: isSet(object.version) ? HummockVersion.fromJSON(object.version) : undefined,
      modifiedCompactionGroups: Array.isArray(object?.modifiedCompactionGroups)
        ? object.modifiedCompactionGroups.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: ReplayVersionDeltaResponse): unknown {
    const obj: any = {};
    message.version !== undefined &&
      (obj.version = message.version ? HummockVersion.toJSON(message.version) : undefined);
    if (message.modifiedCompactionGroups) {
      obj.modifiedCompactionGroups = message.modifiedCompactionGroups.map((e) => Math.round(e));
    } else {
      obj.modifiedCompactionGroups = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ReplayVersionDeltaResponse>, I>>(object: I): ReplayVersionDeltaResponse {
    const message = createBaseReplayVersionDeltaResponse();
    message.version = (object.version !== undefined && object.version !== null)
      ? HummockVersion.fromPartial(object.version)
      : undefined;
    message.modifiedCompactionGroups = object.modifiedCompactionGroups?.map((e) => e) || [];
    return message;
  },
};

function createBaseTriggerCompactionDeterministicRequest(): TriggerCompactionDeterministicRequest {
  return { versionId: 0, compactionGroups: [] };
}

export const TriggerCompactionDeterministicRequest = {
  fromJSON(object: any): TriggerCompactionDeterministicRequest {
    return {
      versionId: isSet(object.versionId) ? Number(object.versionId) : 0,
      compactionGroups: Array.isArray(object?.compactionGroups)
        ? object.compactionGroups.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: TriggerCompactionDeterministicRequest): unknown {
    const obj: any = {};
    message.versionId !== undefined && (obj.versionId = Math.round(message.versionId));
    if (message.compactionGroups) {
      obj.compactionGroups = message.compactionGroups.map((e) => Math.round(e));
    } else {
      obj.compactionGroups = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerCompactionDeterministicRequest>, I>>(
    object: I,
  ): TriggerCompactionDeterministicRequest {
    const message = createBaseTriggerCompactionDeterministicRequest();
    message.versionId = object.versionId ?? 0;
    message.compactionGroups = object.compactionGroups?.map((e) => e) || [];
    return message;
  },
};

function createBaseTriggerCompactionDeterministicResponse(): TriggerCompactionDeterministicResponse {
  return {};
}

export const TriggerCompactionDeterministicResponse = {
  fromJSON(_: any): TriggerCompactionDeterministicResponse {
    return {};
  },

  toJSON(_: TriggerCompactionDeterministicResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TriggerCompactionDeterministicResponse>, I>>(
    _: I,
  ): TriggerCompactionDeterministicResponse {
    const message = createBaseTriggerCompactionDeterministicResponse();
    return message;
  },
};

function createBaseDisableCommitEpochRequest(): DisableCommitEpochRequest {
  return {};
}

export const DisableCommitEpochRequest = {
  fromJSON(_: any): DisableCommitEpochRequest {
    return {};
  },

  toJSON(_: DisableCommitEpochRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DisableCommitEpochRequest>, I>>(_: I): DisableCommitEpochRequest {
    const message = createBaseDisableCommitEpochRequest();
    return message;
  },
};

function createBaseDisableCommitEpochResponse(): DisableCommitEpochResponse {
  return { currentVersion: undefined };
}

export const DisableCommitEpochResponse = {
  fromJSON(object: any): DisableCommitEpochResponse {
    return {
      currentVersion: isSet(object.currentVersion) ? HummockVersion.fromJSON(object.currentVersion) : undefined,
    };
  },

  toJSON(message: DisableCommitEpochResponse): unknown {
    const obj: any = {};
    message.currentVersion !== undefined &&
      (obj.currentVersion = message.currentVersion ? HummockVersion.toJSON(message.currentVersion) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DisableCommitEpochResponse>, I>>(object: I): DisableCommitEpochResponse {
    const message = createBaseDisableCommitEpochResponse();
    message.currentVersion = (object.currentVersion !== undefined && object.currentVersion !== null)
      ? HummockVersion.fromPartial(object.currentVersion)
      : undefined;
    return message;
  },
};

function createBaseRiseCtlListCompactionGroupRequest(): RiseCtlListCompactionGroupRequest {
  return {};
}

export const RiseCtlListCompactionGroupRequest = {
  fromJSON(_: any): RiseCtlListCompactionGroupRequest {
    return {};
  },

  toJSON(_: RiseCtlListCompactionGroupRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlListCompactionGroupRequest>, I>>(
    _: I,
  ): RiseCtlListCompactionGroupRequest {
    const message = createBaseRiseCtlListCompactionGroupRequest();
    return message;
  },
};

function createBaseRiseCtlListCompactionGroupResponse(): RiseCtlListCompactionGroupResponse {
  return { status: undefined, compactionGroups: [] };
}

export const RiseCtlListCompactionGroupResponse = {
  fromJSON(object: any): RiseCtlListCompactionGroupResponse {
    return {
      status: isSet(object.status) ? Status.fromJSON(object.status) : undefined,
      compactionGroups: Array.isArray(object?.compactionGroups)
        ? object.compactionGroups.map((e: any) => CompactionGroupInfo.fromJSON(e))
        : [],
    };
  },

  toJSON(message: RiseCtlListCompactionGroupResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    if (message.compactionGroups) {
      obj.compactionGroups = message.compactionGroups.map((e) => e ? CompactionGroupInfo.toJSON(e) : undefined);
    } else {
      obj.compactionGroups = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlListCompactionGroupResponse>, I>>(
    object: I,
  ): RiseCtlListCompactionGroupResponse {
    const message = createBaseRiseCtlListCompactionGroupResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    message.compactionGroups = object.compactionGroups?.map((e) => CompactionGroupInfo.fromPartial(e)) || [];
    return message;
  },
};

function createBaseRiseCtlUpdateCompactionConfigRequest(): RiseCtlUpdateCompactionConfigRequest {
  return { compactionGroupIds: [], configs: [] };
}

export const RiseCtlUpdateCompactionConfigRequest = {
  fromJSON(object: any): RiseCtlUpdateCompactionConfigRequest {
    return {
      compactionGroupIds: Array.isArray(object?.compactionGroupIds)
        ? object.compactionGroupIds.map((e: any) => Number(e))
        : [],
      configs: Array.isArray(object?.configs)
        ? object.configs.map((e: any) => RiseCtlUpdateCompactionConfigRequest_MutableConfig.fromJSON(e))
        : [],
    };
  },

  toJSON(message: RiseCtlUpdateCompactionConfigRequest): unknown {
    const obj: any = {};
    if (message.compactionGroupIds) {
      obj.compactionGroupIds = message.compactionGroupIds.map((e) => Math.round(e));
    } else {
      obj.compactionGroupIds = [];
    }
    if (message.configs) {
      obj.configs = message.configs.map((e) =>
        e ? RiseCtlUpdateCompactionConfigRequest_MutableConfig.toJSON(e) : undefined
      );
    } else {
      obj.configs = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlUpdateCompactionConfigRequest>, I>>(
    object: I,
  ): RiseCtlUpdateCompactionConfigRequest {
    const message = createBaseRiseCtlUpdateCompactionConfigRequest();
    message.compactionGroupIds = object.compactionGroupIds?.map((e) => e) || [];
    message.configs = object.configs?.map((e) => RiseCtlUpdateCompactionConfigRequest_MutableConfig.fromPartial(e)) ||
      [];
    return message;
  },
};

function createBaseRiseCtlUpdateCompactionConfigRequest_MutableConfig(): RiseCtlUpdateCompactionConfigRequest_MutableConfig {
  return { mutableConfig: undefined };
}

export const RiseCtlUpdateCompactionConfigRequest_MutableConfig = {
  fromJSON(object: any): RiseCtlUpdateCompactionConfigRequest_MutableConfig {
    return {
      mutableConfig: isSet(object.maxBytesForLevelBase)
        ? { $case: "maxBytesForLevelBase", maxBytesForLevelBase: Number(object.maxBytesForLevelBase) }
        : isSet(object.maxBytesForLevelMultiplier)
        ? { $case: "maxBytesForLevelMultiplier", maxBytesForLevelMultiplier: Number(object.maxBytesForLevelMultiplier) }
        : isSet(object.maxCompactionBytes)
        ? { $case: "maxCompactionBytes", maxCompactionBytes: Number(object.maxCompactionBytes) }
        : isSet(object.subLevelMaxCompactionBytes)
        ? { $case: "subLevelMaxCompactionBytes", subLevelMaxCompactionBytes: Number(object.subLevelMaxCompactionBytes) }
        : isSet(object.level0TierCompactFileNumber)
        ? {
          $case: "level0TierCompactFileNumber",
          level0TierCompactFileNumber: Number(object.level0TierCompactFileNumber),
        }
        : isSet(object.targetFileSizeBase)
        ? { $case: "targetFileSizeBase", targetFileSizeBase: Number(object.targetFileSizeBase) }
        : isSet(object.compactionFilterMask)
        ? { $case: "compactionFilterMask", compactionFilterMask: Number(object.compactionFilterMask) }
        : isSet(object.maxSubCompaction)
        ? { $case: "maxSubCompaction", maxSubCompaction: Number(object.maxSubCompaction) }
        : undefined,
    };
  },

  toJSON(message: RiseCtlUpdateCompactionConfigRequest_MutableConfig): unknown {
    const obj: any = {};
    message.mutableConfig?.$case === "maxBytesForLevelBase" &&
      (obj.maxBytesForLevelBase = Math.round(message.mutableConfig?.maxBytesForLevelBase));
    message.mutableConfig?.$case === "maxBytesForLevelMultiplier" &&
      (obj.maxBytesForLevelMultiplier = Math.round(message.mutableConfig?.maxBytesForLevelMultiplier));
    message.mutableConfig?.$case === "maxCompactionBytes" &&
      (obj.maxCompactionBytes = Math.round(message.mutableConfig?.maxCompactionBytes));
    message.mutableConfig?.$case === "subLevelMaxCompactionBytes" &&
      (obj.subLevelMaxCompactionBytes = Math.round(message.mutableConfig?.subLevelMaxCompactionBytes));
    message.mutableConfig?.$case === "level0TierCompactFileNumber" &&
      (obj.level0TierCompactFileNumber = Math.round(message.mutableConfig?.level0TierCompactFileNumber));
    message.mutableConfig?.$case === "targetFileSizeBase" &&
      (obj.targetFileSizeBase = Math.round(message.mutableConfig?.targetFileSizeBase));
    message.mutableConfig?.$case === "compactionFilterMask" &&
      (obj.compactionFilterMask = Math.round(message.mutableConfig?.compactionFilterMask));
    message.mutableConfig?.$case === "maxSubCompaction" &&
      (obj.maxSubCompaction = Math.round(message.mutableConfig?.maxSubCompaction));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlUpdateCompactionConfigRequest_MutableConfig>, I>>(
    object: I,
  ): RiseCtlUpdateCompactionConfigRequest_MutableConfig {
    const message = createBaseRiseCtlUpdateCompactionConfigRequest_MutableConfig();
    if (
      object.mutableConfig?.$case === "maxBytesForLevelBase" &&
      object.mutableConfig?.maxBytesForLevelBase !== undefined &&
      object.mutableConfig?.maxBytesForLevelBase !== null
    ) {
      message.mutableConfig = {
        $case: "maxBytesForLevelBase",
        maxBytesForLevelBase: object.mutableConfig.maxBytesForLevelBase,
      };
    }
    if (
      object.mutableConfig?.$case === "maxBytesForLevelMultiplier" &&
      object.mutableConfig?.maxBytesForLevelMultiplier !== undefined &&
      object.mutableConfig?.maxBytesForLevelMultiplier !== null
    ) {
      message.mutableConfig = {
        $case: "maxBytesForLevelMultiplier",
        maxBytesForLevelMultiplier: object.mutableConfig.maxBytesForLevelMultiplier,
      };
    }
    if (
      object.mutableConfig?.$case === "maxCompactionBytes" &&
      object.mutableConfig?.maxCompactionBytes !== undefined &&
      object.mutableConfig?.maxCompactionBytes !== null
    ) {
      message.mutableConfig = {
        $case: "maxCompactionBytes",
        maxCompactionBytes: object.mutableConfig.maxCompactionBytes,
      };
    }
    if (
      object.mutableConfig?.$case === "subLevelMaxCompactionBytes" &&
      object.mutableConfig?.subLevelMaxCompactionBytes !== undefined &&
      object.mutableConfig?.subLevelMaxCompactionBytes !== null
    ) {
      message.mutableConfig = {
        $case: "subLevelMaxCompactionBytes",
        subLevelMaxCompactionBytes: object.mutableConfig.subLevelMaxCompactionBytes,
      };
    }
    if (
      object.mutableConfig?.$case === "level0TierCompactFileNumber" &&
      object.mutableConfig?.level0TierCompactFileNumber !== undefined &&
      object.mutableConfig?.level0TierCompactFileNumber !== null
    ) {
      message.mutableConfig = {
        $case: "level0TierCompactFileNumber",
        level0TierCompactFileNumber: object.mutableConfig.level0TierCompactFileNumber,
      };
    }
    if (
      object.mutableConfig?.$case === "targetFileSizeBase" &&
      object.mutableConfig?.targetFileSizeBase !== undefined &&
      object.mutableConfig?.targetFileSizeBase !== null
    ) {
      message.mutableConfig = {
        $case: "targetFileSizeBase",
        targetFileSizeBase: object.mutableConfig.targetFileSizeBase,
      };
    }
    if (
      object.mutableConfig?.$case === "compactionFilterMask" &&
      object.mutableConfig?.compactionFilterMask !== undefined &&
      object.mutableConfig?.compactionFilterMask !== null
    ) {
      message.mutableConfig = {
        $case: "compactionFilterMask",
        compactionFilterMask: object.mutableConfig.compactionFilterMask,
      };
    }
    if (
      object.mutableConfig?.$case === "maxSubCompaction" &&
      object.mutableConfig?.maxSubCompaction !== undefined &&
      object.mutableConfig?.maxSubCompaction !== null
    ) {
      message.mutableConfig = { $case: "maxSubCompaction", maxSubCompaction: object.mutableConfig.maxSubCompaction };
    }
    return message;
  },
};

function createBaseRiseCtlUpdateCompactionConfigResponse(): RiseCtlUpdateCompactionConfigResponse {
  return { status: undefined };
}

export const RiseCtlUpdateCompactionConfigResponse = {
  fromJSON(object: any): RiseCtlUpdateCompactionConfigResponse {
    return { status: isSet(object.status) ? Status.fromJSON(object.status) : undefined };
  },

  toJSON(message: RiseCtlUpdateCompactionConfigResponse): unknown {
    const obj: any = {};
    message.status !== undefined && (obj.status = message.status ? Status.toJSON(message.status) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RiseCtlUpdateCompactionConfigResponse>, I>>(
    object: I,
  ): RiseCtlUpdateCompactionConfigResponse {
    const message = createBaseRiseCtlUpdateCompactionConfigResponse();
    message.status = (object.status !== undefined && object.status !== null)
      ? Status.fromPartial(object.status)
      : undefined;
    return message;
  },
};

function createBaseSetCompactorRuntimeConfigRequest(): SetCompactorRuntimeConfigRequest {
  return { contextId: 0, config: undefined };
}

export const SetCompactorRuntimeConfigRequest = {
  fromJSON(object: any): SetCompactorRuntimeConfigRequest {
    return {
      contextId: isSet(object.contextId) ? Number(object.contextId) : 0,
      config: isSet(object.config) ? CompactorRuntimeConfig.fromJSON(object.config) : undefined,
    };
  },

  toJSON(message: SetCompactorRuntimeConfigRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    message.config !== undefined &&
      (obj.config = message.config ? CompactorRuntimeConfig.toJSON(message.config) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SetCompactorRuntimeConfigRequest>, I>>(
    object: I,
  ): SetCompactorRuntimeConfigRequest {
    const message = createBaseSetCompactorRuntimeConfigRequest();
    message.contextId = object.contextId ?? 0;
    message.config = (object.config !== undefined && object.config !== null)
      ? CompactorRuntimeConfig.fromPartial(object.config)
      : undefined;
    return message;
  },
};

function createBaseSetCompactorRuntimeConfigResponse(): SetCompactorRuntimeConfigResponse {
  return {};
}

export const SetCompactorRuntimeConfigResponse = {
  fromJSON(_: any): SetCompactorRuntimeConfigResponse {
    return {};
  },

  toJSON(_: SetCompactorRuntimeConfigResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SetCompactorRuntimeConfigResponse>, I>>(
    _: I,
  ): SetCompactorRuntimeConfigResponse {
    const message = createBaseSetCompactorRuntimeConfigResponse();
    return message;
  },
};

function createBasePinVersionRequest(): PinVersionRequest {
  return { contextId: 0 };
}

export const PinVersionRequest = {
  fromJSON(object: any): PinVersionRequest {
    return { contextId: isSet(object.contextId) ? Number(object.contextId) : 0 };
  },

  toJSON(message: PinVersionRequest): unknown {
    const obj: any = {};
    message.contextId !== undefined && (obj.contextId = Math.round(message.contextId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinVersionRequest>, I>>(object: I): PinVersionRequest {
    const message = createBasePinVersionRequest();
    message.contextId = object.contextId ?? 0;
    return message;
  },
};

function createBasePinVersionResponse(): PinVersionResponse {
  return { pinnedVersion: undefined };
}

export const PinVersionResponse = {
  fromJSON(object: any): PinVersionResponse {
    return { pinnedVersion: isSet(object.pinnedVersion) ? HummockVersion.fromJSON(object.pinnedVersion) : undefined };
  },

  toJSON(message: PinVersionResponse): unknown {
    const obj: any = {};
    message.pinnedVersion !== undefined &&
      (obj.pinnedVersion = message.pinnedVersion ? HummockVersion.toJSON(message.pinnedVersion) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PinVersionResponse>, I>>(object: I): PinVersionResponse {
    const message = createBasePinVersionResponse();
    message.pinnedVersion = (object.pinnedVersion !== undefined && object.pinnedVersion !== null)
      ? HummockVersion.fromPartial(object.pinnedVersion)
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
    level0TierCompactFileNumber: 0,
    compactionMode: CompactionConfig_CompactionMode.UNSPECIFIED,
    compressionAlgorithm: [],
    targetFileSizeBase: 0,
    compactionFilterMask: 0,
    maxSubCompaction: 0,
    maxSpaceReclaimBytes: 0,
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
      maxSpaceReclaimBytes: isSet(object.maxSpaceReclaimBytes) ? Number(object.maxSpaceReclaimBytes) : 0,
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
    message.maxSpaceReclaimBytes !== undefined && (obj.maxSpaceReclaimBytes = Math.round(message.maxSpaceReclaimBytes));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<CompactionConfig>, I>>(object: I): CompactionConfig {
    const message = createBaseCompactionConfig();
    message.maxBytesForLevelBase = object.maxBytesForLevelBase ?? 0;
    message.maxLevel = object.maxLevel ?? 0;
    message.maxBytesForLevelMultiplier = object.maxBytesForLevelMultiplier ?? 0;
    message.maxCompactionBytes = object.maxCompactionBytes ?? 0;
    message.subLevelMaxCompactionBytes = object.subLevelMaxCompactionBytes ?? 0;
    message.level0TierCompactFileNumber = object.level0TierCompactFileNumber ?? 0;
    message.compactionMode = object.compactionMode ?? CompactionConfig_CompactionMode.UNSPECIFIED;
    message.compressionAlgorithm = object.compressionAlgorithm?.map((e) => e) || [];
    message.targetFileSizeBase = object.targetFileSizeBase ?? 0;
    message.compactionFilterMask = object.compactionFilterMask ?? 0;
    message.maxSubCompaction = object.maxSubCompaction ?? 0;
    message.maxSpaceReclaimBytes = object.maxSpaceReclaimBytes ?? 0;
    return message;
  },
};

function createBaseTableStats(): TableStats {
  return { totalKeySize: 0, totalValueSize: 0, totalKeyCount: 0 };
}

export const TableStats = {
  fromJSON(object: any): TableStats {
    return {
      totalKeySize: isSet(object.totalKeySize) ? Number(object.totalKeySize) : 0,
      totalValueSize: isSet(object.totalValueSize) ? Number(object.totalValueSize) : 0,
      totalKeyCount: isSet(object.totalKeyCount) ? Number(object.totalKeyCount) : 0,
    };
  },

  toJSON(message: TableStats): unknown {
    const obj: any = {};
    message.totalKeySize !== undefined && (obj.totalKeySize = Math.round(message.totalKeySize));
    message.totalValueSize !== undefined && (obj.totalValueSize = Math.round(message.totalValueSize));
    message.totalKeyCount !== undefined && (obj.totalKeyCount = Math.round(message.totalKeyCount));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableStats>, I>>(object: I): TableStats {
    const message = createBaseTableStats();
    message.totalKeySize = object.totalKeySize ?? 0;
    message.totalValueSize = object.totalValueSize ?? 0;
    message.totalKeyCount = object.totalKeyCount ?? 0;
    return message;
  },
};

function createBaseHummockVersionStats(): HummockVersionStats {
  return { hummockVersionId: 0, tableStats: {} };
}

export const HummockVersionStats = {
  fromJSON(object: any): HummockVersionStats {
    return {
      hummockVersionId: isSet(object.hummockVersionId) ? Number(object.hummockVersionId) : 0,
      tableStats: isObject(object.tableStats)
        ? Object.entries(object.tableStats).reduce<{ [key: number]: TableStats }>((acc, [key, value]) => {
          acc[Number(key)] = TableStats.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: HummockVersionStats): unknown {
    const obj: any = {};
    message.hummockVersionId !== undefined && (obj.hummockVersionId = Math.round(message.hummockVersionId));
    obj.tableStats = {};
    if (message.tableStats) {
      Object.entries(message.tableStats).forEach(([k, v]) => {
        obj.tableStats[k] = TableStats.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionStats>, I>>(object: I): HummockVersionStats {
    const message = createBaseHummockVersionStats();
    message.hummockVersionId = object.hummockVersionId ?? 0;
    message.tableStats = Object.entries(object.tableStats ?? {}).reduce<{ [key: number]: TableStats }>(
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

function createBaseHummockVersionStats_TableStatsEntry(): HummockVersionStats_TableStatsEntry {
  return { key: 0, value: undefined };
}

export const HummockVersionStats_TableStatsEntry = {
  fromJSON(object: any): HummockVersionStats_TableStatsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? TableStats.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: HummockVersionStats_TableStatsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? TableStats.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HummockVersionStats_TableStatsEntry>, I>>(
    object: I,
  ): HummockVersionStats_TableStatsEntry {
    const message = createBaseHummockVersionStats_TableStatsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? TableStats.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseGetScaleCompactorRequest(): GetScaleCompactorRequest {
  return {};
}

export const GetScaleCompactorRequest = {
  fromJSON(_: any): GetScaleCompactorRequest {
    return {};
  },

  toJSON(_: GetScaleCompactorRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetScaleCompactorRequest>, I>>(_: I): GetScaleCompactorRequest {
    const message = createBaseGetScaleCompactorRequest();
    return message;
  },
};

function createBaseGetScaleCompactorResponse(): GetScaleCompactorResponse {
  return { suggestCores: 0, runningCores: 0, totalCores: 0, waitingCompactionBytes: 0 };
}

export const GetScaleCompactorResponse = {
  fromJSON(object: any): GetScaleCompactorResponse {
    return {
      suggestCores: isSet(object.suggestCores) ? Number(object.suggestCores) : 0,
      runningCores: isSet(object.runningCores) ? Number(object.runningCores) : 0,
      totalCores: isSet(object.totalCores) ? Number(object.totalCores) : 0,
      waitingCompactionBytes: isSet(object.waitingCompactionBytes) ? Number(object.waitingCompactionBytes) : 0,
    };
  },

  toJSON(message: GetScaleCompactorResponse): unknown {
    const obj: any = {};
    message.suggestCores !== undefined && (obj.suggestCores = Math.round(message.suggestCores));
    message.runningCores !== undefined && (obj.runningCores = Math.round(message.runningCores));
    message.totalCores !== undefined && (obj.totalCores = Math.round(message.totalCores));
    message.waitingCompactionBytes !== undefined &&
      (obj.waitingCompactionBytes = Math.round(message.waitingCompactionBytes));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetScaleCompactorResponse>, I>>(object: I): GetScaleCompactorResponse {
    const message = createBaseGetScaleCompactorResponse();
    message.suggestCores = object.suggestCores ?? 0;
    message.runningCores = object.runningCores ?? 0;
    message.totalCores = object.totalCores ?? 0;
    message.waitingCompactionBytes = object.waitingCompactionBytes ?? 0;
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
