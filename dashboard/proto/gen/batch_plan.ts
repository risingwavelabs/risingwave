/* eslint-disable */
import * as Long from "long";
import * as _m0 from "protobufjs/minimal";
import { Buffer, HostAddress, WorkerNode } from "./common";
import { IntervalUnit } from "./data";
import { AggCall, ExprNode, InputRefExpr, ProjectSetSelectItem, TableFunction } from "./expr";
import {
  ColumnDesc,
  ColumnOrder,
  Field,
  JoinType,
  joinTypeFromJSON,
  joinTypeToJSON,
  OrderType,
  orderTypeFromJSON,
  orderTypeToJSON,
  StorageTableDesc,
} from "./plan_common";

export const protobufPackage = "batch_plan";

export interface RowSeqScanNode {
  tableDesc: StorageTableDesc | undefined;
  columnIds: number[];
  /**
   * All the ranges need to be read. i.e., they are OR'ed.
   *
   * Empty `scan_ranges` means full table scan.
   */
  scanRanges: ScanRange[];
  /**
   * The partition to read for scan tasks.
   *
   * Will be filled by the scheduler.
   */
  vnodeBitmap: Buffer | undefined;
}

export interface SysRowSeqScanNode {
  tableName: string;
  columnDescs: ColumnDesc[];
}

/**
 * The range to scan, which specifies a consecutive range of the PK
 * and can represent: (Suppose there are N columns in the PK)
 * - full table scan: Should not occur. Use an empty `Vec<ScanRange>` instead.
 * - index range scan: `eq_conds` includes i (between 0 and N-1, inclusive) values,
 *     and `lower_bound` & `upper_bound` is the range for the (i+1)th column
 * - index point get: `eq_conds` includes N values, and `lower_bound` & `upper_bound` are `None`
 */
export interface ScanRange {
  /** The i-th element represents the value of the i-th PK column. */
  eqConds: Uint8Array[];
  /** The lower bound of the next PK column subsequent to those in `eq_conds`. */
  lowerBound:
    | ScanRange_Bound
    | undefined;
  /** The upper bound of the next PK column subsequent to those in `eq_conds`. */
  upperBound: ScanRange_Bound | undefined;
}

/** `None` represent unbounded. */
export interface ScanRange_Bound {
  value: Uint8Array;
  inclusive: boolean;
}

export interface SourceScanNode {
  tableId: number;
  /** timestamp_ms is used for offset synchronization of high level consumer groups, this field will be deprecated if a more elegant approach is available in the future */
  timestampMs: number;
  columnIds: number[];
}

export interface ProjectNode {
  selectList: ExprNode[];
}

export interface FilterNode {
  searchCondition: ExprNode | undefined;
}

export interface InsertNode {
  tableSourceId: number;
  columnIds: number[];
  /** Id of the materialized view which is used to determine which compute node to execute the dml fragment. */
  associatedMviewId: number;
}

export interface DeleteNode {
  tableSourceId: number;
  /** Id of the materialized view which is used to determine which compute node to execute the dml fragment. */
  associatedMviewId: number;
}

export interface UpdateNode {
  tableSourceId: number;
  exprs: ExprNode[];
  /** Id of the materialized view which is used to determine which compute node to execute the dml fragment. */
  associatedMviewId: number;
}

export interface ValuesNode {
  tuples: ValuesNode_ExprTuple[];
  fields: Field[];
}

export interface ValuesNode_ExprTuple {
  cells: ExprNode[];
}

export interface OrderByNode {
  columnOrders: ColumnOrder[];
}

export interface TopNNode {
  columnOrders: ColumnOrder[];
  limit: number;
  offset: number;
}

export interface LimitNode {
  limit: number;
  offset: number;
}

export interface NestedLoopJoinNode {
  joinType: JoinType;
  joinCond: ExprNode | undefined;
  outputIndices: number[];
}

export interface HashAggNode {
  groupKey: number[];
  aggCalls: AggCall[];
}

export interface ExpandNode {
  columnSubsets: ExpandNode_Subset[];
}

export interface ExpandNode_Subset {
  columnIndices: number[];
}

export interface ProjectSetNode {
  selectList: ProjectSetSelectItem[];
}

export interface SortAggNode {
  groupKey: ExprNode[];
  aggCalls: AggCall[];
}

export interface HashJoinNode {
  joinType: JoinType;
  leftKey: number[];
  rightKey: number[];
  condition: ExprNode | undefined;
  outputIndices: number[];
  /**
   * Null safe means it treats `null = null` as true.
   * Each key pair can be null safe independently. (left_key, right_key, null_safe)
   */
  nullSafe: boolean[];
}

export interface SortMergeJoinNode {
  joinType: JoinType;
  leftKey: number[];
  rightKey: number[];
  direction: OrderType;
  outputIndices: number[];
}

export interface HopWindowNode {
  timeCol: InputRefExpr | undefined;
  windowSlide: IntervalUnit | undefined;
  windowSize: IntervalUnit | undefined;
  outputIndices: number[];
}

export interface TableFunctionNode {
  tableFunction: TableFunction | undefined;
}

/** Task is a running instance of Stage. */
export interface TaskId {
  queryId: string;
  stageId: number;
  taskId: number;
}

/**
 * Every task will create N buffers (channels) for parent operators to fetch results from,
 * where N is the parallelism of parent stage.
 */
export interface TaskOutputId {
  taskId:
    | TaskId
    | undefined;
  /** The id of output channel to fetch from */
  outputId: number;
}

export interface LocalExecutePlan {
  plan: PlanFragment | undefined;
  epoch: number;
}

/** ExchangeSource describes where to read results from children operators */
export interface ExchangeSource {
  taskOutputId: TaskOutputId | undefined;
  host: HostAddress | undefined;
  plan: LocalExecutePlan | undefined;
}

export interface ExchangeNode {
  sources: ExchangeSource[];
  inputSchema: Field[];
}

export interface MergeSortExchangeNode {
  exchange: ExchangeNode | undefined;
  columnOrders: ColumnOrder[];
}

export interface LookupJoinNode {
  joinType: JoinType;
  condition: ExprNode | undefined;
  buildSideKey: number[];
  probeSideTableDesc: StorageTableDesc | undefined;
  probeSideVnodeMapping: number[];
  probeSideColumnIds: number[];
  outputIndices: number[];
  workerNodes: WorkerNode[];
}

export interface UnionNode {
}

export interface PlanNode {
  children: PlanNode[];
  insert: InsertNode | undefined;
  delete: DeleteNode | undefined;
  update: UpdateNode | undefined;
  project: ProjectNode | undefined;
  hashAgg: HashAggNode | undefined;
  filter: FilterNode | undefined;
  exchange: ExchangeNode | undefined;
  orderBy: OrderByNode | undefined;
  nestedLoopJoin: NestedLoopJoinNode | undefined;
  topN: TopNNode | undefined;
  sortAgg: SortAggNode | undefined;
  rowSeqScan: RowSeqScanNode | undefined;
  limit: LimitNode | undefined;
  values: ValuesNode | undefined;
  hashJoin: HashJoinNode | undefined;
  mergeSortExchange: MergeSortExchangeNode | undefined;
  sortMergeJoin: SortMergeJoinNode | undefined;
  hopWindow: HopWindowNode | undefined;
  tableFunction: TableFunctionNode | undefined;
  sysRowSeqScan: SysRowSeqScanNode | undefined;
  expand: ExpandNode | undefined;
  lookupJoin: LookupJoinNode | undefined;
  projectSet: ProjectSetNode | undefined;
  union: UnionNode | undefined;
  identity: string;
}

/**
 * ExchangeInfo determines how to distribute results to tasks of next stage.
 *
 * Note that the fragment itself does not know the where are the receivers. Instead, it prepares results in
 * N buffers and wait for parent operators (`Exchange` nodes) to pull data from a specified buffer
 */
export interface ExchangeInfo {
  mode: ExchangeInfo_DistributionMode;
  broadcastInfo: ExchangeInfo_BroadcastInfo | undefined;
  hashInfo: ExchangeInfo_HashInfo | undefined;
}

export enum ExchangeInfo_DistributionMode {
  /** UNSPECIFIED - No partitioning at all, used for root segment which aggregates query results */
  UNSPECIFIED = 0,
  SINGLE = 1,
  BROADCAST = 2,
  HASH = 3,
  UNRECOGNIZED = -1,
}

export function exchangeInfo_DistributionModeFromJSON(object: any): ExchangeInfo_DistributionMode {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return ExchangeInfo_DistributionMode.UNSPECIFIED;
    case 1:
    case "SINGLE":
      return ExchangeInfo_DistributionMode.SINGLE;
    case 2:
    case "BROADCAST":
      return ExchangeInfo_DistributionMode.BROADCAST;
    case 3:
    case "HASH":
      return ExchangeInfo_DistributionMode.HASH;
    case -1:
    case "UNRECOGNIZED":
    default:
      return ExchangeInfo_DistributionMode.UNRECOGNIZED;
  }
}

export function exchangeInfo_DistributionModeToJSON(object: ExchangeInfo_DistributionMode): string {
  switch (object) {
    case ExchangeInfo_DistributionMode.UNSPECIFIED:
      return "UNSPECIFIED";
    case ExchangeInfo_DistributionMode.SINGLE:
      return "SINGLE";
    case ExchangeInfo_DistributionMode.BROADCAST:
      return "BROADCAST";
    case ExchangeInfo_DistributionMode.HASH:
      return "HASH";
    case ExchangeInfo_DistributionMode.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface ExchangeInfo_BroadcastInfo {
  count: number;
}

export interface ExchangeInfo_HashInfo {
  outputCount: number;
  key: number[];
}

export interface PlanFragment {
  root: PlanNode | undefined;
  exchangeInfo: ExchangeInfo | undefined;
}

function createBaseRowSeqScanNode(): RowSeqScanNode {
  return { tableDesc: undefined, columnIds: [], scanRanges: [], vnodeBitmap: undefined };
}

export const RowSeqScanNode = {
  encode(message: RowSeqScanNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableDesc !== undefined) {
      StorageTableDesc.encode(message.tableDesc, writer.uint32(10).fork()).ldelim();
    }
    writer.uint32(18).fork();
    for (const v of message.columnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    for (const v of message.scanRanges) {
      ScanRange.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    if (message.vnodeBitmap !== undefined) {
      Buffer.encode(message.vnodeBitmap, writer.uint32(34).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RowSeqScanNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRowSeqScanNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableDesc = StorageTableDesc.decode(reader, reader.uint32());
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.columnIds.push(reader.int32());
            }
          } else {
            message.columnIds.push(reader.int32());
          }
          break;
        case 3:
          message.scanRanges.push(ScanRange.decode(reader, reader.uint32()));
          break;
        case 4:
          message.vnodeBitmap = Buffer.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): RowSeqScanNode {
    return {
      tableDesc: isSet(object.tableDesc) ? StorageTableDesc.fromJSON(object.tableDesc) : undefined,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
      scanRanges: Array.isArray(object?.scanRanges) ? object.scanRanges.map((e: any) => ScanRange.fromJSON(e)) : [],
      vnodeBitmap: isSet(object.vnodeBitmap) ? Buffer.fromJSON(object.vnodeBitmap) : undefined,
    };
  },

  toJSON(message: RowSeqScanNode): unknown {
    const obj: any = {};
    message.tableDesc !== undefined &&
      (obj.tableDesc = message.tableDesc ? StorageTableDesc.toJSON(message.tableDesc) : undefined);
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    if (message.scanRanges) {
      obj.scanRanges = message.scanRanges.map((e) => e ? ScanRange.toJSON(e) : undefined);
    } else {
      obj.scanRanges = [];
    }
    message.vnodeBitmap !== undefined &&
      (obj.vnodeBitmap = message.vnodeBitmap ? Buffer.toJSON(message.vnodeBitmap) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RowSeqScanNode>, I>>(object: I): RowSeqScanNode {
    const message = createBaseRowSeqScanNode();
    message.tableDesc = (object.tableDesc !== undefined && object.tableDesc !== null)
      ? StorageTableDesc.fromPartial(object.tableDesc)
      : undefined;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    message.scanRanges = object.scanRanges?.map((e) => ScanRange.fromPartial(e)) || [];
    message.vnodeBitmap = (object.vnodeBitmap !== undefined && object.vnodeBitmap !== null)
      ? Buffer.fromPartial(object.vnodeBitmap)
      : undefined;
    return message;
  },
};

function createBaseSysRowSeqScanNode(): SysRowSeqScanNode {
  return { tableName: "", columnDescs: [] };
}

export const SysRowSeqScanNode = {
  encode(message: SysRowSeqScanNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableName !== "") {
      writer.uint32(10).string(message.tableName);
    }
    for (const v of message.columnDescs) {
      ColumnDesc.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SysRowSeqScanNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSysRowSeqScanNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableName = reader.string();
          break;
        case 2:
          message.columnDescs.push(ColumnDesc.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SysRowSeqScanNode {
    return {
      tableName: isSet(object.tableName) ? String(object.tableName) : "",
      columnDescs: Array.isArray(object?.columnDescs) ? object.columnDescs.map((e: any) => ColumnDesc.fromJSON(e)) : [],
    };
  },

  toJSON(message: SysRowSeqScanNode): unknown {
    const obj: any = {};
    message.tableName !== undefined && (obj.tableName = message.tableName);
    if (message.columnDescs) {
      obj.columnDescs = message.columnDescs.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.columnDescs = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SysRowSeqScanNode>, I>>(object: I): SysRowSeqScanNode {
    const message = createBaseSysRowSeqScanNode();
    message.tableName = object.tableName ?? "";
    message.columnDescs = object.columnDescs?.map((e) => ColumnDesc.fromPartial(e)) || [];
    return message;
  },
};

function createBaseScanRange(): ScanRange {
  return { eqConds: [], lowerBound: undefined, upperBound: undefined };
}

export const ScanRange = {
  encode(message: ScanRange, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.eqConds) {
      writer.uint32(10).bytes(v!);
    }
    if (message.lowerBound !== undefined) {
      ScanRange_Bound.encode(message.lowerBound, writer.uint32(18).fork()).ldelim();
    }
    if (message.upperBound !== undefined) {
      ScanRange_Bound.encode(message.upperBound, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ScanRange {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseScanRange();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.eqConds.push(reader.bytes());
          break;
        case 2:
          message.lowerBound = ScanRange_Bound.decode(reader, reader.uint32());
          break;
        case 3:
          message.upperBound = ScanRange_Bound.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ScanRange {
    return {
      eqConds: Array.isArray(object?.eqConds) ? object.eqConds.map((e: any) => bytesFromBase64(e)) : [],
      lowerBound: isSet(object.lowerBound) ? ScanRange_Bound.fromJSON(object.lowerBound) : undefined,
      upperBound: isSet(object.upperBound) ? ScanRange_Bound.fromJSON(object.upperBound) : undefined,
    };
  },

  toJSON(message: ScanRange): unknown {
    const obj: any = {};
    if (message.eqConds) {
      obj.eqConds = message.eqConds.map((e) => base64FromBytes(e !== undefined ? e : new Uint8Array()));
    } else {
      obj.eqConds = [];
    }
    message.lowerBound !== undefined &&
      (obj.lowerBound = message.lowerBound ? ScanRange_Bound.toJSON(message.lowerBound) : undefined);
    message.upperBound !== undefined &&
      (obj.upperBound = message.upperBound ? ScanRange_Bound.toJSON(message.upperBound) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ScanRange>, I>>(object: I): ScanRange {
    const message = createBaseScanRange();
    message.eqConds = object.eqConds?.map((e) => e) || [];
    message.lowerBound = (object.lowerBound !== undefined && object.lowerBound !== null)
      ? ScanRange_Bound.fromPartial(object.lowerBound)
      : undefined;
    message.upperBound = (object.upperBound !== undefined && object.upperBound !== null)
      ? ScanRange_Bound.fromPartial(object.upperBound)
      : undefined;
    return message;
  },
};

function createBaseScanRange_Bound(): ScanRange_Bound {
  return { value: new Uint8Array(), inclusive: false };
}

export const ScanRange_Bound = {
  encode(message: ScanRange_Bound, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.value.length !== 0) {
      writer.uint32(10).bytes(message.value);
    }
    if (message.inclusive === true) {
      writer.uint32(16).bool(message.inclusive);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ScanRange_Bound {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseScanRange_Bound();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.value = reader.bytes();
          break;
        case 2:
          message.inclusive = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ScanRange_Bound {
    return {
      value: isSet(object.value) ? bytesFromBase64(object.value) : new Uint8Array(),
      inclusive: isSet(object.inclusive) ? Boolean(object.inclusive) : false,
    };
  },

  toJSON(message: ScanRange_Bound): unknown {
    const obj: any = {};
    message.value !== undefined &&
      (obj.value = base64FromBytes(message.value !== undefined ? message.value : new Uint8Array()));
    message.inclusive !== undefined && (obj.inclusive = message.inclusive);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ScanRange_Bound>, I>>(object: I): ScanRange_Bound {
    const message = createBaseScanRange_Bound();
    message.value = object.value ?? new Uint8Array();
    message.inclusive = object.inclusive ?? false;
    return message;
  },
};

function createBaseSourceScanNode(): SourceScanNode {
  return { tableId: 0, timestampMs: 0, columnIds: [] };
}

export const SourceScanNode = {
  encode(message: SourceScanNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableId !== 0) {
      writer.uint32(8).uint32(message.tableId);
    }
    if (message.timestampMs !== 0) {
      writer.uint32(16).int64(message.timestampMs);
    }
    writer.uint32(26).fork();
    for (const v of message.columnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SourceScanNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSourceScanNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableId = reader.uint32();
          break;
        case 2:
          message.timestampMs = longToNumber(reader.int64() as Long);
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.columnIds.push(reader.int32());
            }
          } else {
            message.columnIds.push(reader.int32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SourceScanNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      timestampMs: isSet(object.timestampMs) ? Number(object.timestampMs) : 0,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: SourceScanNode): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.timestampMs !== undefined && (obj.timestampMs = Math.round(message.timestampMs));
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceScanNode>, I>>(object: I): SourceScanNode {
    const message = createBaseSourceScanNode();
    message.tableId = object.tableId ?? 0;
    message.timestampMs = object.timestampMs ?? 0;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseProjectNode(): ProjectNode {
  return { selectList: [] };
}

export const ProjectNode = {
  encode(message: ProjectNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.selectList) {
      ExprNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ProjectNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseProjectNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.selectList.push(ExprNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ProjectNode {
    return {
      selectList: Array.isArray(object?.selectList) ? object.selectList.map((e: any) => ExprNode.fromJSON(e)) : [],
    };
  },

  toJSON(message: ProjectNode): unknown {
    const obj: any = {};
    if (message.selectList) {
      obj.selectList = message.selectList.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.selectList = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProjectNode>, I>>(object: I): ProjectNode {
    const message = createBaseProjectNode();
    message.selectList = object.selectList?.map((e) => ExprNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseFilterNode(): FilterNode {
  return { searchCondition: undefined };
}

export const FilterNode = {
  encode(message: FilterNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.searchCondition !== undefined) {
      ExprNode.encode(message.searchCondition, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FilterNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFilterNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.searchCondition = ExprNode.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): FilterNode {
    return { searchCondition: isSet(object.searchCondition) ? ExprNode.fromJSON(object.searchCondition) : undefined };
  },

  toJSON(message: FilterNode): unknown {
    const obj: any = {};
    message.searchCondition !== undefined &&
      (obj.searchCondition = message.searchCondition ? ExprNode.toJSON(message.searchCondition) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FilterNode>, I>>(object: I): FilterNode {
    const message = createBaseFilterNode();
    message.searchCondition = (object.searchCondition !== undefined && object.searchCondition !== null)
      ? ExprNode.fromPartial(object.searchCondition)
      : undefined;
    return message;
  },
};

function createBaseInsertNode(): InsertNode {
  return { tableSourceId: 0, columnIds: [], associatedMviewId: 0 };
}

export const InsertNode = {
  encode(message: InsertNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableSourceId !== 0) {
      writer.uint32(8).uint32(message.tableSourceId);
    }
    writer.uint32(18).fork();
    for (const v of message.columnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    if (message.associatedMviewId !== 0) {
      writer.uint32(24).uint32(message.associatedMviewId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): InsertNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseInsertNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableSourceId = reader.uint32();
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.columnIds.push(reader.int32());
            }
          } else {
            message.columnIds.push(reader.int32());
          }
          break;
        case 3:
          message.associatedMviewId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): InsertNode {
    return {
      tableSourceId: isSet(object.tableSourceId) ? Number(object.tableSourceId) : 0,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
      associatedMviewId: isSet(object.associatedMviewId) ? Number(object.associatedMviewId) : 0,
    };
  },

  toJSON(message: InsertNode): unknown {
    const obj: any = {};
    message.tableSourceId !== undefined && (obj.tableSourceId = Math.round(message.tableSourceId));
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    message.associatedMviewId !== undefined && (obj.associatedMviewId = Math.round(message.associatedMviewId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InsertNode>, I>>(object: I): InsertNode {
    const message = createBaseInsertNode();
    message.tableSourceId = object.tableSourceId ?? 0;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    message.associatedMviewId = object.associatedMviewId ?? 0;
    return message;
  },
};

function createBaseDeleteNode(): DeleteNode {
  return { tableSourceId: 0, associatedMviewId: 0 };
}

export const DeleteNode = {
  encode(message: DeleteNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableSourceId !== 0) {
      writer.uint32(8).uint32(message.tableSourceId);
    }
    if (message.associatedMviewId !== 0) {
      writer.uint32(16).uint32(message.associatedMviewId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DeleteNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDeleteNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableSourceId = reader.uint32();
          break;
        case 2:
          message.associatedMviewId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): DeleteNode {
    return {
      tableSourceId: isSet(object.tableSourceId) ? Number(object.tableSourceId) : 0,
      associatedMviewId: isSet(object.associatedMviewId) ? Number(object.associatedMviewId) : 0,
    };
  },

  toJSON(message: DeleteNode): unknown {
    const obj: any = {};
    message.tableSourceId !== undefined && (obj.tableSourceId = Math.round(message.tableSourceId));
    message.associatedMviewId !== undefined && (obj.associatedMviewId = Math.round(message.associatedMviewId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeleteNode>, I>>(object: I): DeleteNode {
    const message = createBaseDeleteNode();
    message.tableSourceId = object.tableSourceId ?? 0;
    message.associatedMviewId = object.associatedMviewId ?? 0;
    return message;
  },
};

function createBaseUpdateNode(): UpdateNode {
  return { tableSourceId: 0, exprs: [], associatedMviewId: 0 };
}

export const UpdateNode = {
  encode(message: UpdateNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableSourceId !== 0) {
      writer.uint32(8).uint32(message.tableSourceId);
    }
    for (const v of message.exprs) {
      ExprNode.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.associatedMviewId !== 0) {
      writer.uint32(24).uint32(message.associatedMviewId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UpdateNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUpdateNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableSourceId = reader.uint32();
          break;
        case 2:
          message.exprs.push(ExprNode.decode(reader, reader.uint32()));
          break;
        case 3:
          message.associatedMviewId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): UpdateNode {
    return {
      tableSourceId: isSet(object.tableSourceId) ? Number(object.tableSourceId) : 0,
      exprs: Array.isArray(object?.exprs) ? object.exprs.map((e: any) => ExprNode.fromJSON(e)) : [],
      associatedMviewId: isSet(object.associatedMviewId) ? Number(object.associatedMviewId) : 0,
    };
  },

  toJSON(message: UpdateNode): unknown {
    const obj: any = {};
    message.tableSourceId !== undefined && (obj.tableSourceId = Math.round(message.tableSourceId));
    if (message.exprs) {
      obj.exprs = message.exprs.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.exprs = [];
    }
    message.associatedMviewId !== undefined && (obj.associatedMviewId = Math.round(message.associatedMviewId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateNode>, I>>(object: I): UpdateNode {
    const message = createBaseUpdateNode();
    message.tableSourceId = object.tableSourceId ?? 0;
    message.exprs = object.exprs?.map((e) => ExprNode.fromPartial(e)) || [];
    message.associatedMviewId = object.associatedMviewId ?? 0;
    return message;
  },
};

function createBaseValuesNode(): ValuesNode {
  return { tuples: [], fields: [] };
}

export const ValuesNode = {
  encode(message: ValuesNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.tuples) {
      ValuesNode_ExprTuple.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.fields) {
      Field.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValuesNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValuesNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tuples.push(ValuesNode_ExprTuple.decode(reader, reader.uint32()));
          break;
        case 2:
          message.fields.push(Field.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ValuesNode {
    return {
      tuples: Array.isArray(object?.tuples) ? object.tuples.map((e: any) => ValuesNode_ExprTuple.fromJSON(e)) : [],
      fields: Array.isArray(object?.fields) ? object.fields.map((e: any) => Field.fromJSON(e)) : [],
    };
  },

  toJSON(message: ValuesNode): unknown {
    const obj: any = {};
    if (message.tuples) {
      obj.tuples = message.tuples.map((e) => e ? ValuesNode_ExprTuple.toJSON(e) : undefined);
    } else {
      obj.tuples = [];
    }
    if (message.fields) {
      obj.fields = message.fields.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.fields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ValuesNode>, I>>(object: I): ValuesNode {
    const message = createBaseValuesNode();
    message.tuples = object.tuples?.map((e) => ValuesNode_ExprTuple.fromPartial(e)) || [];
    message.fields = object.fields?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseValuesNode_ExprTuple(): ValuesNode_ExprTuple {
  return { cells: [] };
}

export const ValuesNode_ExprTuple = {
  encode(message: ValuesNode_ExprTuple, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.cells) {
      ExprNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ValuesNode_ExprTuple {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseValuesNode_ExprTuple();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.cells.push(ExprNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ValuesNode_ExprTuple {
    return { cells: Array.isArray(object?.cells) ? object.cells.map((e: any) => ExprNode.fromJSON(e)) : [] };
  },

  toJSON(message: ValuesNode_ExprTuple): unknown {
    const obj: any = {};
    if (message.cells) {
      obj.cells = message.cells.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.cells = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ValuesNode_ExprTuple>, I>>(object: I): ValuesNode_ExprTuple {
    const message = createBaseValuesNode_ExprTuple();
    message.cells = object.cells?.map((e) => ExprNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseOrderByNode(): OrderByNode {
  return { columnOrders: [] };
}

export const OrderByNode = {
  encode(message: OrderByNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.columnOrders) {
      ColumnOrder.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): OrderByNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseOrderByNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnOrders.push(ColumnOrder.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): OrderByNode {
    return {
      columnOrders: Array.isArray(object?.columnOrders)
        ? object.columnOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
    };
  },

  toJSON(message: OrderByNode): unknown {
    const obj: any = {};
    if (message.columnOrders) {
      obj.columnOrders = message.columnOrders.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.columnOrders = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<OrderByNode>, I>>(object: I): OrderByNode {
    const message = createBaseOrderByNode();
    message.columnOrders = object.columnOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    return message;
  },
};

function createBaseTopNNode(): TopNNode {
  return { columnOrders: [], limit: 0, offset: 0 };
}

export const TopNNode = {
  encode(message: TopNNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.columnOrders) {
      ColumnOrder.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    if (message.limit !== 0) {
      writer.uint32(16).uint64(message.limit);
    }
    if (message.offset !== 0) {
      writer.uint32(24).uint64(message.offset);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TopNNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTopNNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnOrders.push(ColumnOrder.decode(reader, reader.uint32()));
          break;
        case 2:
          message.limit = longToNumber(reader.uint64() as Long);
          break;
        case 3:
          message.offset = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TopNNode {
    return {
      columnOrders: Array.isArray(object?.columnOrders)
        ? object.columnOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
    };
  },

  toJSON(message: TopNNode): unknown {
    const obj: any = {};
    if (message.columnOrders) {
      obj.columnOrders = message.columnOrders.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.columnOrders = [];
    }
    message.limit !== undefined && (obj.limit = Math.round(message.limit));
    message.offset !== undefined && (obj.offset = Math.round(message.offset));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TopNNode>, I>>(object: I): TopNNode {
    const message = createBaseTopNNode();
    message.columnOrders = object.columnOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    return message;
  },
};

function createBaseLimitNode(): LimitNode {
  return { limit: 0, offset: 0 };
}

export const LimitNode = {
  encode(message: LimitNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.limit !== 0) {
      writer.uint32(8).uint64(message.limit);
    }
    if (message.offset !== 0) {
      writer.uint32(16).uint64(message.offset);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LimitNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLimitNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.limit = longToNumber(reader.uint64() as Long);
          break;
        case 2:
          message.offset = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LimitNode {
    return {
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
    };
  },

  toJSON(message: LimitNode): unknown {
    const obj: any = {};
    message.limit !== undefined && (obj.limit = Math.round(message.limit));
    message.offset !== undefined && (obj.offset = Math.round(message.offset));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LimitNode>, I>>(object: I): LimitNode {
    const message = createBaseLimitNode();
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    return message;
  },
};

function createBaseNestedLoopJoinNode(): NestedLoopJoinNode {
  return { joinType: 0, joinCond: undefined, outputIndices: [] };
}

export const NestedLoopJoinNode = {
  encode(message: NestedLoopJoinNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.joinType !== 0) {
      writer.uint32(8).int32(message.joinType);
    }
    if (message.joinCond !== undefined) {
      ExprNode.encode(message.joinCond, writer.uint32(18).fork()).ldelim();
    }
    writer.uint32(26).fork();
    for (const v of message.outputIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): NestedLoopJoinNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseNestedLoopJoinNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.joinType = reader.int32() as any;
          break;
        case 2:
          message.joinCond = ExprNode.decode(reader, reader.uint32());
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.outputIndices.push(reader.uint32());
            }
          } else {
            message.outputIndices.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): NestedLoopJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : 0,
      joinCond: isSet(object.joinCond) ? ExprNode.fromJSON(object.joinCond) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: NestedLoopJoinNode): unknown {
    const obj: any = {};
    message.joinType !== undefined && (obj.joinType = joinTypeToJSON(message.joinType));
    message.joinCond !== undefined && (obj.joinCond = message.joinCond ? ExprNode.toJSON(message.joinCond) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<NestedLoopJoinNode>, I>>(object: I): NestedLoopJoinNode {
    const message = createBaseNestedLoopJoinNode();
    message.joinType = object.joinType ?? 0;
    message.joinCond = (object.joinCond !== undefined && object.joinCond !== null)
      ? ExprNode.fromPartial(object.joinCond)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseHashAggNode(): HashAggNode {
  return { groupKey: [], aggCalls: [] };
}

export const HashAggNode = {
  encode(message: HashAggNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.groupKey) {
      writer.uint32(v);
    }
    writer.ldelim();
    for (const v of message.aggCalls) {
      AggCall.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HashAggNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHashAggNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.groupKey.push(reader.uint32());
            }
          } else {
            message.groupKey.push(reader.uint32());
          }
          break;
        case 2:
          message.aggCalls.push(AggCall.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HashAggNode {
    return {
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => Number(e)) : [],
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
    };
  },

  toJSON(message: HashAggNode): unknown {
    const obj: any = {};
    if (message.groupKey) {
      obj.groupKey = message.groupKey.map((e) => Math.round(e));
    } else {
      obj.groupKey = [];
    }
    if (message.aggCalls) {
      obj.aggCalls = message.aggCalls.map((e) => e ? AggCall.toJSON(e) : undefined);
    } else {
      obj.aggCalls = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashAggNode>, I>>(object: I): HashAggNode {
    const message = createBaseHashAggNode();
    message.groupKey = object.groupKey?.map((e) => e) || [];
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    return message;
  },
};

function createBaseExpandNode(): ExpandNode {
  return { columnSubsets: [] };
}

export const ExpandNode = {
  encode(message: ExpandNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.columnSubsets) {
      ExpandNode_Subset.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExpandNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExpandNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnSubsets.push(ExpandNode_Subset.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExpandNode {
    return {
      columnSubsets: Array.isArray(object?.columnSubsets)
        ? object.columnSubsets.map((e: any) => ExpandNode_Subset.fromJSON(e))
        : [],
    };
  },

  toJSON(message: ExpandNode): unknown {
    const obj: any = {};
    if (message.columnSubsets) {
      obj.columnSubsets = message.columnSubsets.map((e) => e ? ExpandNode_Subset.toJSON(e) : undefined);
    } else {
      obj.columnSubsets = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExpandNode>, I>>(object: I): ExpandNode {
    const message = createBaseExpandNode();
    message.columnSubsets = object.columnSubsets?.map((e) => ExpandNode_Subset.fromPartial(e)) || [];
    return message;
  },
};

function createBaseExpandNode_Subset(): ExpandNode_Subset {
  return { columnIndices: [] };
}

export const ExpandNode_Subset = {
  encode(message: ExpandNode_Subset, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    writer.uint32(10).fork();
    for (const v of message.columnIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExpandNode_Subset {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExpandNode_Subset();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.columnIndices.push(reader.uint32());
            }
          } else {
            message.columnIndices.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExpandNode_Subset {
    return {
      columnIndices: Array.isArray(object?.columnIndices) ? object.columnIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ExpandNode_Subset): unknown {
    const obj: any = {};
    if (message.columnIndices) {
      obj.columnIndices = message.columnIndices.map((e) => Math.round(e));
    } else {
      obj.columnIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExpandNode_Subset>, I>>(object: I): ExpandNode_Subset {
    const message = createBaseExpandNode_Subset();
    message.columnIndices = object.columnIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseProjectSetNode(): ProjectSetNode {
  return { selectList: [] };
}

export const ProjectSetNode = {
  encode(message: ProjectSetNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.selectList) {
      ProjectSetSelectItem.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ProjectSetNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseProjectSetNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.selectList.push(ProjectSetSelectItem.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ProjectSetNode {
    return {
      selectList: Array.isArray(object?.selectList)
        ? object.selectList.map((e: any) => ProjectSetSelectItem.fromJSON(e))
        : [],
    };
  },

  toJSON(message: ProjectSetNode): unknown {
    const obj: any = {};
    if (message.selectList) {
      obj.selectList = message.selectList.map((e) => e ? ProjectSetSelectItem.toJSON(e) : undefined);
    } else {
      obj.selectList = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProjectSetNode>, I>>(object: I): ProjectSetNode {
    const message = createBaseProjectSetNode();
    message.selectList = object.selectList?.map((e) => ProjectSetSelectItem.fromPartial(e)) || [];
    return message;
  },
};

function createBaseSortAggNode(): SortAggNode {
  return { groupKey: [], aggCalls: [] };
}

export const SortAggNode = {
  encode(message: SortAggNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.groupKey) {
      ExprNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.aggCalls) {
      AggCall.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SortAggNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSortAggNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.groupKey.push(ExprNode.decode(reader, reader.uint32()));
          break;
        case 2:
          message.aggCalls.push(AggCall.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SortAggNode {
    return {
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => ExprNode.fromJSON(e)) : [],
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
    };
  },

  toJSON(message: SortAggNode): unknown {
    const obj: any = {};
    if (message.groupKey) {
      obj.groupKey = message.groupKey.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.groupKey = [];
    }
    if (message.aggCalls) {
      obj.aggCalls = message.aggCalls.map((e) => e ? AggCall.toJSON(e) : undefined);
    } else {
      obj.aggCalls = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SortAggNode>, I>>(object: I): SortAggNode {
    const message = createBaseSortAggNode();
    message.groupKey = object.groupKey?.map((e) => ExprNode.fromPartial(e)) || [];
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    return message;
  },
};

function createBaseHashJoinNode(): HashJoinNode {
  return { joinType: 0, leftKey: [], rightKey: [], condition: undefined, outputIndices: [], nullSafe: [] };
}

export const HashJoinNode = {
  encode(message: HashJoinNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.joinType !== 0) {
      writer.uint32(8).int32(message.joinType);
    }
    writer.uint32(18).fork();
    for (const v of message.leftKey) {
      writer.int32(v);
    }
    writer.ldelim();
    writer.uint32(26).fork();
    for (const v of message.rightKey) {
      writer.int32(v);
    }
    writer.ldelim();
    if (message.condition !== undefined) {
      ExprNode.encode(message.condition, writer.uint32(34).fork()).ldelim();
    }
    writer.uint32(42).fork();
    for (const v of message.outputIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    writer.uint32(50).fork();
    for (const v of message.nullSafe) {
      writer.bool(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HashJoinNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHashJoinNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.joinType = reader.int32() as any;
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.leftKey.push(reader.int32());
            }
          } else {
            message.leftKey.push(reader.int32());
          }
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.rightKey.push(reader.int32());
            }
          } else {
            message.rightKey.push(reader.int32());
          }
          break;
        case 4:
          message.condition = ExprNode.decode(reader, reader.uint32());
          break;
        case 5:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.outputIndices.push(reader.uint32());
            }
          } else {
            message.outputIndices.push(reader.uint32());
          }
          break;
        case 6:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.nullSafe.push(reader.bool());
            }
          } else {
            message.nullSafe.push(reader.bool());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HashJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : 0,
      leftKey: Array.isArray(object?.leftKey) ? object.leftKey.map((e: any) => Number(e)) : [],
      rightKey: Array.isArray(object?.rightKey) ? object.rightKey.map((e: any) => Number(e)) : [],
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      nullSafe: Array.isArray(object?.nullSafe) ? object.nullSafe.map((e: any) => Boolean(e)) : [],
    };
  },

  toJSON(message: HashJoinNode): unknown {
    const obj: any = {};
    message.joinType !== undefined && (obj.joinType = joinTypeToJSON(message.joinType));
    if (message.leftKey) {
      obj.leftKey = message.leftKey.map((e) => Math.round(e));
    } else {
      obj.leftKey = [];
    }
    if (message.rightKey) {
      obj.rightKey = message.rightKey.map((e) => Math.round(e));
    } else {
      obj.rightKey = [];
    }
    message.condition !== undefined &&
      (obj.condition = message.condition ? ExprNode.toJSON(message.condition) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    if (message.nullSafe) {
      obj.nullSafe = message.nullSafe.map((e) => e);
    } else {
      obj.nullSafe = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashJoinNode>, I>>(object: I): HashJoinNode {
    const message = createBaseHashJoinNode();
    message.joinType = object.joinType ?? 0;
    message.leftKey = object.leftKey?.map((e) => e) || [];
    message.rightKey = object.rightKey?.map((e) => e) || [];
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.nullSafe = object.nullSafe?.map((e) => e) || [];
    return message;
  },
};

function createBaseSortMergeJoinNode(): SortMergeJoinNode {
  return { joinType: 0, leftKey: [], rightKey: [], direction: 0, outputIndices: [] };
}

export const SortMergeJoinNode = {
  encode(message: SortMergeJoinNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.joinType !== 0) {
      writer.uint32(8).int32(message.joinType);
    }
    writer.uint32(18).fork();
    for (const v of message.leftKey) {
      writer.int32(v);
    }
    writer.ldelim();
    writer.uint32(26).fork();
    for (const v of message.rightKey) {
      writer.int32(v);
    }
    writer.ldelim();
    if (message.direction !== 0) {
      writer.uint32(32).int32(message.direction);
    }
    writer.uint32(42).fork();
    for (const v of message.outputIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): SortMergeJoinNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseSortMergeJoinNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.joinType = reader.int32() as any;
          break;
        case 2:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.leftKey.push(reader.int32());
            }
          } else {
            message.leftKey.push(reader.int32());
          }
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.rightKey.push(reader.int32());
            }
          } else {
            message.rightKey.push(reader.int32());
          }
          break;
        case 4:
          message.direction = reader.int32() as any;
          break;
        case 5:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.outputIndices.push(reader.uint32());
            }
          } else {
            message.outputIndices.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): SortMergeJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : 0,
      leftKey: Array.isArray(object?.leftKey) ? object.leftKey.map((e: any) => Number(e)) : [],
      rightKey: Array.isArray(object?.rightKey) ? object.rightKey.map((e: any) => Number(e)) : [],
      direction: isSet(object.direction) ? orderTypeFromJSON(object.direction) : 0,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: SortMergeJoinNode): unknown {
    const obj: any = {};
    message.joinType !== undefined && (obj.joinType = joinTypeToJSON(message.joinType));
    if (message.leftKey) {
      obj.leftKey = message.leftKey.map((e) => Math.round(e));
    } else {
      obj.leftKey = [];
    }
    if (message.rightKey) {
      obj.rightKey = message.rightKey.map((e) => Math.round(e));
    } else {
      obj.rightKey = [];
    }
    message.direction !== undefined && (obj.direction = orderTypeToJSON(message.direction));
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SortMergeJoinNode>, I>>(object: I): SortMergeJoinNode {
    const message = createBaseSortMergeJoinNode();
    message.joinType = object.joinType ?? 0;
    message.leftKey = object.leftKey?.map((e) => e) || [];
    message.rightKey = object.rightKey?.map((e) => e) || [];
    message.direction = object.direction ?? 0;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseHopWindowNode(): HopWindowNode {
  return { timeCol: undefined, windowSlide: undefined, windowSize: undefined, outputIndices: [] };
}

export const HopWindowNode = {
  encode(message: HopWindowNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.timeCol !== undefined) {
      InputRefExpr.encode(message.timeCol, writer.uint32(10).fork()).ldelim();
    }
    if (message.windowSlide !== undefined) {
      IntervalUnit.encode(message.windowSlide, writer.uint32(18).fork()).ldelim();
    }
    if (message.windowSize !== undefined) {
      IntervalUnit.encode(message.windowSize, writer.uint32(26).fork()).ldelim();
    }
    writer.uint32(34).fork();
    for (const v of message.outputIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): HopWindowNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseHopWindowNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.timeCol = InputRefExpr.decode(reader, reader.uint32());
          break;
        case 2:
          message.windowSlide = IntervalUnit.decode(reader, reader.uint32());
          break;
        case 3:
          message.windowSize = IntervalUnit.decode(reader, reader.uint32());
          break;
        case 4:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.outputIndices.push(reader.uint32());
            }
          } else {
            message.outputIndices.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): HopWindowNode {
    return {
      timeCol: isSet(object.timeCol) ? InputRefExpr.fromJSON(object.timeCol) : undefined,
      windowSlide: isSet(object.windowSlide) ? IntervalUnit.fromJSON(object.windowSlide) : undefined,
      windowSize: isSet(object.windowSize) ? IntervalUnit.fromJSON(object.windowSize) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: HopWindowNode): unknown {
    const obj: any = {};
    message.timeCol !== undefined && (obj.timeCol = message.timeCol ? InputRefExpr.toJSON(message.timeCol) : undefined);
    message.windowSlide !== undefined &&
      (obj.windowSlide = message.windowSlide ? IntervalUnit.toJSON(message.windowSlide) : undefined);
    message.windowSize !== undefined &&
      (obj.windowSize = message.windowSize ? IntervalUnit.toJSON(message.windowSize) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HopWindowNode>, I>>(object: I): HopWindowNode {
    const message = createBaseHopWindowNode();
    message.timeCol = (object.timeCol !== undefined && object.timeCol !== null)
      ? InputRefExpr.fromPartial(object.timeCol)
      : undefined;
    message.windowSlide = (object.windowSlide !== undefined && object.windowSlide !== null)
      ? IntervalUnit.fromPartial(object.windowSlide)
      : undefined;
    message.windowSize = (object.windowSize !== undefined && object.windowSize !== null)
      ? IntervalUnit.fromPartial(object.windowSize)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseTableFunctionNode(): TableFunctionNode {
  return { tableFunction: undefined };
}

export const TableFunctionNode = {
  encode(message: TableFunctionNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.tableFunction !== undefined) {
      TableFunction.encode(message.tableFunction, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFunctionNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFunctionNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.tableFunction = TableFunction.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFunctionNode {
    return { tableFunction: isSet(object.tableFunction) ? TableFunction.fromJSON(object.tableFunction) : undefined };
  },

  toJSON(message: TableFunctionNode): unknown {
    const obj: any = {};
    message.tableFunction !== undefined &&
      (obj.tableFunction = message.tableFunction ? TableFunction.toJSON(message.tableFunction) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFunctionNode>, I>>(object: I): TableFunctionNode {
    const message = createBaseTableFunctionNode();
    message.tableFunction = (object.tableFunction !== undefined && object.tableFunction !== null)
      ? TableFunction.fromPartial(object.tableFunction)
      : undefined;
    return message;
  },
};

function createBaseTaskId(): TaskId {
  return { queryId: "", stageId: 0, taskId: 0 };
}

export const TaskId = {
  encode(message: TaskId, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.queryId !== "") {
      writer.uint32(10).string(message.queryId);
    }
    if (message.stageId !== 0) {
      writer.uint32(16).uint32(message.stageId);
    }
    if (message.taskId !== 0) {
      writer.uint32(24).uint32(message.taskId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TaskId {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTaskId();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.queryId = reader.string();
          break;
        case 2:
          message.stageId = reader.uint32();
          break;
        case 3:
          message.taskId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

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

function createBaseTaskOutputId(): TaskOutputId {
  return { taskId: undefined, outputId: 0 };
}

export const TaskOutputId = {
  encode(message: TaskOutputId, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskId !== undefined) {
      TaskId.encode(message.taskId, writer.uint32(10).fork()).ldelim();
    }
    if (message.outputId !== 0) {
      writer.uint32(16).uint32(message.outputId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TaskOutputId {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTaskOutputId();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskId = TaskId.decode(reader, reader.uint32());
          break;
        case 2:
          message.outputId = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TaskOutputId {
    return {
      taskId: isSet(object.taskId) ? TaskId.fromJSON(object.taskId) : undefined,
      outputId: isSet(object.outputId) ? Number(object.outputId) : 0,
    };
  },

  toJSON(message: TaskOutputId): unknown {
    const obj: any = {};
    message.taskId !== undefined && (obj.taskId = message.taskId ? TaskId.toJSON(message.taskId) : undefined);
    message.outputId !== undefined && (obj.outputId = Math.round(message.outputId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TaskOutputId>, I>>(object: I): TaskOutputId {
    const message = createBaseTaskOutputId();
    message.taskId = (object.taskId !== undefined && object.taskId !== null)
      ? TaskId.fromPartial(object.taskId)
      : undefined;
    message.outputId = object.outputId ?? 0;
    return message;
  },
};

function createBaseLocalExecutePlan(): LocalExecutePlan {
  return { plan: undefined, epoch: 0 };
}

export const LocalExecutePlan = {
  encode(message: LocalExecutePlan, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.plan !== undefined) {
      PlanFragment.encode(message.plan, writer.uint32(10).fork()).ldelim();
    }
    if (message.epoch !== 0) {
      writer.uint32(16).uint64(message.epoch);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LocalExecutePlan {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLocalExecutePlan();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.plan = PlanFragment.decode(reader, reader.uint32());
          break;
        case 2:
          message.epoch = longToNumber(reader.uint64() as Long);
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LocalExecutePlan {
    return {
      plan: isSet(object.plan) ? PlanFragment.fromJSON(object.plan) : undefined,
      epoch: isSet(object.epoch) ? Number(object.epoch) : 0,
    };
  },

  toJSON(message: LocalExecutePlan): unknown {
    const obj: any = {};
    message.plan !== undefined && (obj.plan = message.plan ? PlanFragment.toJSON(message.plan) : undefined);
    message.epoch !== undefined && (obj.epoch = Math.round(message.epoch));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LocalExecutePlan>, I>>(object: I): LocalExecutePlan {
    const message = createBaseLocalExecutePlan();
    message.plan = (object.plan !== undefined && object.plan !== null)
      ? PlanFragment.fromPartial(object.plan)
      : undefined;
    message.epoch = object.epoch ?? 0;
    return message;
  },
};

function createBaseExchangeSource(): ExchangeSource {
  return { taskOutputId: undefined, host: undefined, plan: undefined };
}

export const ExchangeSource = {
  encode(message: ExchangeSource, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.taskOutputId !== undefined) {
      TaskOutputId.encode(message.taskOutputId, writer.uint32(10).fork()).ldelim();
    }
    if (message.host !== undefined) {
      HostAddress.encode(message.host, writer.uint32(18).fork()).ldelim();
    }
    if (message.plan !== undefined) {
      LocalExecutePlan.encode(message.plan, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExchangeSource {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExchangeSource();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.taskOutputId = TaskOutputId.decode(reader, reader.uint32());
          break;
        case 2:
          message.host = HostAddress.decode(reader, reader.uint32());
          break;
        case 3:
          message.plan = LocalExecutePlan.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExchangeSource {
    return {
      taskOutputId: isSet(object.taskOutputId) ? TaskOutputId.fromJSON(object.taskOutputId) : undefined,
      host: isSet(object.host) ? HostAddress.fromJSON(object.host) : undefined,
      plan: isSet(object.plan) ? LocalExecutePlan.fromJSON(object.plan) : undefined,
    };
  },

  toJSON(message: ExchangeSource): unknown {
    const obj: any = {};
    message.taskOutputId !== undefined &&
      (obj.taskOutputId = message.taskOutputId ? TaskOutputId.toJSON(message.taskOutputId) : undefined);
    message.host !== undefined && (obj.host = message.host ? HostAddress.toJSON(message.host) : undefined);
    message.plan !== undefined && (obj.plan = message.plan ? LocalExecutePlan.toJSON(message.plan) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeSource>, I>>(object: I): ExchangeSource {
    const message = createBaseExchangeSource();
    message.taskOutputId = (object.taskOutputId !== undefined && object.taskOutputId !== null)
      ? TaskOutputId.fromPartial(object.taskOutputId)
      : undefined;
    message.host = (object.host !== undefined && object.host !== null)
      ? HostAddress.fromPartial(object.host)
      : undefined;
    message.plan = (object.plan !== undefined && object.plan !== null)
      ? LocalExecutePlan.fromPartial(object.plan)
      : undefined;
    return message;
  },
};

function createBaseExchangeNode(): ExchangeNode {
  return { sources: [], inputSchema: [] };
}

export const ExchangeNode = {
  encode(message: ExchangeNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.sources) {
      ExchangeSource.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.inputSchema) {
      Field.encode(v!, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExchangeNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExchangeNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.sources.push(ExchangeSource.decode(reader, reader.uint32()));
          break;
        case 3:
          message.inputSchema.push(Field.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExchangeNode {
    return {
      sources: Array.isArray(object?.sources) ? object.sources.map((e: any) => ExchangeSource.fromJSON(e)) : [],
      inputSchema: Array.isArray(object?.inputSchema) ? object.inputSchema.map((e: any) => Field.fromJSON(e)) : [],
    };
  },

  toJSON(message: ExchangeNode): unknown {
    const obj: any = {};
    if (message.sources) {
      obj.sources = message.sources.map((e) => e ? ExchangeSource.toJSON(e) : undefined);
    } else {
      obj.sources = [];
    }
    if (message.inputSchema) {
      obj.inputSchema = message.inputSchema.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.inputSchema = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeNode>, I>>(object: I): ExchangeNode {
    const message = createBaseExchangeNode();
    message.sources = object.sources?.map((e) => ExchangeSource.fromPartial(e)) || [];
    message.inputSchema = object.inputSchema?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseMergeSortExchangeNode(): MergeSortExchangeNode {
  return { exchange: undefined, columnOrders: [] };
}

export const MergeSortExchangeNode = {
  encode(message: MergeSortExchangeNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.exchange !== undefined) {
      ExchangeNode.encode(message.exchange, writer.uint32(10).fork()).ldelim();
    }
    for (const v of message.columnOrders) {
      ColumnOrder.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): MergeSortExchangeNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseMergeSortExchangeNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.exchange = ExchangeNode.decode(reader, reader.uint32());
          break;
        case 2:
          message.columnOrders.push(ColumnOrder.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): MergeSortExchangeNode {
    return {
      exchange: isSet(object.exchange) ? ExchangeNode.fromJSON(object.exchange) : undefined,
      columnOrders: Array.isArray(object?.columnOrders)
        ? object.columnOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
    };
  },

  toJSON(message: MergeSortExchangeNode): unknown {
    const obj: any = {};
    message.exchange !== undefined &&
      (obj.exchange = message.exchange ? ExchangeNode.toJSON(message.exchange) : undefined);
    if (message.columnOrders) {
      obj.columnOrders = message.columnOrders.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.columnOrders = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MergeSortExchangeNode>, I>>(object: I): MergeSortExchangeNode {
    const message = createBaseMergeSortExchangeNode();
    message.exchange = (object.exchange !== undefined && object.exchange !== null)
      ? ExchangeNode.fromPartial(object.exchange)
      : undefined;
    message.columnOrders = object.columnOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    return message;
  },
};

function createBaseLookupJoinNode(): LookupJoinNode {
  return {
    joinType: 0,
    condition: undefined,
    buildSideKey: [],
    probeSideTableDesc: undefined,
    probeSideVnodeMapping: [],
    probeSideColumnIds: [],
    outputIndices: [],
    workerNodes: [],
  };
}

export const LookupJoinNode = {
  encode(message: LookupJoinNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.joinType !== 0) {
      writer.uint32(8).int32(message.joinType);
    }
    if (message.condition !== undefined) {
      ExprNode.encode(message.condition, writer.uint32(18).fork()).ldelim();
    }
    writer.uint32(26).fork();
    for (const v of message.buildSideKey) {
      writer.uint32(v);
    }
    writer.ldelim();
    if (message.probeSideTableDesc !== undefined) {
      StorageTableDesc.encode(message.probeSideTableDesc, writer.uint32(34).fork()).ldelim();
    }
    writer.uint32(42).fork();
    for (const v of message.probeSideVnodeMapping) {
      writer.uint32(v);
    }
    writer.ldelim();
    writer.uint32(50).fork();
    for (const v of message.probeSideColumnIds) {
      writer.int32(v);
    }
    writer.ldelim();
    writer.uint32(58).fork();
    for (const v of message.outputIndices) {
      writer.uint32(v);
    }
    writer.ldelim();
    for (const v of message.workerNodes) {
      WorkerNode.encode(v!, writer.uint32(66).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): LookupJoinNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseLookupJoinNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.joinType = reader.int32() as any;
          break;
        case 2:
          message.condition = ExprNode.decode(reader, reader.uint32());
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.buildSideKey.push(reader.uint32());
            }
          } else {
            message.buildSideKey.push(reader.uint32());
          }
          break;
        case 4:
          message.probeSideTableDesc = StorageTableDesc.decode(reader, reader.uint32());
          break;
        case 5:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.probeSideVnodeMapping.push(reader.uint32());
            }
          } else {
            message.probeSideVnodeMapping.push(reader.uint32());
          }
          break;
        case 6:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.probeSideColumnIds.push(reader.int32());
            }
          } else {
            message.probeSideColumnIds.push(reader.int32());
          }
          break;
        case 7:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.outputIndices.push(reader.uint32());
            }
          } else {
            message.outputIndices.push(reader.uint32());
          }
          break;
        case 8:
          message.workerNodes.push(WorkerNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): LookupJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : 0,
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      buildSideKey: Array.isArray(object?.buildSideKey) ? object.buildSideKey.map((e: any) => Number(e)) : [],
      probeSideTableDesc: isSet(object.probeSideTableDesc)
        ? StorageTableDesc.fromJSON(object.probeSideTableDesc)
        : undefined,
      probeSideVnodeMapping: Array.isArray(object?.probeSideVnodeMapping)
        ? object.probeSideVnodeMapping.map((e: any) => Number(e))
        : [],
      probeSideColumnIds: Array.isArray(object?.probeSideColumnIds)
        ? object.probeSideColumnIds.map((e: any) => Number(e))
        : [],
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      workerNodes: Array.isArray(object?.workerNodes) ? object.workerNodes.map((e: any) => WorkerNode.fromJSON(e)) : [],
    };
  },

  toJSON(message: LookupJoinNode): unknown {
    const obj: any = {};
    message.joinType !== undefined && (obj.joinType = joinTypeToJSON(message.joinType));
    message.condition !== undefined &&
      (obj.condition = message.condition ? ExprNode.toJSON(message.condition) : undefined);
    if (message.buildSideKey) {
      obj.buildSideKey = message.buildSideKey.map((e) => Math.round(e));
    } else {
      obj.buildSideKey = [];
    }
    message.probeSideTableDesc !== undefined && (obj.probeSideTableDesc = message.probeSideTableDesc
      ? StorageTableDesc.toJSON(message.probeSideTableDesc)
      : undefined);
    if (message.probeSideVnodeMapping) {
      obj.probeSideVnodeMapping = message.probeSideVnodeMapping.map((e) => Math.round(e));
    } else {
      obj.probeSideVnodeMapping = [];
    }
    if (message.probeSideColumnIds) {
      obj.probeSideColumnIds = message.probeSideColumnIds.map((e) => Math.round(e));
    } else {
      obj.probeSideColumnIds = [];
    }
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    if (message.workerNodes) {
      obj.workerNodes = message.workerNodes.map((e) => e ? WorkerNode.toJSON(e) : undefined);
    } else {
      obj.workerNodes = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LookupJoinNode>, I>>(object: I): LookupJoinNode {
    const message = createBaseLookupJoinNode();
    message.joinType = object.joinType ?? 0;
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.buildSideKey = object.buildSideKey?.map((e) => e) || [];
    message.probeSideTableDesc = (object.probeSideTableDesc !== undefined && object.probeSideTableDesc !== null)
      ? StorageTableDesc.fromPartial(object.probeSideTableDesc)
      : undefined;
    message.probeSideVnodeMapping = object.probeSideVnodeMapping?.map((e) => e) || [];
    message.probeSideColumnIds = object.probeSideColumnIds?.map((e) => e) || [];
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.workerNodes = object.workerNodes?.map((e) => WorkerNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUnionNode(): UnionNode {
  return {};
}

export const UnionNode = {
  encode(_: UnionNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): UnionNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseUnionNode();
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

  fromJSON(_: any): UnionNode {
    return {};
  },

  toJSON(_: UnionNode): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UnionNode>, I>>(_: I): UnionNode {
    const message = createBaseUnionNode();
    return message;
  },
};

function createBasePlanNode(): PlanNode {
  return {
    children: [],
    insert: undefined,
    delete: undefined,
    update: undefined,
    project: undefined,
    hashAgg: undefined,
    filter: undefined,
    exchange: undefined,
    orderBy: undefined,
    nestedLoopJoin: undefined,
    topN: undefined,
    sortAgg: undefined,
    rowSeqScan: undefined,
    limit: undefined,
    values: undefined,
    hashJoin: undefined,
    mergeSortExchange: undefined,
    sortMergeJoin: undefined,
    hopWindow: undefined,
    tableFunction: undefined,
    sysRowSeqScan: undefined,
    expand: undefined,
    lookupJoin: undefined,
    projectSet: undefined,
    union: undefined,
    identity: "",
  };
}

export const PlanNode = {
  encode(message: PlanNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.children) {
      PlanNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    if (message.insert !== undefined) {
      InsertNode.encode(message.insert, writer.uint32(18).fork()).ldelim();
    }
    if (message.delete !== undefined) {
      DeleteNode.encode(message.delete, writer.uint32(26).fork()).ldelim();
    }
    if (message.update !== undefined) {
      UpdateNode.encode(message.update, writer.uint32(34).fork()).ldelim();
    }
    if (message.project !== undefined) {
      ProjectNode.encode(message.project, writer.uint32(42).fork()).ldelim();
    }
    if (message.hashAgg !== undefined) {
      HashAggNode.encode(message.hashAgg, writer.uint32(58).fork()).ldelim();
    }
    if (message.filter !== undefined) {
      FilterNode.encode(message.filter, writer.uint32(66).fork()).ldelim();
    }
    if (message.exchange !== undefined) {
      ExchangeNode.encode(message.exchange, writer.uint32(74).fork()).ldelim();
    }
    if (message.orderBy !== undefined) {
      OrderByNode.encode(message.orderBy, writer.uint32(82).fork()).ldelim();
    }
    if (message.nestedLoopJoin !== undefined) {
      NestedLoopJoinNode.encode(message.nestedLoopJoin, writer.uint32(90).fork()).ldelim();
    }
    if (message.topN !== undefined) {
      TopNNode.encode(message.topN, writer.uint32(114).fork()).ldelim();
    }
    if (message.sortAgg !== undefined) {
      SortAggNode.encode(message.sortAgg, writer.uint32(122).fork()).ldelim();
    }
    if (message.rowSeqScan !== undefined) {
      RowSeqScanNode.encode(message.rowSeqScan, writer.uint32(130).fork()).ldelim();
    }
    if (message.limit !== undefined) {
      LimitNode.encode(message.limit, writer.uint32(138).fork()).ldelim();
    }
    if (message.values !== undefined) {
      ValuesNode.encode(message.values, writer.uint32(146).fork()).ldelim();
    }
    if (message.hashJoin !== undefined) {
      HashJoinNode.encode(message.hashJoin, writer.uint32(154).fork()).ldelim();
    }
    if (message.mergeSortExchange !== undefined) {
      MergeSortExchangeNode.encode(message.mergeSortExchange, writer.uint32(170).fork()).ldelim();
    }
    if (message.sortMergeJoin !== undefined) {
      SortMergeJoinNode.encode(message.sortMergeJoin, writer.uint32(178).fork()).ldelim();
    }
    if (message.hopWindow !== undefined) {
      HopWindowNode.encode(message.hopWindow, writer.uint32(202).fork()).ldelim();
    }
    if (message.tableFunction !== undefined) {
      TableFunctionNode.encode(message.tableFunction, writer.uint32(210).fork()).ldelim();
    }
    if (message.sysRowSeqScan !== undefined) {
      SysRowSeqScanNode.encode(message.sysRowSeqScan, writer.uint32(218).fork()).ldelim();
    }
    if (message.expand !== undefined) {
      ExpandNode.encode(message.expand, writer.uint32(226).fork()).ldelim();
    }
    if (message.lookupJoin !== undefined) {
      LookupJoinNode.encode(message.lookupJoin, writer.uint32(234).fork()).ldelim();
    }
    if (message.projectSet !== undefined) {
      ProjectSetNode.encode(message.projectSet, writer.uint32(242).fork()).ldelim();
    }
    if (message.union !== undefined) {
      UnionNode.encode(message.union, writer.uint32(250).fork()).ldelim();
    }
    if (message.identity !== "") {
      writer.uint32(194).string(message.identity);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PlanNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePlanNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.children.push(PlanNode.decode(reader, reader.uint32()));
          break;
        case 2:
          message.insert = InsertNode.decode(reader, reader.uint32());
          break;
        case 3:
          message.delete = DeleteNode.decode(reader, reader.uint32());
          break;
        case 4:
          message.update = UpdateNode.decode(reader, reader.uint32());
          break;
        case 5:
          message.project = ProjectNode.decode(reader, reader.uint32());
          break;
        case 7:
          message.hashAgg = HashAggNode.decode(reader, reader.uint32());
          break;
        case 8:
          message.filter = FilterNode.decode(reader, reader.uint32());
          break;
        case 9:
          message.exchange = ExchangeNode.decode(reader, reader.uint32());
          break;
        case 10:
          message.orderBy = OrderByNode.decode(reader, reader.uint32());
          break;
        case 11:
          message.nestedLoopJoin = NestedLoopJoinNode.decode(reader, reader.uint32());
          break;
        case 14:
          message.topN = TopNNode.decode(reader, reader.uint32());
          break;
        case 15:
          message.sortAgg = SortAggNode.decode(reader, reader.uint32());
          break;
        case 16:
          message.rowSeqScan = RowSeqScanNode.decode(reader, reader.uint32());
          break;
        case 17:
          message.limit = LimitNode.decode(reader, reader.uint32());
          break;
        case 18:
          message.values = ValuesNode.decode(reader, reader.uint32());
          break;
        case 19:
          message.hashJoin = HashJoinNode.decode(reader, reader.uint32());
          break;
        case 21:
          message.mergeSortExchange = MergeSortExchangeNode.decode(reader, reader.uint32());
          break;
        case 22:
          message.sortMergeJoin = SortMergeJoinNode.decode(reader, reader.uint32());
          break;
        case 25:
          message.hopWindow = HopWindowNode.decode(reader, reader.uint32());
          break;
        case 26:
          message.tableFunction = TableFunctionNode.decode(reader, reader.uint32());
          break;
        case 27:
          message.sysRowSeqScan = SysRowSeqScanNode.decode(reader, reader.uint32());
          break;
        case 28:
          message.expand = ExpandNode.decode(reader, reader.uint32());
          break;
        case 29:
          message.lookupJoin = LookupJoinNode.decode(reader, reader.uint32());
          break;
        case 30:
          message.projectSet = ProjectSetNode.decode(reader, reader.uint32());
          break;
        case 31:
          message.union = UnionNode.decode(reader, reader.uint32());
          break;
        case 24:
          message.identity = reader.string();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PlanNode {
    return {
      children: Array.isArray(object?.children) ? object.children.map((e: any) => PlanNode.fromJSON(e)) : [],
      insert: isSet(object.insert) ? InsertNode.fromJSON(object.insert) : undefined,
      delete: isSet(object.delete) ? DeleteNode.fromJSON(object.delete) : undefined,
      update: isSet(object.update) ? UpdateNode.fromJSON(object.update) : undefined,
      project: isSet(object.project) ? ProjectNode.fromJSON(object.project) : undefined,
      hashAgg: isSet(object.hashAgg) ? HashAggNode.fromJSON(object.hashAgg) : undefined,
      filter: isSet(object.filter) ? FilterNode.fromJSON(object.filter) : undefined,
      exchange: isSet(object.exchange) ? ExchangeNode.fromJSON(object.exchange) : undefined,
      orderBy: isSet(object.orderBy) ? OrderByNode.fromJSON(object.orderBy) : undefined,
      nestedLoopJoin: isSet(object.nestedLoopJoin) ? NestedLoopJoinNode.fromJSON(object.nestedLoopJoin) : undefined,
      topN: isSet(object.topN) ? TopNNode.fromJSON(object.topN) : undefined,
      sortAgg: isSet(object.sortAgg) ? SortAggNode.fromJSON(object.sortAgg) : undefined,
      rowSeqScan: isSet(object.rowSeqScan) ? RowSeqScanNode.fromJSON(object.rowSeqScan) : undefined,
      limit: isSet(object.limit) ? LimitNode.fromJSON(object.limit) : undefined,
      values: isSet(object.values) ? ValuesNode.fromJSON(object.values) : undefined,
      hashJoin: isSet(object.hashJoin) ? HashJoinNode.fromJSON(object.hashJoin) : undefined,
      mergeSortExchange: isSet(object.mergeSortExchange)
        ? MergeSortExchangeNode.fromJSON(object.mergeSortExchange)
        : undefined,
      sortMergeJoin: isSet(object.sortMergeJoin) ? SortMergeJoinNode.fromJSON(object.sortMergeJoin) : undefined,
      hopWindow: isSet(object.hopWindow) ? HopWindowNode.fromJSON(object.hopWindow) : undefined,
      tableFunction: isSet(object.tableFunction) ? TableFunctionNode.fromJSON(object.tableFunction) : undefined,
      sysRowSeqScan: isSet(object.sysRowSeqScan) ? SysRowSeqScanNode.fromJSON(object.sysRowSeqScan) : undefined,
      expand: isSet(object.expand) ? ExpandNode.fromJSON(object.expand) : undefined,
      lookupJoin: isSet(object.lookupJoin) ? LookupJoinNode.fromJSON(object.lookupJoin) : undefined,
      projectSet: isSet(object.projectSet) ? ProjectSetNode.fromJSON(object.projectSet) : undefined,
      union: isSet(object.union) ? UnionNode.fromJSON(object.union) : undefined,
      identity: isSet(object.identity) ? String(object.identity) : "",
    };
  },

  toJSON(message: PlanNode): unknown {
    const obj: any = {};
    if (message.children) {
      obj.children = message.children.map((e) => e ? PlanNode.toJSON(e) : undefined);
    } else {
      obj.children = [];
    }
    message.insert !== undefined && (obj.insert = message.insert ? InsertNode.toJSON(message.insert) : undefined);
    message.delete !== undefined && (obj.delete = message.delete ? DeleteNode.toJSON(message.delete) : undefined);
    message.update !== undefined && (obj.update = message.update ? UpdateNode.toJSON(message.update) : undefined);
    message.project !== undefined && (obj.project = message.project ? ProjectNode.toJSON(message.project) : undefined);
    message.hashAgg !== undefined && (obj.hashAgg = message.hashAgg ? HashAggNode.toJSON(message.hashAgg) : undefined);
    message.filter !== undefined && (obj.filter = message.filter ? FilterNode.toJSON(message.filter) : undefined);
    message.exchange !== undefined &&
      (obj.exchange = message.exchange ? ExchangeNode.toJSON(message.exchange) : undefined);
    message.orderBy !== undefined && (obj.orderBy = message.orderBy ? OrderByNode.toJSON(message.orderBy) : undefined);
    message.nestedLoopJoin !== undefined &&
      (obj.nestedLoopJoin = message.nestedLoopJoin ? NestedLoopJoinNode.toJSON(message.nestedLoopJoin) : undefined);
    message.topN !== undefined && (obj.topN = message.topN ? TopNNode.toJSON(message.topN) : undefined);
    message.sortAgg !== undefined && (obj.sortAgg = message.sortAgg ? SortAggNode.toJSON(message.sortAgg) : undefined);
    message.rowSeqScan !== undefined &&
      (obj.rowSeqScan = message.rowSeqScan ? RowSeqScanNode.toJSON(message.rowSeqScan) : undefined);
    message.limit !== undefined && (obj.limit = message.limit ? LimitNode.toJSON(message.limit) : undefined);
    message.values !== undefined && (obj.values = message.values ? ValuesNode.toJSON(message.values) : undefined);
    message.hashJoin !== undefined &&
      (obj.hashJoin = message.hashJoin ? HashJoinNode.toJSON(message.hashJoin) : undefined);
    message.mergeSortExchange !== undefined && (obj.mergeSortExchange = message.mergeSortExchange
      ? MergeSortExchangeNode.toJSON(message.mergeSortExchange)
      : undefined);
    message.sortMergeJoin !== undefined &&
      (obj.sortMergeJoin = message.sortMergeJoin ? SortMergeJoinNode.toJSON(message.sortMergeJoin) : undefined);
    message.hopWindow !== undefined &&
      (obj.hopWindow = message.hopWindow ? HopWindowNode.toJSON(message.hopWindow) : undefined);
    message.tableFunction !== undefined &&
      (obj.tableFunction = message.tableFunction ? TableFunctionNode.toJSON(message.tableFunction) : undefined);
    message.sysRowSeqScan !== undefined &&
      (obj.sysRowSeqScan = message.sysRowSeqScan ? SysRowSeqScanNode.toJSON(message.sysRowSeqScan) : undefined);
    message.expand !== undefined && (obj.expand = message.expand ? ExpandNode.toJSON(message.expand) : undefined);
    message.lookupJoin !== undefined &&
      (obj.lookupJoin = message.lookupJoin ? LookupJoinNode.toJSON(message.lookupJoin) : undefined);
    message.projectSet !== undefined &&
      (obj.projectSet = message.projectSet ? ProjectSetNode.toJSON(message.projectSet) : undefined);
    message.union !== undefined && (obj.union = message.union ? UnionNode.toJSON(message.union) : undefined);
    message.identity !== undefined && (obj.identity = message.identity);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PlanNode>, I>>(object: I): PlanNode {
    const message = createBasePlanNode();
    message.children = object.children?.map((e) => PlanNode.fromPartial(e)) || [];
    message.insert = (object.insert !== undefined && object.insert !== null)
      ? InsertNode.fromPartial(object.insert)
      : undefined;
    message.delete = (object.delete !== undefined && object.delete !== null)
      ? DeleteNode.fromPartial(object.delete)
      : undefined;
    message.update = (object.update !== undefined && object.update !== null)
      ? UpdateNode.fromPartial(object.update)
      : undefined;
    message.project = (object.project !== undefined && object.project !== null)
      ? ProjectNode.fromPartial(object.project)
      : undefined;
    message.hashAgg = (object.hashAgg !== undefined && object.hashAgg !== null)
      ? HashAggNode.fromPartial(object.hashAgg)
      : undefined;
    message.filter = (object.filter !== undefined && object.filter !== null)
      ? FilterNode.fromPartial(object.filter)
      : undefined;
    message.exchange = (object.exchange !== undefined && object.exchange !== null)
      ? ExchangeNode.fromPartial(object.exchange)
      : undefined;
    message.orderBy = (object.orderBy !== undefined && object.orderBy !== null)
      ? OrderByNode.fromPartial(object.orderBy)
      : undefined;
    message.nestedLoopJoin = (object.nestedLoopJoin !== undefined && object.nestedLoopJoin !== null)
      ? NestedLoopJoinNode.fromPartial(object.nestedLoopJoin)
      : undefined;
    message.topN = (object.topN !== undefined && object.topN !== null) ? TopNNode.fromPartial(object.topN) : undefined;
    message.sortAgg = (object.sortAgg !== undefined && object.sortAgg !== null)
      ? SortAggNode.fromPartial(object.sortAgg)
      : undefined;
    message.rowSeqScan = (object.rowSeqScan !== undefined && object.rowSeqScan !== null)
      ? RowSeqScanNode.fromPartial(object.rowSeqScan)
      : undefined;
    message.limit = (object.limit !== undefined && object.limit !== null)
      ? LimitNode.fromPartial(object.limit)
      : undefined;
    message.values = (object.values !== undefined && object.values !== null)
      ? ValuesNode.fromPartial(object.values)
      : undefined;
    message.hashJoin = (object.hashJoin !== undefined && object.hashJoin !== null)
      ? HashJoinNode.fromPartial(object.hashJoin)
      : undefined;
    message.mergeSortExchange = (object.mergeSortExchange !== undefined && object.mergeSortExchange !== null)
      ? MergeSortExchangeNode.fromPartial(object.mergeSortExchange)
      : undefined;
    message.sortMergeJoin = (object.sortMergeJoin !== undefined && object.sortMergeJoin !== null)
      ? SortMergeJoinNode.fromPartial(object.sortMergeJoin)
      : undefined;
    message.hopWindow = (object.hopWindow !== undefined && object.hopWindow !== null)
      ? HopWindowNode.fromPartial(object.hopWindow)
      : undefined;
    message.tableFunction = (object.tableFunction !== undefined && object.tableFunction !== null)
      ? TableFunctionNode.fromPartial(object.tableFunction)
      : undefined;
    message.sysRowSeqScan = (object.sysRowSeqScan !== undefined && object.sysRowSeqScan !== null)
      ? SysRowSeqScanNode.fromPartial(object.sysRowSeqScan)
      : undefined;
    message.expand = (object.expand !== undefined && object.expand !== null)
      ? ExpandNode.fromPartial(object.expand)
      : undefined;
    message.lookupJoin = (object.lookupJoin !== undefined && object.lookupJoin !== null)
      ? LookupJoinNode.fromPartial(object.lookupJoin)
      : undefined;
    message.projectSet = (object.projectSet !== undefined && object.projectSet !== null)
      ? ProjectSetNode.fromPartial(object.projectSet)
      : undefined;
    message.union = (object.union !== undefined && object.union !== null)
      ? UnionNode.fromPartial(object.union)
      : undefined;
    message.identity = object.identity ?? "";
    return message;
  },
};

function createBaseExchangeInfo(): ExchangeInfo {
  return { mode: 0, broadcastInfo: undefined, hashInfo: undefined };
}

export const ExchangeInfo = {
  encode(message: ExchangeInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.mode !== 0) {
      writer.uint32(8).int32(message.mode);
    }
    if (message.broadcastInfo !== undefined) {
      ExchangeInfo_BroadcastInfo.encode(message.broadcastInfo, writer.uint32(18).fork()).ldelim();
    }
    if (message.hashInfo !== undefined) {
      ExchangeInfo_HashInfo.encode(message.hashInfo, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExchangeInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExchangeInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.mode = reader.int32() as any;
          break;
        case 2:
          message.broadcastInfo = ExchangeInfo_BroadcastInfo.decode(reader, reader.uint32());
          break;
        case 3:
          message.hashInfo = ExchangeInfo_HashInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExchangeInfo {
    return {
      mode: isSet(object.mode) ? exchangeInfo_DistributionModeFromJSON(object.mode) : 0,
      broadcastInfo: isSet(object.broadcastInfo)
        ? ExchangeInfo_BroadcastInfo.fromJSON(object.broadcastInfo)
        : undefined,
      hashInfo: isSet(object.hashInfo) ? ExchangeInfo_HashInfo.fromJSON(object.hashInfo) : undefined,
    };
  },

  toJSON(message: ExchangeInfo): unknown {
    const obj: any = {};
    message.mode !== undefined && (obj.mode = exchangeInfo_DistributionModeToJSON(message.mode));
    message.broadcastInfo !== undefined &&
      (obj.broadcastInfo = message.broadcastInfo
        ? ExchangeInfo_BroadcastInfo.toJSON(message.broadcastInfo)
        : undefined);
    message.hashInfo !== undefined &&
      (obj.hashInfo = message.hashInfo ? ExchangeInfo_HashInfo.toJSON(message.hashInfo) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeInfo>, I>>(object: I): ExchangeInfo {
    const message = createBaseExchangeInfo();
    message.mode = object.mode ?? 0;
    message.broadcastInfo = (object.broadcastInfo !== undefined && object.broadcastInfo !== null)
      ? ExchangeInfo_BroadcastInfo.fromPartial(object.broadcastInfo)
      : undefined;
    message.hashInfo = (object.hashInfo !== undefined && object.hashInfo !== null)
      ? ExchangeInfo_HashInfo.fromPartial(object.hashInfo)
      : undefined;
    return message;
  },
};

function createBaseExchangeInfo_BroadcastInfo(): ExchangeInfo_BroadcastInfo {
  return { count: 0 };
}

export const ExchangeInfo_BroadcastInfo = {
  encode(message: ExchangeInfo_BroadcastInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.count !== 0) {
      writer.uint32(8).uint32(message.count);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExchangeInfo_BroadcastInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExchangeInfo_BroadcastInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.count = reader.uint32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExchangeInfo_BroadcastInfo {
    return { count: isSet(object.count) ? Number(object.count) : 0 };
  },

  toJSON(message: ExchangeInfo_BroadcastInfo): unknown {
    const obj: any = {};
    message.count !== undefined && (obj.count = Math.round(message.count));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeInfo_BroadcastInfo>, I>>(object: I): ExchangeInfo_BroadcastInfo {
    const message = createBaseExchangeInfo_BroadcastInfo();
    message.count = object.count ?? 0;
    return message;
  },
};

function createBaseExchangeInfo_HashInfo(): ExchangeInfo_HashInfo {
  return { outputCount: 0, key: [] };
}

export const ExchangeInfo_HashInfo = {
  encode(message: ExchangeInfo_HashInfo, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.outputCount !== 0) {
      writer.uint32(8).uint32(message.outputCount);
    }
    writer.uint32(26).fork();
    for (const v of message.key) {
      writer.uint32(v);
    }
    writer.ldelim();
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExchangeInfo_HashInfo {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExchangeInfo_HashInfo();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.outputCount = reader.uint32();
          break;
        case 3:
          if ((tag & 7) === 2) {
            const end2 = reader.uint32() + reader.pos;
            while (reader.pos < end2) {
              message.key.push(reader.uint32());
            }
          } else {
            message.key.push(reader.uint32());
          }
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExchangeInfo_HashInfo {
    return {
      outputCount: isSet(object.outputCount) ? Number(object.outputCount) : 0,
      key: Array.isArray(object?.key) ? object.key.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ExchangeInfo_HashInfo): unknown {
    const obj: any = {};
    message.outputCount !== undefined && (obj.outputCount = Math.round(message.outputCount));
    if (message.key) {
      obj.key = message.key.map((e) => Math.round(e));
    } else {
      obj.key = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeInfo_HashInfo>, I>>(object: I): ExchangeInfo_HashInfo {
    const message = createBaseExchangeInfo_HashInfo();
    message.outputCount = object.outputCount ?? 0;
    message.key = object.key?.map((e) => e) || [];
    return message;
  },
};

function createBasePlanFragment(): PlanFragment {
  return { root: undefined, exchangeInfo: undefined };
}

export const PlanFragment = {
  encode(message: PlanFragment, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.root !== undefined) {
      PlanNode.encode(message.root, writer.uint32(10).fork()).ldelim();
    }
    if (message.exchangeInfo !== undefined) {
      ExchangeInfo.encode(message.exchangeInfo, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): PlanFragment {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBasePlanFragment();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.root = PlanNode.decode(reader, reader.uint32());
          break;
        case 2:
          message.exchangeInfo = ExchangeInfo.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): PlanFragment {
    return {
      root: isSet(object.root) ? PlanNode.fromJSON(object.root) : undefined,
      exchangeInfo: isSet(object.exchangeInfo) ? ExchangeInfo.fromJSON(object.exchangeInfo) : undefined,
    };
  },

  toJSON(message: PlanFragment): unknown {
    const obj: any = {};
    message.root !== undefined && (obj.root = message.root ? PlanNode.toJSON(message.root) : undefined);
    message.exchangeInfo !== undefined &&
      (obj.exchangeInfo = message.exchangeInfo ? ExchangeInfo.toJSON(message.exchangeInfo) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PlanFragment>, I>>(object: I): PlanFragment {
    const message = createBasePlanFragment();
    message.root = (object.root !== undefined && object.root !== null) ? PlanNode.fromPartial(object.root) : undefined;
    message.exchangeInfo = (object.exchangeInfo !== undefined && object.exchangeInfo !== null)
      ? ExchangeInfo.fromPartial(object.exchangeInfo)
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
