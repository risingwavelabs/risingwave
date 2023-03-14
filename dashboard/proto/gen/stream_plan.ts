/* eslint-disable */
import { SinkType, sinkTypeFromJSON, sinkTypeToJSON, StreamSourceInfo, Table, WatermarkDesc } from "./catalog";
import { Buffer, ColumnOrder } from "./common";
import { Datum, Epoch, IntervalUnit, StreamChunk } from "./data";
import { AggCall, ExprNode, InputRef, ProjectSetSelectItem } from "./expr";
import {
  ColumnCatalog,
  ColumnDesc,
  Field,
  JoinType,
  joinTypeFromJSON,
  joinTypeToJSON,
  StorageTableDesc,
} from "./plan_common";
import { ConnectorSplits } from "./source";

export const protobufPackage = "stream_plan";

export const HandleConflictBehavior = {
  NO_CHECK_UNSPECIFIED: "NO_CHECK_UNSPECIFIED",
  OVERWRITE: "OVERWRITE",
  IGNORE: "IGNORE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type HandleConflictBehavior = typeof HandleConflictBehavior[keyof typeof HandleConflictBehavior];

export function handleConflictBehaviorFromJSON(object: any): HandleConflictBehavior {
  switch (object) {
    case 0:
    case "NO_CHECK_UNSPECIFIED":
      return HandleConflictBehavior.NO_CHECK_UNSPECIFIED;
    case 1:
    case "OVERWRITE":
      return HandleConflictBehavior.OVERWRITE;
    case 2:
    case "IGNORE":
      return HandleConflictBehavior.IGNORE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return HandleConflictBehavior.UNRECOGNIZED;
  }
}

export function handleConflictBehaviorToJSON(object: HandleConflictBehavior): string {
  switch (object) {
    case HandleConflictBehavior.NO_CHECK_UNSPECIFIED:
      return "NO_CHECK_UNSPECIFIED";
    case HandleConflictBehavior.OVERWRITE:
      return "OVERWRITE";
    case HandleConflictBehavior.IGNORE:
      return "IGNORE";
    case HandleConflictBehavior.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const ChainType = {
  CHAIN_UNSPECIFIED: "CHAIN_UNSPECIFIED",
  /** CHAIN - CHAIN is corresponding to the chain executor. */
  CHAIN: "CHAIN",
  /** REARRANGE - REARRANGE is corresponding to the rearranged chain executor. */
  REARRANGE: "REARRANGE",
  /** BACKFILL - BACKFILL is corresponding to the backfill executor. */
  BACKFILL: "BACKFILL",
  /** UPSTREAM_ONLY - UPSTREAM_ONLY is corresponding to the chain executor, but doesn't consume the snapshot. */
  UPSTREAM_ONLY: "UPSTREAM_ONLY",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type ChainType = typeof ChainType[keyof typeof ChainType];

export function chainTypeFromJSON(object: any): ChainType {
  switch (object) {
    case 0:
    case "CHAIN_UNSPECIFIED":
      return ChainType.CHAIN_UNSPECIFIED;
    case 1:
    case "CHAIN":
      return ChainType.CHAIN;
    case 2:
    case "REARRANGE":
      return ChainType.REARRANGE;
    case 3:
    case "BACKFILL":
      return ChainType.BACKFILL;
    case 4:
    case "UPSTREAM_ONLY":
      return ChainType.UPSTREAM_ONLY;
    case -1:
    case "UNRECOGNIZED":
    default:
      return ChainType.UNRECOGNIZED;
  }
}

export function chainTypeToJSON(object: ChainType): string {
  switch (object) {
    case ChainType.CHAIN_UNSPECIFIED:
      return "CHAIN_UNSPECIFIED";
    case ChainType.CHAIN:
      return "CHAIN";
    case ChainType.REARRANGE:
      return "REARRANGE";
    case ChainType.BACKFILL:
      return "BACKFILL";
    case ChainType.UPSTREAM_ONLY:
      return "UPSTREAM_ONLY";
    case ChainType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const DispatcherType = {
  UNSPECIFIED: "UNSPECIFIED",
  /** HASH - Dispatch by hash key, hashed by consistent hash. */
  HASH: "HASH",
  /**
   * BROADCAST - Broadcast to all downstreams.
   *
   * Note a broadcast cannot be represented as multiple simple dispatchers, since they are
   * different when we update dispatchers during scaling.
   */
  BROADCAST: "BROADCAST",
  /** SIMPLE - Only one downstream. */
  SIMPLE: "SIMPLE",
  /**
   * NO_SHUFFLE - A special kind of exchange that doesn't involve shuffle. The upstream actor will be directly
   * piped into the downstream actor, if there are the same number of actors. If number of actors
   * are not the same, should use hash instead. Should be only used when distribution is the same.
   */
  NO_SHUFFLE: "NO_SHUFFLE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type DispatcherType = typeof DispatcherType[keyof typeof DispatcherType];

export function dispatcherTypeFromJSON(object: any): DispatcherType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return DispatcherType.UNSPECIFIED;
    case 1:
    case "HASH":
      return DispatcherType.HASH;
    case 2:
    case "BROADCAST":
      return DispatcherType.BROADCAST;
    case 3:
    case "SIMPLE":
      return DispatcherType.SIMPLE;
    case 4:
    case "NO_SHUFFLE":
      return DispatcherType.NO_SHUFFLE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return DispatcherType.UNRECOGNIZED;
  }
}

export function dispatcherTypeToJSON(object: DispatcherType): string {
  switch (object) {
    case DispatcherType.UNSPECIFIED:
      return "UNSPECIFIED";
    case DispatcherType.HASH:
      return "HASH";
    case DispatcherType.BROADCAST:
      return "BROADCAST";
    case DispatcherType.SIMPLE:
      return "SIMPLE";
    case DispatcherType.NO_SHUFFLE:
      return "NO_SHUFFLE";
    case DispatcherType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export const FragmentTypeFlag = {
  FRAGMENT_UNSPECIFIED: "FRAGMENT_UNSPECIFIED",
  SOURCE: "SOURCE",
  MVIEW: "MVIEW",
  SINK: "SINK",
  NOW: "NOW",
  CHAIN_NODE: "CHAIN_NODE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type FragmentTypeFlag = typeof FragmentTypeFlag[keyof typeof FragmentTypeFlag];

export function fragmentTypeFlagFromJSON(object: any): FragmentTypeFlag {
  switch (object) {
    case 0:
    case "FRAGMENT_UNSPECIFIED":
      return FragmentTypeFlag.FRAGMENT_UNSPECIFIED;
    case 1:
    case "SOURCE":
      return FragmentTypeFlag.SOURCE;
    case 2:
    case "MVIEW":
      return FragmentTypeFlag.MVIEW;
    case 4:
    case "SINK":
      return FragmentTypeFlag.SINK;
    case 8:
    case "NOW":
      return FragmentTypeFlag.NOW;
    case 16:
    case "CHAIN_NODE":
      return FragmentTypeFlag.CHAIN_NODE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return FragmentTypeFlag.UNRECOGNIZED;
  }
}

export function fragmentTypeFlagToJSON(object: FragmentTypeFlag): string {
  switch (object) {
    case FragmentTypeFlag.FRAGMENT_UNSPECIFIED:
      return "FRAGMENT_UNSPECIFIED";
    case FragmentTypeFlag.SOURCE:
      return "SOURCE";
    case FragmentTypeFlag.MVIEW:
      return "MVIEW";
    case FragmentTypeFlag.SINK:
      return "SINK";
    case FragmentTypeFlag.NOW:
      return "NOW";
    case FragmentTypeFlag.CHAIN_NODE:
      return "CHAIN_NODE";
    case FragmentTypeFlag.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface AddMutation {
  /** New dispatchers for each actor. */
  actorDispatchers: { [key: number]: AddMutation_Dispatchers };
  /**
   * We may embed a source change split mutation here.
   * TODO: we may allow multiple mutations in a single barrier.
   */
  actorSplits: { [key: number]: ConnectorSplits };
}

export interface AddMutation_Dispatchers {
  dispatchers: Dispatcher[];
}

export interface AddMutation_ActorDispatchersEntry {
  key: number;
  value: AddMutation_Dispatchers | undefined;
}

export interface AddMutation_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

export interface StopMutation {
  actors: number[];
}

export interface UpdateMutation {
  /** Dispatcher updates. */
  dispatcherUpdate: UpdateMutation_DispatcherUpdate[];
  /** Merge updates. */
  mergeUpdate: UpdateMutation_MergeUpdate[];
  /** Vnode bitmap updates for each actor. */
  actorVnodeBitmapUpdate: { [key: number]: Buffer };
  /** All actors to be dropped in this update. */
  droppedActors: number[];
  /** Source updates. */
  actorSplits: { [key: number]: ConnectorSplits };
}

export interface UpdateMutation_DispatcherUpdate {
  /** Dispatcher can be uniquely identified by a combination of actor id and dispatcher id. */
  actorId: number;
  dispatcherId: number;
  /**
   * The hash mapping for consistent hash.
   * For dispatcher types other than HASH, this is ignored.
   */
  hashMapping:
    | ActorMapping
    | undefined;
  /** Added downstream actors. */
  addedDownstreamActorId: number[];
  /** Removed downstream actors. */
  removedDownstreamActorId: number[];
}

export interface UpdateMutation_MergeUpdate {
  /** Merge executor can be uniquely identified by a combination of actor id and upstream fragment id. */
  actorId: number;
  upstreamFragmentId: number;
  /**
   * - For scaling, this is always `None`.
   * - For plan change, the upstream fragment will be changed to a new one, and this will be `Some`.
   *   In this case, all the upstream actors should be removed and replaced by the `new` ones.
   */
  newUpstreamFragmentId?:
    | number
    | undefined;
  /** Added upstream actors. */
  addedUpstreamActorId: number[];
  /** Removed upstream actors. */
  removedUpstreamActorId: number[];
}

export interface UpdateMutation_ActorVnodeBitmapUpdateEntry {
  key: number;
  value: Buffer | undefined;
}

export interface UpdateMutation_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

export interface SourceChangeSplitMutation {
  actorSplits: { [key: number]: ConnectorSplits };
}

export interface SourceChangeSplitMutation_ActorSplitsEntry {
  key: number;
  value: ConnectorSplits | undefined;
}

export interface PauseMutation {
}

export interface ResumeMutation {
}

export interface Barrier {
  epoch: Epoch | undefined;
  mutation?:
    | { $case: "add"; add: AddMutation }
    | { $case: "stop"; stop: StopMutation }
    | { $case: "update"; update: UpdateMutation }
    | { $case: "splits"; splits: SourceChangeSplitMutation }
    | { $case: "pause"; pause: PauseMutation }
    | { $case: "resume"; resume: ResumeMutation };
  /** Used for tracing. */
  span: Uint8Array;
  /** Whether this barrier do checkpoint */
  checkpoint: boolean;
  /** Record the actors that the barrier has passed. Only used for debugging. */
  passedActors: number[];
}

export interface Watermark {
  /** The reference to the watermark column in the stream's schema. */
  column:
    | InputRef
    | undefined;
  /** The watermark value, there will be no record having a greater value in the watermark column. */
  val: Datum | undefined;
}

export interface StreamMessage {
  streamMessage?: { $case: "streamChunk"; streamChunk: StreamChunk } | { $case: "barrier"; barrier: Barrier } | {
    $case: "watermark";
    watermark: Watermark;
  };
}

/** Hash mapping for compute node. Stores mapping from virtual node to actor id. */
export interface ActorMapping {
  originalIndices: number[];
  data: number[];
}

export interface StreamSource {
  sourceId: number;
  stateTable: Table | undefined;
  rowIdIndex?: number | undefined;
  columns: ColumnCatalog[];
  pkColumnIds: number[];
  properties: { [key: string]: string };
  info: StreamSourceInfo | undefined;
  sourceName: string;
}

export interface StreamSource_PropertiesEntry {
  key: string;
  value: string;
}

export interface SourceNode {
  /**
   * The source node can contain either a stream source or nothing. So here we extract all
   * information about stream source to a message, and here it will be an `Option` in Rust.
   */
  sourceInner: StreamSource | undefined;
}

export interface SinkDesc {
  id: number;
  name: string;
  definition: string;
  columns: ColumnDesc[];
  pk: ColumnOrder[];
  streamKey: number[];
  distributionKey: number[];
  properties: { [key: string]: string };
  sinkType: SinkType;
}

export interface SinkDesc_PropertiesEntry {
  key: string;
  value: string;
}

export interface SinkNode {
  sinkDesc: SinkDesc | undefined;
}

export interface ProjectNode {
  selectList: ExprNode[];
  /**
   * this two field is expressing a list of usize pair, which means when project receives a
   * watermark with `watermark_input_key[i]` column index, it should derive a new watermark
   * with `watermark_output_key[i]`th expression
   */
  watermarkInputKey: number[];
  watermarkOutputKey: number[];
}

export interface FilterNode {
  searchCondition: ExprNode | undefined;
}

/**
 * A materialized view is regarded as a table.
 * In addition, we also specify primary key to MV for efficient point lookup during update and deletion.
 *
 * The node will be used for both create mv and create index.
 * - When creating mv, `pk == distribution_key == column_orders`.
 * - When creating index, `column_orders` will contain both
 *   arrange columns and pk columns, while distribution key will be arrange columns.
 */
export interface MaterializeNode {
  tableId: number;
  /** Column indexes and orders of primary key. */
  columnOrders: ColumnOrder[];
  /** Used for internal table states. */
  table:
    | Table
    | undefined;
  /** Used to handle pk conflict, open it when upstream executor is source executor. */
  handlePkConflictBehavior: HandleConflictBehavior;
}

export interface AggCallState {
  inner?: { $case: "resultValueState"; resultValueState: AggCallState_ResultValueState } | {
    $case: "tableState";
    tableState: AggCallState_TableState;
  } | { $case: "materializedInputState"; materializedInputState: AggCallState_MaterializedInputState };
}

/** use the one column of stream's result table as the AggCall's state, used for count/sum/append-only extreme. */
export interface AggCallState_ResultValueState {
}

/** use untransformed result as the AggCall's state, used for append-only approx count distinct. */
export interface AggCallState_TableState {
  table: Table | undefined;
}

/** use the some column of the Upstream's materialization as the AggCall's state, used for extreme/string_agg/array_agg. */
export interface AggCallState_MaterializedInputState {
  table:
    | Table
    | undefined;
  /** for constructing state table column mapping */
  includedUpstreamIndices: number[];
  tableValueIndices: number[];
}

export interface SimpleAggNode {
  aggCalls: AggCall[];
  /** Only used for local simple agg, not used for global simple agg. */
  distributionKey: number[];
  aggCallStates: AggCallState[];
  resultTable:
    | Table
    | undefined;
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
  distinctDedupTables: { [key: number]: Table };
  rowCountIndex: number;
}

export interface SimpleAggNode_DistinctDedupTablesEntry {
  key: number;
  value: Table | undefined;
}

export interface HashAggNode {
  groupKey: number[];
  aggCalls: AggCall[];
  aggCallStates: AggCallState[];
  resultTable:
    | Table
    | undefined;
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
  distinctDedupTables: { [key: number]: Table };
  rowCountIndex: number;
}

export interface HashAggNode_DistinctDedupTablesEntry {
  key: number;
  value: Table | undefined;
}

export interface TopNNode {
  /** 0 means no limit as limit of 0 means this node should be optimized away */
  limit: number;
  offset: number;
  table: Table | undefined;
  orderBy: ColumnOrder[];
  withTies: boolean;
}

export interface GroupTopNNode {
  /** 0 means no limit as limit of 0 means this node should be optimized away */
  limit: number;
  offset: number;
  groupKey: number[];
  table: Table | undefined;
  orderBy: ColumnOrder[];
  withTies: boolean;
}

export interface HashJoinNode {
  joinType: JoinType;
  leftKey: number[];
  rightKey: number[];
  condition:
    | ExprNode
    | undefined;
  /** Used for internal table states. */
  leftTable:
    | Table
    | undefined;
  /** Used for internal table states. */
  rightTable:
    | Table
    | undefined;
  /** Used for internal table states. */
  leftDegreeTable:
    | Table
    | undefined;
  /** Used for internal table states. */
  rightDegreeTable:
    | Table
    | undefined;
  /** The output indices of current node */
  outputIndices: number[];
  /**
   * Left deduped input pk indices. The pk of the left_table and
   * left_degree_table is  [left_join_key | left_deduped_input_pk_indices]
   * and is expected to be the shortest key which starts with
   * the join key and satisfies unique constrain.
   */
  leftDedupedInputPkIndices: number[];
  /**
   * Right deduped input pk indices. The pk of the right_table and
   * right_degree_table is  [right_join_key | right_deduped_input_pk_indices]
   * and is expected to be the shortest key which starts with
   * the join key and satisfies unique constrain.
   */
  rightDedupedInputPkIndices: number[];
  nullSafe: boolean[];
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
}

export interface TemporalJoinNode {
  joinType: JoinType;
  leftKey: number[];
  rightKey: number[];
  nullSafe: boolean[];
  condition:
    | ExprNode
    | undefined;
  /** The output indices of current node */
  outputIndices: number[];
  /** The table desc of the lookup side table. */
  tableDesc:
    | StorageTableDesc
    | undefined;
  /** The output indices of the lookup side table */
  tableOutputIndices: number[];
}

export interface DynamicFilterNode {
  leftKey: number;
  /** Must be one of <, <=, >, >= */
  condition:
    | ExprNode
    | undefined;
  /** Left table stores all states with predicate possibly not NULL. */
  leftTable:
    | Table
    | undefined;
  /** Right table stores single value from RHS of predicate. */
  rightTable: Table | undefined;
}

/**
 * Delta join with two indexes. This is a pseudo plan node generated on frontend. On meta
 * service, it will be rewritten into lookup joins.
 */
export interface DeltaIndexJoinNode {
  joinType: JoinType;
  leftKey: number[];
  rightKey: number[];
  condition:
    | ExprNode
    | undefined;
  /** Table id of the left index. */
  leftTableId: number;
  /** Table id of the right index. */
  rightTableId: number;
  /** Info about the left index */
  leftInfo:
    | ArrangementInfo
    | undefined;
  /** Info about the right index */
  rightInfo:
    | ArrangementInfo
    | undefined;
  /** the output indices of current node */
  outputIndices: number[];
}

export interface HopWindowNode {
  timeCol: number;
  windowSlide: IntervalUnit | undefined;
  windowSize: IntervalUnit | undefined;
  outputIndices: number[];
  windowStartExprs: ExprNode[];
  windowEndExprs: ExprNode[];
}

export interface MergeNode {
  upstreamActorId: number[];
  upstreamFragmentId: number;
  /**
   * Type of the upstream dispatcher. If there's always one upstream according to this
   * type, the compute node may use the `ReceiverExecutor` as an optimization.
   */
  upstreamDispatcherType: DispatcherType;
  /** The schema of input columns. TODO: remove this field. */
  fields: Field[];
}

/**
 * passed from frontend to meta, used by fragmenter to generate `MergeNode`
 * and maybe `DispatcherNode` later.
 */
export interface ExchangeNode {
  strategy: DispatchStrategy | undefined;
}

/**
 * ChainNode is used for mv on mv.
 * ChainNode is like a "UNION" on mv snapshot and streaming. So it takes two inputs with fixed order:
 *   1. MergeNode (as a placeholder) for streaming read.
 *   2. BatchPlanNode for snapshot read.
 */
export interface ChainNode {
  tableId: number;
  /** The schema of input stream, which will be used to build a MergeNode */
  upstreamFields: Field[];
  /**
   * The columns from the upstream table to output.
   * TODO: rename this field.
   */
  upstreamColumnIndices: number[];
  /**
   * The columns from the upstream table that'll be internally required by this chain node.
   * TODO: This is currently only used by backfill table scan. We should also apply it to the upstream dispatcher (#4529).
   */
  upstreamColumnIds: number[];
  /**
   * Generally, the barrier needs to be rearranged during the MV creation process, so that data can
   * be flushed to shared buffer periodically, instead of making the first epoch from batch query extra
   * large. However, in some cases, e.g., shared state, the barrier cannot be rearranged in ChainNode.
   * ChainType is used to decide which implementation for the ChainNode.
   */
  chainType: ChainType;
  /**
   * Whether the upstream materialize is and this chain should be a singleton.
   * FIXME: This is a workaround for fragmenter since the distribution info will be lost if there's only one
   * fragment in the downstream mview. Remove this when we refactor the fragmenter.
   */
  isSingleton: boolean;
  /** The upstream materialized view info used by backfill. */
  tableDesc: StorageTableDesc | undefined;
}

/**
 * BatchPlanNode is used for mv on mv snapshot read.
 * BatchPlanNode is supposed to carry a batch plan that can be optimized with the streaming plan_common.
 * Currently, streaming to batch push down is not yet supported, BatchPlanNode is simply a table scan.
 */
export interface BatchPlanNode {
  tableDesc: StorageTableDesc | undefined;
  columnIds: number[];
}

export interface ArrangementInfo {
  /**
   * Order key of the arrangement, including order by columns and pk from the materialize
   * executor.
   */
  arrangeKeyOrders: ColumnOrder[];
  /** Column descs of the arrangement */
  columnDescs: ColumnDesc[];
  /** Used to build storage table by stream lookup join of delta join. */
  tableDesc: StorageTableDesc | undefined;
}

/**
 * Special node for shared state, which will only be produced in fragmenter. ArrangeNode will
 * produce a special Materialize executor, which materializes data for downstream to query.
 */
export interface ArrangeNode {
  /** Info about the arrangement */
  tableInfo:
    | ArrangementInfo
    | undefined;
  /** Hash key of the materialize node, which is a subset of pk. */
  distributionKey: number[];
  /** Used for internal table states. */
  table:
    | Table
    | undefined;
  /** Used to control whether doing sanity check, open it when upstream executor is source executor. */
  handlePkConflictBehavior: HandleConflictBehavior;
}

/** Special node for shared state. LookupNode will join an arrangement with a stream. */
export interface LookupNode {
  /** Join key of the arrangement side */
  arrangeKey: number[];
  /** Join key of the stream side */
  streamKey: number[];
  /** Whether to join the current epoch of arrangement */
  useCurrentEpoch: boolean;
  /**
   * Sometimes we need to re-order the output data to meet the requirement of schema.
   * By default, lookup executor will produce `<arrangement side, stream side>`. We
   * will then apply the column mapping to the combined result.
   */
  columnMapping: number[];
  arrangementTableId?:
    | { $case: "tableId"; tableId: number }
    | { $case: "indexId"; indexId: number };
  /** Info about the arrangement */
  arrangementTableInfo: ArrangementInfo | undefined;
}

/** WatermarkFilter needs to filter the upstream data by the water mark. */
export interface WatermarkFilterNode {
  /** The watermark descs */
  watermarkDescs: WatermarkDesc[];
  /** The tables used to persist watermarks, the key is vnode. */
  tables: Table[];
}

/** Acts like a merger, but on different inputs. */
export interface UnionNode {
}

/** Special node for shared state. Merge and align barrier from upstreams. Pipe inputs in order. */
export interface LookupUnionNode {
  order: number[];
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

/** Sorts inputs and outputs ordered data based on watermark. */
export interface SortNode {
  /** Persists data above watermark. */
  stateTable:
    | Table
    | undefined;
  /** Column index of watermark to perform sorting. */
  sortColumnIndex: number;
}

/** Merges two streams from streaming and batch for data manipulation. */
export interface DmlNode {
  /** Id of the table on which DML performs. */
  tableId: number;
  /** Version of the table. */
  tableVersionId: number;
  /** Column descriptions of the table. */
  columnDescs: ColumnDesc[];
}

export interface RowIdGenNode {
  rowIdIndex: number;
}

export interface NowNode {
  /** Persists emitted 'now'. */
  stateTable: Table | undefined;
}

export interface StreamNode {
  nodeBody?:
    | { $case: "source"; source: SourceNode }
    | { $case: "project"; project: ProjectNode }
    | { $case: "filter"; filter: FilterNode }
    | { $case: "materialize"; materialize: MaterializeNode }
    | { $case: "localSimpleAgg"; localSimpleAgg: SimpleAggNode }
    | { $case: "globalSimpleAgg"; globalSimpleAgg: SimpleAggNode }
    | { $case: "hashAgg"; hashAgg: HashAggNode }
    | { $case: "appendOnlyTopN"; appendOnlyTopN: TopNNode }
    | { $case: "hashJoin"; hashJoin: HashJoinNode }
    | { $case: "topN"; topN: TopNNode }
    | { $case: "hopWindow"; hopWindow: HopWindowNode }
    | { $case: "merge"; merge: MergeNode }
    | { $case: "exchange"; exchange: ExchangeNode }
    | { $case: "chain"; chain: ChainNode }
    | { $case: "batchPlan"; batchPlan: BatchPlanNode }
    | { $case: "lookup"; lookup: LookupNode }
    | { $case: "arrange"; arrange: ArrangeNode }
    | { $case: "lookupUnion"; lookupUnion: LookupUnionNode }
    | { $case: "union"; union: UnionNode }
    | { $case: "deltaIndexJoin"; deltaIndexJoin: DeltaIndexJoinNode }
    | { $case: "sink"; sink: SinkNode }
    | { $case: "expand"; expand: ExpandNode }
    | { $case: "dynamicFilter"; dynamicFilter: DynamicFilterNode }
    | { $case: "projectSet"; projectSet: ProjectSetNode }
    | { $case: "groupTopN"; groupTopN: GroupTopNNode }
    | { $case: "sort"; sort: SortNode }
    | { $case: "watermarkFilter"; watermarkFilter: WatermarkFilterNode }
    | { $case: "dml"; dml: DmlNode }
    | { $case: "rowIdGen"; rowIdGen: RowIdGenNode }
    | { $case: "now"; now: NowNode }
    | { $case: "appendOnlyGroupTopN"; appendOnlyGroupTopN: GroupTopNNode }
    | { $case: "temporalJoin"; temporalJoin: TemporalJoinNode };
  /**
   * The id for the operator. This is local per mview.
   * TODO: should better be a uint32.
   */
  operatorId: number;
  /** Child node in plan aka. upstream nodes in the streaming DAG */
  input: StreamNode[];
  streamKey: number[];
  appendOnly: boolean;
  identity: string;
  /** The schema of the plan node */
  fields: Field[];
}

/**
 * The property of an edge in the fragment graph.
 * This is essientially a "logical" version of `Dispatcher`. See the doc of `Dispatcher` for more details.
 */
export interface DispatchStrategy {
  type: DispatcherType;
  distKeyIndices: number[];
  outputIndices: number[];
}

/**
 * A dispatcher redistribute messages.
 * We encode both the type and other usage information in the proto.
 */
export interface Dispatcher {
  type: DispatcherType;
  /**
   * Indices of the columns to be used for hashing.
   * For dispatcher types other than HASH, this is ignored.
   */
  distKeyIndices: number[];
  /**
   * Indices of the columns to output.
   * In most cases, this contains all columns in the input. But for some cases like MV on MV or
   * schema change, we may only output a subset of the columns.
   */
  outputIndices: number[];
  /**
   * The hash mapping for consistent hash.
   * For dispatcher types other than HASH, this is ignored.
   */
  hashMapping:
    | ActorMapping
    | undefined;
  /**
   * Dispatcher can be uniquely identified by a combination of actor id and dispatcher id.
   * This is exactly the same as its downstream fragment id.
   */
  dispatcherId: number;
  /** Number of downstreams decides how many endpoints a dispatcher should dispatch. */
  downstreamActorId: number[];
}

/** A StreamActor is a running fragment of the overall stream graph, */
export interface StreamActor {
  actorId: number;
  fragmentId: number;
  nodes: StreamNode | undefined;
  dispatcher: Dispatcher[];
  /**
   * The actors that send messages to this actor.
   * Note that upstream actor ids are also stored in the proto of merge nodes.
   * It is painstaking to traverse through the node tree and get upstream actor id from the root StreamNode.
   * We duplicate the information here to ease the parsing logic in stream manager.
   */
  upstreamActorId: number[];
  /**
   * Vnodes that the executors in this actor own.
   * If the fragment is a singleton, this field will not be set and leave a `None`.
   */
  vnodeBitmap:
    | Buffer
    | undefined;
  /** The SQL definition of this materialized view. Used for debugging only. */
  mviewDefinition: string;
}

/** The environment associated with a stream plan */
export interface StreamEnvironment {
  /** The timezone associated with the streaming plan. Only applies to MV for now. */
  timezone: string;
}

export interface StreamFragmentGraph {
  /** all the fragments in the graph. */
  fragments: { [key: number]: StreamFragmentGraph_StreamFragment };
  /** edges between fragments. */
  edges: StreamFragmentGraph_StreamFragmentEdge[];
  dependentRelationIds: number[];
  tableIdsCnt: number;
  env:
    | StreamEnvironment
    | undefined;
  /** If none, default parallelism will be applied. */
  parallelism: StreamFragmentGraph_Parallelism | undefined;
}

export interface StreamFragmentGraph_StreamFragment {
  /** 0-based on frontend, and will be rewritten to global id on meta. */
  fragmentId: number;
  /** root stream node in this fragment. */
  node:
    | StreamNode
    | undefined;
  /** Bitwise-OR of FragmentTypeFlags */
  fragmentTypeMask: number;
  /** mark whether this fragment should only have one actor. */
  isSingleton: boolean;
  /** Number of table ids (stateful states) for this fragment. */
  tableIdsCnt: number;
  /** Mark the upstream table ids of this fragment, Used for fragments with `Chain`s. */
  upstreamTableIds: number[];
}

export interface StreamFragmentGraph_StreamFragmentEdge {
  /** Dispatch strategy for the fragment. */
  dispatchStrategy:
    | DispatchStrategy
    | undefined;
  /**
   * A unique identifier of this edge. Generally it should be exchange node's operator id. When
   * rewriting fragments into delta joins or when inserting 1-to-1 exchange, there will be
   * virtual links generated.
   */
  linkId: number;
  upstreamId: number;
  downstreamId: number;
}

export interface StreamFragmentGraph_Parallelism {
  parallelism: number;
}

export interface StreamFragmentGraph_FragmentsEntry {
  key: number;
  value: StreamFragmentGraph_StreamFragment | undefined;
}

function createBaseAddMutation(): AddMutation {
  return { actorDispatchers: {}, actorSplits: {} };
}

export const AddMutation = {
  fromJSON(object: any): AddMutation {
    return {
      actorDispatchers: isObject(object.actorDispatchers)
        ? Object.entries(object.actorDispatchers).reduce<{ [key: number]: AddMutation_Dispatchers }>(
          (acc, [key, value]) => {
            acc[Number(key)] = AddMutation_Dispatchers.fromJSON(value);
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
    };
  },

  toJSON(message: AddMutation): unknown {
    const obj: any = {};
    obj.actorDispatchers = {};
    if (message.actorDispatchers) {
      Object.entries(message.actorDispatchers).forEach(([k, v]) => {
        obj.actorDispatchers[k] = AddMutation_Dispatchers.toJSON(v);
      });
    }
    obj.actorSplits = {};
    if (message.actorSplits) {
      Object.entries(message.actorSplits).forEach(([k, v]) => {
        obj.actorSplits[k] = ConnectorSplits.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddMutation>, I>>(object: I): AddMutation {
    const message = createBaseAddMutation();
    message.actorDispatchers = Object.entries(object.actorDispatchers ?? {}).reduce<
      { [key: number]: AddMutation_Dispatchers }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = AddMutation_Dispatchers.fromPartial(value);
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
    return message;
  },
};

function createBaseAddMutation_Dispatchers(): AddMutation_Dispatchers {
  return { dispatchers: [] };
}

export const AddMutation_Dispatchers = {
  fromJSON(object: any): AddMutation_Dispatchers {
    return {
      dispatchers: Array.isArray(object?.dispatchers) ? object.dispatchers.map((e: any) => Dispatcher.fromJSON(e)) : [],
    };
  },

  toJSON(message: AddMutation_Dispatchers): unknown {
    const obj: any = {};
    if (message.dispatchers) {
      obj.dispatchers = message.dispatchers.map((e) => e ? Dispatcher.toJSON(e) : undefined);
    } else {
      obj.dispatchers = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddMutation_Dispatchers>, I>>(object: I): AddMutation_Dispatchers {
    const message = createBaseAddMutation_Dispatchers();
    message.dispatchers = object.dispatchers?.map((e) => Dispatcher.fromPartial(e)) || [];
    return message;
  },
};

function createBaseAddMutation_ActorDispatchersEntry(): AddMutation_ActorDispatchersEntry {
  return { key: 0, value: undefined };
}

export const AddMutation_ActorDispatchersEntry = {
  fromJSON(object: any): AddMutation_ActorDispatchersEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? AddMutation_Dispatchers.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: AddMutation_ActorDispatchersEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? AddMutation_Dispatchers.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddMutation_ActorDispatchersEntry>, I>>(
    object: I,
  ): AddMutation_ActorDispatchersEntry {
    const message = createBaseAddMutation_ActorDispatchersEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? AddMutation_Dispatchers.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseAddMutation_ActorSplitsEntry(): AddMutation_ActorSplitsEntry {
  return { key: 0, value: undefined };
}

export const AddMutation_ActorSplitsEntry = {
  fromJSON(object: any): AddMutation_ActorSplitsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ConnectorSplits.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: AddMutation_ActorSplitsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? ConnectorSplits.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AddMutation_ActorSplitsEntry>, I>>(object: I): AddMutation_ActorSplitsEntry {
    const message = createBaseAddMutation_ActorSplitsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ConnectorSplits.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseStopMutation(): StopMutation {
  return { actors: [] };
}

export const StopMutation = {
  fromJSON(object: any): StopMutation {
    return { actors: Array.isArray(object?.actors) ? object.actors.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: StopMutation): unknown {
    const obj: any = {};
    if (message.actors) {
      obj.actors = message.actors.map((e) => Math.round(e));
    } else {
      obj.actors = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StopMutation>, I>>(object: I): StopMutation {
    const message = createBaseStopMutation();
    message.actors = object.actors?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateMutation(): UpdateMutation {
  return { dispatcherUpdate: [], mergeUpdate: [], actorVnodeBitmapUpdate: {}, droppedActors: [], actorSplits: {} };
}

export const UpdateMutation = {
  fromJSON(object: any): UpdateMutation {
    return {
      dispatcherUpdate: Array.isArray(object?.dispatcherUpdate)
        ? object.dispatcherUpdate.map((e: any) => UpdateMutation_DispatcherUpdate.fromJSON(e))
        : [],
      mergeUpdate: Array.isArray(object?.mergeUpdate)
        ? object.mergeUpdate.map((e: any) => UpdateMutation_MergeUpdate.fromJSON(e))
        : [],
      actorVnodeBitmapUpdate: isObject(object.actorVnodeBitmapUpdate)
        ? Object.entries(object.actorVnodeBitmapUpdate).reduce<{ [key: number]: Buffer }>((acc, [key, value]) => {
          acc[Number(key)] = Buffer.fromJSON(value);
          return acc;
        }, {})
        : {},
      droppedActors: Array.isArray(object?.droppedActors)
        ? object.droppedActors.map((e: any) => Number(e))
        : [],
      actorSplits: isObject(object.actorSplits)
        ? Object.entries(object.actorSplits).reduce<{ [key: number]: ConnectorSplits }>((acc, [key, value]) => {
          acc[Number(key)] = ConnectorSplits.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: UpdateMutation): unknown {
    const obj: any = {};
    if (message.dispatcherUpdate) {
      obj.dispatcherUpdate = message.dispatcherUpdate.map((e) =>
        e ? UpdateMutation_DispatcherUpdate.toJSON(e) : undefined
      );
    } else {
      obj.dispatcherUpdate = [];
    }
    if (message.mergeUpdate) {
      obj.mergeUpdate = message.mergeUpdate.map((e) => e ? UpdateMutation_MergeUpdate.toJSON(e) : undefined);
    } else {
      obj.mergeUpdate = [];
    }
    obj.actorVnodeBitmapUpdate = {};
    if (message.actorVnodeBitmapUpdate) {
      Object.entries(message.actorVnodeBitmapUpdate).forEach(([k, v]) => {
        obj.actorVnodeBitmapUpdate[k] = Buffer.toJSON(v);
      });
    }
    if (message.droppedActors) {
      obj.droppedActors = message.droppedActors.map((e) => Math.round(e));
    } else {
      obj.droppedActors = [];
    }
    obj.actorSplits = {};
    if (message.actorSplits) {
      Object.entries(message.actorSplits).forEach(([k, v]) => {
        obj.actorSplits[k] = ConnectorSplits.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation>, I>>(object: I): UpdateMutation {
    const message = createBaseUpdateMutation();
    message.dispatcherUpdate = object.dispatcherUpdate?.map((e) => UpdateMutation_DispatcherUpdate.fromPartial(e)) ||
      [];
    message.mergeUpdate = object.mergeUpdate?.map((e) => UpdateMutation_MergeUpdate.fromPartial(e)) || [];
    message.actorVnodeBitmapUpdate = Object.entries(object.actorVnodeBitmapUpdate ?? {}).reduce<
      { [key: number]: Buffer }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = Buffer.fromPartial(value);
      }
      return acc;
    }, {});
    message.droppedActors = object.droppedActors?.map((e) => e) || [];
    message.actorSplits = Object.entries(object.actorSplits ?? {}).reduce<{ [key: number]: ConnectorSplits }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = ConnectorSplits.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseUpdateMutation_DispatcherUpdate(): UpdateMutation_DispatcherUpdate {
  return {
    actorId: 0,
    dispatcherId: 0,
    hashMapping: undefined,
    addedDownstreamActorId: [],
    removedDownstreamActorId: [],
  };
}

export const UpdateMutation_DispatcherUpdate = {
  fromJSON(object: any): UpdateMutation_DispatcherUpdate {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      dispatcherId: isSet(object.dispatcherId) ? Number(object.dispatcherId) : 0,
      hashMapping: isSet(object.hashMapping) ? ActorMapping.fromJSON(object.hashMapping) : undefined,
      addedDownstreamActorId: Array.isArray(object?.addedDownstreamActorId)
        ? object.addedDownstreamActorId.map((e: any) => Number(e))
        : [],
      removedDownstreamActorId: Array.isArray(object?.removedDownstreamActorId)
        ? object.removedDownstreamActorId.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: UpdateMutation_DispatcherUpdate): unknown {
    const obj: any = {};
    message.actorId !== undefined && (obj.actorId = Math.round(message.actorId));
    message.dispatcherId !== undefined && (obj.dispatcherId = Math.round(message.dispatcherId));
    message.hashMapping !== undefined &&
      (obj.hashMapping = message.hashMapping ? ActorMapping.toJSON(message.hashMapping) : undefined);
    if (message.addedDownstreamActorId) {
      obj.addedDownstreamActorId = message.addedDownstreamActorId.map((e) => Math.round(e));
    } else {
      obj.addedDownstreamActorId = [];
    }
    if (message.removedDownstreamActorId) {
      obj.removedDownstreamActorId = message.removedDownstreamActorId.map((e) => Math.round(e));
    } else {
      obj.removedDownstreamActorId = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_DispatcherUpdate>, I>>(
    object: I,
  ): UpdateMutation_DispatcherUpdate {
    const message = createBaseUpdateMutation_DispatcherUpdate();
    message.actorId = object.actorId ?? 0;
    message.dispatcherId = object.dispatcherId ?? 0;
    message.hashMapping = (object.hashMapping !== undefined && object.hashMapping !== null)
      ? ActorMapping.fromPartial(object.hashMapping)
      : undefined;
    message.addedDownstreamActorId = object.addedDownstreamActorId?.map((e) => e) || [];
    message.removedDownstreamActorId = object.removedDownstreamActorId?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateMutation_MergeUpdate(): UpdateMutation_MergeUpdate {
  return {
    actorId: 0,
    upstreamFragmentId: 0,
    newUpstreamFragmentId: undefined,
    addedUpstreamActorId: [],
    removedUpstreamActorId: [],
  };
}

export const UpdateMutation_MergeUpdate = {
  fromJSON(object: any): UpdateMutation_MergeUpdate {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      upstreamFragmentId: isSet(object.upstreamFragmentId) ? Number(object.upstreamFragmentId) : 0,
      newUpstreamFragmentId: isSet(object.newUpstreamFragmentId) ? Number(object.newUpstreamFragmentId) : undefined,
      addedUpstreamActorId: Array.isArray(object?.addedUpstreamActorId)
        ? object.addedUpstreamActorId.map((e: any) => Number(e))
        : [],
      removedUpstreamActorId: Array.isArray(object?.removedUpstreamActorId)
        ? object.removedUpstreamActorId.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: UpdateMutation_MergeUpdate): unknown {
    const obj: any = {};
    message.actorId !== undefined && (obj.actorId = Math.round(message.actorId));
    message.upstreamFragmentId !== undefined && (obj.upstreamFragmentId = Math.round(message.upstreamFragmentId));
    message.newUpstreamFragmentId !== undefined &&
      (obj.newUpstreamFragmentId = Math.round(message.newUpstreamFragmentId));
    if (message.addedUpstreamActorId) {
      obj.addedUpstreamActorId = message.addedUpstreamActorId.map((e) => Math.round(e));
    } else {
      obj.addedUpstreamActorId = [];
    }
    if (message.removedUpstreamActorId) {
      obj.removedUpstreamActorId = message.removedUpstreamActorId.map((e) => Math.round(e));
    } else {
      obj.removedUpstreamActorId = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_MergeUpdate>, I>>(object: I): UpdateMutation_MergeUpdate {
    const message = createBaseUpdateMutation_MergeUpdate();
    message.actorId = object.actorId ?? 0;
    message.upstreamFragmentId = object.upstreamFragmentId ?? 0;
    message.newUpstreamFragmentId = object.newUpstreamFragmentId ?? undefined;
    message.addedUpstreamActorId = object.addedUpstreamActorId?.map((e) => e) || [];
    message.removedUpstreamActorId = object.removedUpstreamActorId?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateMutation_ActorVnodeBitmapUpdateEntry(): UpdateMutation_ActorVnodeBitmapUpdateEntry {
  return { key: 0, value: undefined };
}

export const UpdateMutation_ActorVnodeBitmapUpdateEntry = {
  fromJSON(object: any): UpdateMutation_ActorVnodeBitmapUpdateEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? Buffer.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: UpdateMutation_ActorVnodeBitmapUpdateEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? Buffer.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_ActorVnodeBitmapUpdateEntry>, I>>(
    object: I,
  ): UpdateMutation_ActorVnodeBitmapUpdateEntry {
    const message = createBaseUpdateMutation_ActorVnodeBitmapUpdateEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? Buffer.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseUpdateMutation_ActorSplitsEntry(): UpdateMutation_ActorSplitsEntry {
  return { key: 0, value: undefined };
}

export const UpdateMutation_ActorSplitsEntry = {
  fromJSON(object: any): UpdateMutation_ActorSplitsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ConnectorSplits.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: UpdateMutation_ActorSplitsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? ConnectorSplits.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_ActorSplitsEntry>, I>>(
    object: I,
  ): UpdateMutation_ActorSplitsEntry {
    const message = createBaseUpdateMutation_ActorSplitsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ConnectorSplits.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseSourceChangeSplitMutation(): SourceChangeSplitMutation {
  return { actorSplits: {} };
}

export const SourceChangeSplitMutation = {
  fromJSON(object: any): SourceChangeSplitMutation {
    return {
      actorSplits: isObject(object.actorSplits)
        ? Object.entries(object.actorSplits).reduce<{ [key: number]: ConnectorSplits }>((acc, [key, value]) => {
          acc[Number(key)] = ConnectorSplits.fromJSON(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: SourceChangeSplitMutation): unknown {
    const obj: any = {};
    obj.actorSplits = {};
    if (message.actorSplits) {
      Object.entries(message.actorSplits).forEach(([k, v]) => {
        obj.actorSplits[k] = ConnectorSplits.toJSON(v);
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceChangeSplitMutation>, I>>(object: I): SourceChangeSplitMutation {
    const message = createBaseSourceChangeSplitMutation();
    message.actorSplits = Object.entries(object.actorSplits ?? {}).reduce<{ [key: number]: ConnectorSplits }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = ConnectorSplits.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseSourceChangeSplitMutation_ActorSplitsEntry(): SourceChangeSplitMutation_ActorSplitsEntry {
  return { key: 0, value: undefined };
}

export const SourceChangeSplitMutation_ActorSplitsEntry = {
  fromJSON(object: any): SourceChangeSplitMutation_ActorSplitsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? ConnectorSplits.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: SourceChangeSplitMutation_ActorSplitsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? ConnectorSplits.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceChangeSplitMutation_ActorSplitsEntry>, I>>(
    object: I,
  ): SourceChangeSplitMutation_ActorSplitsEntry {
    const message = createBaseSourceChangeSplitMutation_ActorSplitsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? ConnectorSplits.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBasePauseMutation(): PauseMutation {
  return {};
}

export const PauseMutation = {
  fromJSON(_: any): PauseMutation {
    return {};
  },

  toJSON(_: PauseMutation): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PauseMutation>, I>>(_: I): PauseMutation {
    const message = createBasePauseMutation();
    return message;
  },
};

function createBaseResumeMutation(): ResumeMutation {
  return {};
}

export const ResumeMutation = {
  fromJSON(_: any): ResumeMutation {
    return {};
  },

  toJSON(_: ResumeMutation): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ResumeMutation>, I>>(_: I): ResumeMutation {
    const message = createBaseResumeMutation();
    return message;
  },
};

function createBaseBarrier(): Barrier {
  return { epoch: undefined, mutation: undefined, span: new Uint8Array(), checkpoint: false, passedActors: [] };
}

export const Barrier = {
  fromJSON(object: any): Barrier {
    return {
      epoch: isSet(object.epoch) ? Epoch.fromJSON(object.epoch) : undefined,
      mutation: isSet(object.add)
        ? { $case: "add", add: AddMutation.fromJSON(object.add) }
        : isSet(object.stop)
        ? { $case: "stop", stop: StopMutation.fromJSON(object.stop) }
        : isSet(object.update)
        ? { $case: "update", update: UpdateMutation.fromJSON(object.update) }
        : isSet(object.splits)
        ? { $case: "splits", splits: SourceChangeSplitMutation.fromJSON(object.splits) }
        : isSet(object.pause)
        ? { $case: "pause", pause: PauseMutation.fromJSON(object.pause) }
        : isSet(object.resume)
        ? { $case: "resume", resume: ResumeMutation.fromJSON(object.resume) }
        : undefined,
      span: isSet(object.span) ? bytesFromBase64(object.span) : new Uint8Array(),
      checkpoint: isSet(object.checkpoint) ? Boolean(object.checkpoint) : false,
      passedActors: Array.isArray(object?.passedActors)
        ? object.passedActors.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: Barrier): unknown {
    const obj: any = {};
    message.epoch !== undefined && (obj.epoch = message.epoch ? Epoch.toJSON(message.epoch) : undefined);
    message.mutation?.$case === "add" &&
      (obj.add = message.mutation?.add ? AddMutation.toJSON(message.mutation?.add) : undefined);
    message.mutation?.$case === "stop" &&
      (obj.stop = message.mutation?.stop ? StopMutation.toJSON(message.mutation?.stop) : undefined);
    message.mutation?.$case === "update" &&
      (obj.update = message.mutation?.update ? UpdateMutation.toJSON(message.mutation?.update) : undefined);
    message.mutation?.$case === "splits" &&
      (obj.splits = message.mutation?.splits ? SourceChangeSplitMutation.toJSON(message.mutation?.splits) : undefined);
    message.mutation?.$case === "pause" &&
      (obj.pause = message.mutation?.pause ? PauseMutation.toJSON(message.mutation?.pause) : undefined);
    message.mutation?.$case === "resume" &&
      (obj.resume = message.mutation?.resume ? ResumeMutation.toJSON(message.mutation?.resume) : undefined);
    message.span !== undefined &&
      (obj.span = base64FromBytes(message.span !== undefined ? message.span : new Uint8Array()));
    message.checkpoint !== undefined && (obj.checkpoint = message.checkpoint);
    if (message.passedActors) {
      obj.passedActors = message.passedActors.map((e) => Math.round(e));
    } else {
      obj.passedActors = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Barrier>, I>>(object: I): Barrier {
    const message = createBaseBarrier();
    message.epoch = (object.epoch !== undefined && object.epoch !== null) ? Epoch.fromPartial(object.epoch) : undefined;
    if (object.mutation?.$case === "add" && object.mutation?.add !== undefined && object.mutation?.add !== null) {
      message.mutation = { $case: "add", add: AddMutation.fromPartial(object.mutation.add) };
    }
    if (object.mutation?.$case === "stop" && object.mutation?.stop !== undefined && object.mutation?.stop !== null) {
      message.mutation = { $case: "stop", stop: StopMutation.fromPartial(object.mutation.stop) };
    }
    if (
      object.mutation?.$case === "update" && object.mutation?.update !== undefined && object.mutation?.update !== null
    ) {
      message.mutation = { $case: "update", update: UpdateMutation.fromPartial(object.mutation.update) };
    }
    if (
      object.mutation?.$case === "splits" && object.mutation?.splits !== undefined && object.mutation?.splits !== null
    ) {
      message.mutation = { $case: "splits", splits: SourceChangeSplitMutation.fromPartial(object.mutation.splits) };
    }
    if (object.mutation?.$case === "pause" && object.mutation?.pause !== undefined && object.mutation?.pause !== null) {
      message.mutation = { $case: "pause", pause: PauseMutation.fromPartial(object.mutation.pause) };
    }
    if (
      object.mutation?.$case === "resume" && object.mutation?.resume !== undefined && object.mutation?.resume !== null
    ) {
      message.mutation = { $case: "resume", resume: ResumeMutation.fromPartial(object.mutation.resume) };
    }
    message.span = object.span ?? new Uint8Array();
    message.checkpoint = object.checkpoint ?? false;
    message.passedActors = object.passedActors?.map((e) => e) || [];
    return message;
  },
};

function createBaseWatermark(): Watermark {
  return { column: undefined, val: undefined };
}

export const Watermark = {
  fromJSON(object: any): Watermark {
    return {
      column: isSet(object.column) ? InputRef.fromJSON(object.column) : undefined,
      val: isSet(object.val) ? Datum.fromJSON(object.val) : undefined,
    };
  },

  toJSON(message: Watermark): unknown {
    const obj: any = {};
    message.column !== undefined && (obj.column = message.column ? InputRef.toJSON(message.column) : undefined);
    message.val !== undefined && (obj.val = message.val ? Datum.toJSON(message.val) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Watermark>, I>>(object: I): Watermark {
    const message = createBaseWatermark();
    message.column = (object.column !== undefined && object.column !== null)
      ? InputRef.fromPartial(object.column)
      : undefined;
    message.val = (object.val !== undefined && object.val !== null) ? Datum.fromPartial(object.val) : undefined;
    return message;
  },
};

function createBaseStreamMessage(): StreamMessage {
  return { streamMessage: undefined };
}

export const StreamMessage = {
  fromJSON(object: any): StreamMessage {
    return {
      streamMessage: isSet(object.streamChunk)
        ? { $case: "streamChunk", streamChunk: StreamChunk.fromJSON(object.streamChunk) }
        : isSet(object.barrier)
        ? { $case: "barrier", barrier: Barrier.fromJSON(object.barrier) }
        : isSet(object.watermark)
        ? { $case: "watermark", watermark: Watermark.fromJSON(object.watermark) }
        : undefined,
    };
  },

  toJSON(message: StreamMessage): unknown {
    const obj: any = {};
    message.streamMessage?.$case === "streamChunk" && (obj.streamChunk = message.streamMessage?.streamChunk
      ? StreamChunk.toJSON(message.streamMessage?.streamChunk)
      : undefined);
    message.streamMessage?.$case === "barrier" &&
      (obj.barrier = message.streamMessage?.barrier ? Barrier.toJSON(message.streamMessage?.barrier) : undefined);
    message.streamMessage?.$case === "watermark" &&
      (obj.watermark = message.streamMessage?.watermark
        ? Watermark.toJSON(message.streamMessage?.watermark)
        : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamMessage>, I>>(object: I): StreamMessage {
    const message = createBaseStreamMessage();
    if (
      object.streamMessage?.$case === "streamChunk" &&
      object.streamMessage?.streamChunk !== undefined &&
      object.streamMessage?.streamChunk !== null
    ) {
      message.streamMessage = {
        $case: "streamChunk",
        streamChunk: StreamChunk.fromPartial(object.streamMessage.streamChunk),
      };
    }
    if (
      object.streamMessage?.$case === "barrier" &&
      object.streamMessage?.barrier !== undefined &&
      object.streamMessage?.barrier !== null
    ) {
      message.streamMessage = { $case: "barrier", barrier: Barrier.fromPartial(object.streamMessage.barrier) };
    }
    if (
      object.streamMessage?.$case === "watermark" &&
      object.streamMessage?.watermark !== undefined &&
      object.streamMessage?.watermark !== null
    ) {
      message.streamMessage = { $case: "watermark", watermark: Watermark.fromPartial(object.streamMessage.watermark) };
    }
    return message;
  },
};

function createBaseActorMapping(): ActorMapping {
  return { originalIndices: [], data: [] };
}

export const ActorMapping = {
  fromJSON(object: any): ActorMapping {
    return {
      originalIndices: Array.isArray(object?.originalIndices) ? object.originalIndices.map((e: any) => Number(e)) : [],
      data: Array.isArray(object?.data) ? object.data.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: ActorMapping): unknown {
    const obj: any = {};
    if (message.originalIndices) {
      obj.originalIndices = message.originalIndices.map((e) => Math.round(e));
    } else {
      obj.originalIndices = [];
    }
    if (message.data) {
      obj.data = message.data.map((e) => Math.round(e));
    } else {
      obj.data = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ActorMapping>, I>>(object: I): ActorMapping {
    const message = createBaseActorMapping();
    message.originalIndices = object.originalIndices?.map((e) => e) || [];
    message.data = object.data?.map((e) => e) || [];
    return message;
  },
};

function createBaseStreamSource(): StreamSource {
  return {
    sourceId: 0,
    stateTable: undefined,
    rowIdIndex: undefined,
    columns: [],
    pkColumnIds: [],
    properties: {},
    info: undefined,
    sourceName: "",
  };
}

export const StreamSource = {
  fromJSON(object: any): StreamSource {
    return {
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      stateTable: isSet(object.stateTable) ? Table.fromJSON(object.stateTable) : undefined,
      rowIdIndex: isSet(object.rowIdIndex) ? Number(object.rowIdIndex) : undefined,
      columns: Array.isArray(object?.columns) ? object.columns.map((e: any) => ColumnCatalog.fromJSON(e)) : [],
      pkColumnIds: Array.isArray(object?.pkColumnIds) ? object.pkColumnIds.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      info: isSet(object.info) ? StreamSourceInfo.fromJSON(object.info) : undefined,
      sourceName: isSet(object.sourceName) ? String(object.sourceName) : "",
    };
  },

  toJSON(message: StreamSource): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    message.stateTable !== undefined &&
      (obj.stateTable = message.stateTable ? Table.toJSON(message.stateTable) : undefined);
    message.rowIdIndex !== undefined && (obj.rowIdIndex = Math.round(message.rowIdIndex));
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnCatalog.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pkColumnIds) {
      obj.pkColumnIds = message.pkColumnIds.map((e) => Math.round(e));
    } else {
      obj.pkColumnIds = [];
    }
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.info !== undefined && (obj.info = message.info ? StreamSourceInfo.toJSON(message.info) : undefined);
    message.sourceName !== undefined && (obj.sourceName = message.sourceName);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSource>, I>>(object: I): StreamSource {
    const message = createBaseStreamSource();
    message.sourceId = object.sourceId ?? 0;
    message.stateTable = (object.stateTable !== undefined && object.stateTable !== null)
      ? Table.fromPartial(object.stateTable)
      : undefined;
    message.rowIdIndex = object.rowIdIndex ?? undefined;
    message.columns = object.columns?.map((e) => ColumnCatalog.fromPartial(e)) || [];
    message.pkColumnIds = object.pkColumnIds?.map((e) => e) || [];
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.info = (object.info !== undefined && object.info !== null)
      ? StreamSourceInfo.fromPartial(object.info)
      : undefined;
    message.sourceName = object.sourceName ?? "";
    return message;
  },
};

function createBaseStreamSource_PropertiesEntry(): StreamSource_PropertiesEntry {
  return { key: "", value: "" };
}

export const StreamSource_PropertiesEntry = {
  fromJSON(object: any): StreamSource_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: StreamSource_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamSource_PropertiesEntry>, I>>(object: I): StreamSource_PropertiesEntry {
    const message = createBaseStreamSource_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSourceNode(): SourceNode {
  return { sourceInner: undefined };
}

export const SourceNode = {
  fromJSON(object: any): SourceNode {
    return { sourceInner: isSet(object.sourceInner) ? StreamSource.fromJSON(object.sourceInner) : undefined };
  },

  toJSON(message: SourceNode): unknown {
    const obj: any = {};
    message.sourceInner !== undefined &&
      (obj.sourceInner = message.sourceInner ? StreamSource.toJSON(message.sourceInner) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceNode>, I>>(object: I): SourceNode {
    const message = createBaseSourceNode();
    message.sourceInner = (object.sourceInner !== undefined && object.sourceInner !== null)
      ? StreamSource.fromPartial(object.sourceInner)
      : undefined;
    return message;
  },
};

function createBaseSinkDesc(): SinkDesc {
  return {
    id: 0,
    name: "",
    definition: "",
    columns: [],
    pk: [],
    streamKey: [],
    distributionKey: [],
    properties: {},
    sinkType: SinkType.UNSPECIFIED,
  };
}

export const SinkDesc = {
  fromJSON(object: any): SinkDesc {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      name: isSet(object.name) ? String(object.name) : "",
      definition: isSet(object.definition) ? String(object.definition) : "",
      columns: Array.isArray(object?.columns)
        ? object.columns.map((e: any) => ColumnDesc.fromJSON(e))
        : [],
      pk: Array.isArray(object?.pk) ? object.pk.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
      sinkType: isSet(object.sinkType) ? sinkTypeFromJSON(object.sinkType) : SinkType.UNSPECIFIED,
    };
  },

  toJSON(message: SinkDesc): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.name !== undefined && (obj.name = message.name);
    message.definition !== undefined && (obj.definition = message.definition);
    if (message.columns) {
      obj.columns = message.columns.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.columns = [];
    }
    if (message.pk) {
      obj.pk = message.pk.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.pk = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
    } else {
      obj.streamKey = [];
    }
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    message.sinkType !== undefined && (obj.sinkType = sinkTypeToJSON(message.sinkType));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkDesc>, I>>(object: I): SinkDesc {
    const message = createBaseSinkDesc();
    message.id = object.id ?? 0;
    message.name = object.name ?? "";
    message.definition = object.definition ?? "";
    message.columns = object.columns?.map((e) => ColumnDesc.fromPartial(e)) || [];
    message.pk = object.pk?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    message.sinkType = object.sinkType ?? SinkType.UNSPECIFIED;
    return message;
  },
};

function createBaseSinkDesc_PropertiesEntry(): SinkDesc_PropertiesEntry {
  return { key: "", value: "" };
}

export const SinkDesc_PropertiesEntry = {
  fromJSON(object: any): SinkDesc_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: SinkDesc_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkDesc_PropertiesEntry>, I>>(object: I): SinkDesc_PropertiesEntry {
    const message = createBaseSinkDesc_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseSinkNode(): SinkNode {
  return { sinkDesc: undefined };
}

export const SinkNode = {
  fromJSON(object: any): SinkNode {
    return { sinkDesc: isSet(object.sinkDesc) ? SinkDesc.fromJSON(object.sinkDesc) : undefined };
  },

  toJSON(message: SinkNode): unknown {
    const obj: any = {};
    message.sinkDesc !== undefined && (obj.sinkDesc = message.sinkDesc ? SinkDesc.toJSON(message.sinkDesc) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkNode>, I>>(object: I): SinkNode {
    const message = createBaseSinkNode();
    message.sinkDesc = (object.sinkDesc !== undefined && object.sinkDesc !== null)
      ? SinkDesc.fromPartial(object.sinkDesc)
      : undefined;
    return message;
  },
};

function createBaseProjectNode(): ProjectNode {
  return { selectList: [], watermarkInputKey: [], watermarkOutputKey: [] };
}

export const ProjectNode = {
  fromJSON(object: any): ProjectNode {
    return {
      selectList: Array.isArray(object?.selectList) ? object.selectList.map((e: any) => ExprNode.fromJSON(e)) : [],
      watermarkInputKey: Array.isArray(object?.watermarkInputKey)
        ? object.watermarkInputKey.map((e: any) => Number(e))
        : [],
      watermarkOutputKey: Array.isArray(object?.watermarkOutputKey)
        ? object.watermarkOutputKey.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: ProjectNode): unknown {
    const obj: any = {};
    if (message.selectList) {
      obj.selectList = message.selectList.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.selectList = [];
    }
    if (message.watermarkInputKey) {
      obj.watermarkInputKey = message.watermarkInputKey.map((e) => Math.round(e));
    } else {
      obj.watermarkInputKey = [];
    }
    if (message.watermarkOutputKey) {
      obj.watermarkOutputKey = message.watermarkOutputKey.map((e) => Math.round(e));
    } else {
      obj.watermarkOutputKey = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProjectNode>, I>>(object: I): ProjectNode {
    const message = createBaseProjectNode();
    message.selectList = object.selectList?.map((e) => ExprNode.fromPartial(e)) || [];
    message.watermarkInputKey = object.watermarkInputKey?.map((e) => e) || [];
    message.watermarkOutputKey = object.watermarkOutputKey?.map((e) => e) || [];
    return message;
  },
};

function createBaseFilterNode(): FilterNode {
  return { searchCondition: undefined };
}

export const FilterNode = {
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

function createBaseMaterializeNode(): MaterializeNode {
  return {
    tableId: 0,
    columnOrders: [],
    table: undefined,
    handlePkConflictBehavior: HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
  };
}

export const MaterializeNode = {
  fromJSON(object: any): MaterializeNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      columnOrders: Array.isArray(object?.columnOrders)
        ? object.columnOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      handlePkConflictBehavior: isSet(object.handlePkConflictBehavior)
        ? handleConflictBehaviorFromJSON(object.handlePkConflictBehavior)
        : HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
    };
  },

  toJSON(message: MaterializeNode): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    if (message.columnOrders) {
      obj.columnOrders = message.columnOrders.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.columnOrders = [];
    }
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    message.handlePkConflictBehavior !== undefined &&
      (obj.handlePkConflictBehavior = handleConflictBehaviorToJSON(message.handlePkConflictBehavior));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MaterializeNode>, I>>(object: I): MaterializeNode {
    const message = createBaseMaterializeNode();
    message.tableId = object.tableId ?? 0;
    message.columnOrders = object.columnOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.handlePkConflictBehavior = object.handlePkConflictBehavior ?? HandleConflictBehavior.NO_CHECK_UNSPECIFIED;
    return message;
  },
};

function createBaseAggCallState(): AggCallState {
  return { inner: undefined };
}

export const AggCallState = {
  fromJSON(object: any): AggCallState {
    return {
      inner: isSet(object.resultValueState)
        ? {
          $case: "resultValueState",
          resultValueState: AggCallState_ResultValueState.fromJSON(object.resultValueState),
        }
        : isSet(object.tableState)
        ? { $case: "tableState", tableState: AggCallState_TableState.fromJSON(object.tableState) }
        : isSet(object.materializedInputState)
        ? {
          $case: "materializedInputState",
          materializedInputState: AggCallState_MaterializedInputState.fromJSON(object.materializedInputState),
        }
        : undefined,
    };
  },

  toJSON(message: AggCallState): unknown {
    const obj: any = {};
    message.inner?.$case === "resultValueState" && (obj.resultValueState = message.inner?.resultValueState
      ? AggCallState_ResultValueState.toJSON(message.inner?.resultValueState)
      : undefined);
    message.inner?.$case === "tableState" && (obj.tableState = message.inner?.tableState
      ? AggCallState_TableState.toJSON(message.inner?.tableState)
      : undefined);
    message.inner?.$case === "materializedInputState" &&
      (obj.materializedInputState = message.inner?.materializedInputState
        ? AggCallState_MaterializedInputState.toJSON(message.inner?.materializedInputState)
        : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCallState>, I>>(object: I): AggCallState {
    const message = createBaseAggCallState();
    if (
      object.inner?.$case === "resultValueState" &&
      object.inner?.resultValueState !== undefined &&
      object.inner?.resultValueState !== null
    ) {
      message.inner = {
        $case: "resultValueState",
        resultValueState: AggCallState_ResultValueState.fromPartial(object.inner.resultValueState),
      };
    }
    if (
      object.inner?.$case === "tableState" &&
      object.inner?.tableState !== undefined &&
      object.inner?.tableState !== null
    ) {
      message.inner = { $case: "tableState", tableState: AggCallState_TableState.fromPartial(object.inner.tableState) };
    }
    if (
      object.inner?.$case === "materializedInputState" &&
      object.inner?.materializedInputState !== undefined &&
      object.inner?.materializedInputState !== null
    ) {
      message.inner = {
        $case: "materializedInputState",
        materializedInputState: AggCallState_MaterializedInputState.fromPartial(object.inner.materializedInputState),
      };
    }
    return message;
  },
};

function createBaseAggCallState_ResultValueState(): AggCallState_ResultValueState {
  return {};
}

export const AggCallState_ResultValueState = {
  fromJSON(_: any): AggCallState_ResultValueState {
    return {};
  },

  toJSON(_: AggCallState_ResultValueState): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCallState_ResultValueState>, I>>(_: I): AggCallState_ResultValueState {
    const message = createBaseAggCallState_ResultValueState();
    return message;
  },
};

function createBaseAggCallState_TableState(): AggCallState_TableState {
  return { table: undefined };
}

export const AggCallState_TableState = {
  fromJSON(object: any): AggCallState_TableState {
    return { table: isSet(object.table) ? Table.fromJSON(object.table) : undefined };
  },

  toJSON(message: AggCallState_TableState): unknown {
    const obj: any = {};
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCallState_TableState>, I>>(object: I): AggCallState_TableState {
    const message = createBaseAggCallState_TableState();
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    return message;
  },
};

function createBaseAggCallState_MaterializedInputState(): AggCallState_MaterializedInputState {
  return { table: undefined, includedUpstreamIndices: [], tableValueIndices: [] };
}

export const AggCallState_MaterializedInputState = {
  fromJSON(object: any): AggCallState_MaterializedInputState {
    return {
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      includedUpstreamIndices: Array.isArray(object?.includedUpstreamIndices)
        ? object.includedUpstreamIndices.map((e: any) => Number(e))
        : [],
      tableValueIndices: Array.isArray(object?.tableValueIndices)
        ? object.tableValueIndices.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: AggCallState_MaterializedInputState): unknown {
    const obj: any = {};
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    if (message.includedUpstreamIndices) {
      obj.includedUpstreamIndices = message.includedUpstreamIndices.map((e) => Math.round(e));
    } else {
      obj.includedUpstreamIndices = [];
    }
    if (message.tableValueIndices) {
      obj.tableValueIndices = message.tableValueIndices.map((e) => Math.round(e));
    } else {
      obj.tableValueIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCallState_MaterializedInputState>, I>>(
    object: I,
  ): AggCallState_MaterializedInputState {
    const message = createBaseAggCallState_MaterializedInputState();
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.includedUpstreamIndices = object.includedUpstreamIndices?.map((e) => e) || [];
    message.tableValueIndices = object.tableValueIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseSimpleAggNode(): SimpleAggNode {
  return {
    aggCalls: [],
    distributionKey: [],
    aggCallStates: [],
    resultTable: undefined,
    isAppendOnly: false,
    distinctDedupTables: {},
    rowCountIndex: 0,
  };
}

export const SimpleAggNode = {
  fromJSON(object: any): SimpleAggNode {
    return {
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      aggCallStates: Array.isArray(object?.aggCallStates)
        ? object.aggCallStates.map((e: any) => AggCallState.fromJSON(e))
        : [],
      resultTable: isSet(object.resultTable) ? Table.fromJSON(object.resultTable) : undefined,
      isAppendOnly: isSet(object.isAppendOnly) ? Boolean(object.isAppendOnly) : false,
      distinctDedupTables: isObject(object.distinctDedupTables)
        ? Object.entries(object.distinctDedupTables).reduce<{ [key: number]: Table }>((acc, [key, value]) => {
          acc[Number(key)] = Table.fromJSON(value);
          return acc;
        }, {})
        : {},
      rowCountIndex: isSet(object.rowCountIndex) ? Number(object.rowCountIndex) : 0,
    };
  },

  toJSON(message: SimpleAggNode): unknown {
    const obj: any = {};
    if (message.aggCalls) {
      obj.aggCalls = message.aggCalls.map((e) => e ? AggCall.toJSON(e) : undefined);
    } else {
      obj.aggCalls = [];
    }
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    if (message.aggCallStates) {
      obj.aggCallStates = message.aggCallStates.map((e) => e ? AggCallState.toJSON(e) : undefined);
    } else {
      obj.aggCallStates = [];
    }
    message.resultTable !== undefined &&
      (obj.resultTable = message.resultTable ? Table.toJSON(message.resultTable) : undefined);
    message.isAppendOnly !== undefined && (obj.isAppendOnly = message.isAppendOnly);
    obj.distinctDedupTables = {};
    if (message.distinctDedupTables) {
      Object.entries(message.distinctDedupTables).forEach(([k, v]) => {
        obj.distinctDedupTables[k] = Table.toJSON(v);
      });
    }
    message.rowCountIndex !== undefined && (obj.rowCountIndex = Math.round(message.rowCountIndex));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SimpleAggNode>, I>>(object: I): SimpleAggNode {
    const message = createBaseSimpleAggNode();
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.aggCallStates = object.aggCallStates?.map((e) => AggCallState.fromPartial(e)) || [];
    message.resultTable = (object.resultTable !== undefined && object.resultTable !== null)
      ? Table.fromPartial(object.resultTable)
      : undefined;
    message.isAppendOnly = object.isAppendOnly ?? false;
    message.distinctDedupTables = Object.entries(object.distinctDedupTables ?? {}).reduce<{ [key: number]: Table }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = Table.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.rowCountIndex = object.rowCountIndex ?? 0;
    return message;
  },
};

function createBaseSimpleAggNode_DistinctDedupTablesEntry(): SimpleAggNode_DistinctDedupTablesEntry {
  return { key: 0, value: undefined };
}

export const SimpleAggNode_DistinctDedupTablesEntry = {
  fromJSON(object: any): SimpleAggNode_DistinctDedupTablesEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? Table.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: SimpleAggNode_DistinctDedupTablesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? Table.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SimpleAggNode_DistinctDedupTablesEntry>, I>>(
    object: I,
  ): SimpleAggNode_DistinctDedupTablesEntry {
    const message = createBaseSimpleAggNode_DistinctDedupTablesEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null) ? Table.fromPartial(object.value) : undefined;
    return message;
  },
};

function createBaseHashAggNode(): HashAggNode {
  return {
    groupKey: [],
    aggCalls: [],
    aggCallStates: [],
    resultTable: undefined,
    isAppendOnly: false,
    distinctDedupTables: {},
    rowCountIndex: 0,
  };
}

export const HashAggNode = {
  fromJSON(object: any): HashAggNode {
    return {
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => Number(e)) : [],
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
      aggCallStates: Array.isArray(object?.aggCallStates)
        ? object.aggCallStates.map((e: any) => AggCallState.fromJSON(e))
        : [],
      resultTable: isSet(object.resultTable) ? Table.fromJSON(object.resultTable) : undefined,
      isAppendOnly: isSet(object.isAppendOnly) ? Boolean(object.isAppendOnly) : false,
      distinctDedupTables: isObject(object.distinctDedupTables)
        ? Object.entries(object.distinctDedupTables).reduce<{ [key: number]: Table }>((acc, [key, value]) => {
          acc[Number(key)] = Table.fromJSON(value);
          return acc;
        }, {})
        : {},
      rowCountIndex: isSet(object.rowCountIndex) ? Number(object.rowCountIndex) : 0,
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
    if (message.aggCallStates) {
      obj.aggCallStates = message.aggCallStates.map((e) => e ? AggCallState.toJSON(e) : undefined);
    } else {
      obj.aggCallStates = [];
    }
    message.resultTable !== undefined &&
      (obj.resultTable = message.resultTable ? Table.toJSON(message.resultTable) : undefined);
    message.isAppendOnly !== undefined && (obj.isAppendOnly = message.isAppendOnly);
    obj.distinctDedupTables = {};
    if (message.distinctDedupTables) {
      Object.entries(message.distinctDedupTables).forEach(([k, v]) => {
        obj.distinctDedupTables[k] = Table.toJSON(v);
      });
    }
    message.rowCountIndex !== undefined && (obj.rowCountIndex = Math.round(message.rowCountIndex));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashAggNode>, I>>(object: I): HashAggNode {
    const message = createBaseHashAggNode();
    message.groupKey = object.groupKey?.map((e) => e) || [];
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    message.aggCallStates = object.aggCallStates?.map((e) => AggCallState.fromPartial(e)) || [];
    message.resultTable = (object.resultTable !== undefined && object.resultTable !== null)
      ? Table.fromPartial(object.resultTable)
      : undefined;
    message.isAppendOnly = object.isAppendOnly ?? false;
    message.distinctDedupTables = Object.entries(object.distinctDedupTables ?? {}).reduce<{ [key: number]: Table }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[Number(key)] = Table.fromPartial(value);
        }
        return acc;
      },
      {},
    );
    message.rowCountIndex = object.rowCountIndex ?? 0;
    return message;
  },
};

function createBaseHashAggNode_DistinctDedupTablesEntry(): HashAggNode_DistinctDedupTablesEntry {
  return { key: 0, value: undefined };
}

export const HashAggNode_DistinctDedupTablesEntry = {
  fromJSON(object: any): HashAggNode_DistinctDedupTablesEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? Table.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: HashAggNode_DistinctDedupTablesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined && (obj.value = message.value ? Table.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashAggNode_DistinctDedupTablesEntry>, I>>(
    object: I,
  ): HashAggNode_DistinctDedupTablesEntry {
    const message = createBaseHashAggNode_DistinctDedupTablesEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null) ? Table.fromPartial(object.value) : undefined;
    return message;
  },
};

function createBaseTopNNode(): TopNNode {
  return { limit: 0, offset: 0, table: undefined, orderBy: [], withTies: false };
}

export const TopNNode = {
  fromJSON(object: any): TopNNode {
    return {
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      orderBy: Array.isArray(object?.orderBy) ? object.orderBy.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      withTies: isSet(object.withTies) ? Boolean(object.withTies) : false,
    };
  },

  toJSON(message: TopNNode): unknown {
    const obj: any = {};
    message.limit !== undefined && (obj.limit = Math.round(message.limit));
    message.offset !== undefined && (obj.offset = Math.round(message.offset));
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    if (message.orderBy) {
      obj.orderBy = message.orderBy.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.orderBy = [];
    }
    message.withTies !== undefined && (obj.withTies = message.withTies);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TopNNode>, I>>(object: I): TopNNode {
    const message = createBaseTopNNode();
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.orderBy = object.orderBy?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.withTies = object.withTies ?? false;
    return message;
  },
};

function createBaseGroupTopNNode(): GroupTopNNode {
  return { limit: 0, offset: 0, groupKey: [], table: undefined, orderBy: [], withTies: false };
}

export const GroupTopNNode = {
  fromJSON(object: any): GroupTopNNode {
    return {
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => Number(e)) : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      orderBy: Array.isArray(object?.orderBy) ? object.orderBy.map((e: any) => ColumnOrder.fromJSON(e)) : [],
      withTies: isSet(object.withTies) ? Boolean(object.withTies) : false,
    };
  },

  toJSON(message: GroupTopNNode): unknown {
    const obj: any = {};
    message.limit !== undefined && (obj.limit = Math.round(message.limit));
    message.offset !== undefined && (obj.offset = Math.round(message.offset));
    if (message.groupKey) {
      obj.groupKey = message.groupKey.map((e) => Math.round(e));
    } else {
      obj.groupKey = [];
    }
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    if (message.orderBy) {
      obj.orderBy = message.orderBy.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.orderBy = [];
    }
    message.withTies !== undefined && (obj.withTies = message.withTies);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupTopNNode>, I>>(object: I): GroupTopNNode {
    const message = createBaseGroupTopNNode();
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    message.groupKey = object.groupKey?.map((e) => e) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.orderBy = object.orderBy?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.withTies = object.withTies ?? false;
    return message;
  },
};

function createBaseHashJoinNode(): HashJoinNode {
  return {
    joinType: JoinType.UNSPECIFIED,
    leftKey: [],
    rightKey: [],
    condition: undefined,
    leftTable: undefined,
    rightTable: undefined,
    leftDegreeTable: undefined,
    rightDegreeTable: undefined,
    outputIndices: [],
    leftDedupedInputPkIndices: [],
    rightDedupedInputPkIndices: [],
    nullSafe: [],
    isAppendOnly: false,
  };
}

export const HashJoinNode = {
  fromJSON(object: any): HashJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : JoinType.UNSPECIFIED,
      leftKey: Array.isArray(object?.leftKey) ? object.leftKey.map((e: any) => Number(e)) : [],
      rightKey: Array.isArray(object?.rightKey) ? object.rightKey.map((e: any) => Number(e)) : [],
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      leftTable: isSet(object.leftTable) ? Table.fromJSON(object.leftTable) : undefined,
      rightTable: isSet(object.rightTable) ? Table.fromJSON(object.rightTable) : undefined,
      leftDegreeTable: isSet(object.leftDegreeTable) ? Table.fromJSON(object.leftDegreeTable) : undefined,
      rightDegreeTable: isSet(object.rightDegreeTable) ? Table.fromJSON(object.rightDegreeTable) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      leftDedupedInputPkIndices: Array.isArray(object?.leftDedupedInputPkIndices)
        ? object.leftDedupedInputPkIndices.map((e: any) => Number(e))
        : [],
      rightDedupedInputPkIndices: Array.isArray(object?.rightDedupedInputPkIndices)
        ? object.rightDedupedInputPkIndices.map((e: any) => Number(e))
        : [],
      nullSafe: Array.isArray(object?.nullSafe) ? object.nullSafe.map((e: any) => Boolean(e)) : [],
      isAppendOnly: isSet(object.isAppendOnly) ? Boolean(object.isAppendOnly) : false,
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
    message.leftTable !== undefined &&
      (obj.leftTable = message.leftTable ? Table.toJSON(message.leftTable) : undefined);
    message.rightTable !== undefined &&
      (obj.rightTable = message.rightTable ? Table.toJSON(message.rightTable) : undefined);
    message.leftDegreeTable !== undefined &&
      (obj.leftDegreeTable = message.leftDegreeTable ? Table.toJSON(message.leftDegreeTable) : undefined);
    message.rightDegreeTable !== undefined &&
      (obj.rightDegreeTable = message.rightDegreeTable ? Table.toJSON(message.rightDegreeTable) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    if (message.leftDedupedInputPkIndices) {
      obj.leftDedupedInputPkIndices = message.leftDedupedInputPkIndices.map((e) => Math.round(e));
    } else {
      obj.leftDedupedInputPkIndices = [];
    }
    if (message.rightDedupedInputPkIndices) {
      obj.rightDedupedInputPkIndices = message.rightDedupedInputPkIndices.map((e) => Math.round(e));
    } else {
      obj.rightDedupedInputPkIndices = [];
    }
    if (message.nullSafe) {
      obj.nullSafe = message.nullSafe.map((e) => e);
    } else {
      obj.nullSafe = [];
    }
    message.isAppendOnly !== undefined && (obj.isAppendOnly = message.isAppendOnly);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashJoinNode>, I>>(object: I): HashJoinNode {
    const message = createBaseHashJoinNode();
    message.joinType = object.joinType ?? JoinType.UNSPECIFIED;
    message.leftKey = object.leftKey?.map((e) => e) || [];
    message.rightKey = object.rightKey?.map((e) => e) || [];
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.leftTable = (object.leftTable !== undefined && object.leftTable !== null)
      ? Table.fromPartial(object.leftTable)
      : undefined;
    message.rightTable = (object.rightTable !== undefined && object.rightTable !== null)
      ? Table.fromPartial(object.rightTable)
      : undefined;
    message.leftDegreeTable = (object.leftDegreeTable !== undefined && object.leftDegreeTable !== null)
      ? Table.fromPartial(object.leftDegreeTable)
      : undefined;
    message.rightDegreeTable = (object.rightDegreeTable !== undefined && object.rightDegreeTable !== null)
      ? Table.fromPartial(object.rightDegreeTable)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.leftDedupedInputPkIndices = object.leftDedupedInputPkIndices?.map((e) => e) || [];
    message.rightDedupedInputPkIndices = object.rightDedupedInputPkIndices?.map((e) => e) || [];
    message.nullSafe = object.nullSafe?.map((e) => e) || [];
    message.isAppendOnly = object.isAppendOnly ?? false;
    return message;
  },
};

function createBaseTemporalJoinNode(): TemporalJoinNode {
  return {
    joinType: JoinType.UNSPECIFIED,
    leftKey: [],
    rightKey: [],
    nullSafe: [],
    condition: undefined,
    outputIndices: [],
    tableDesc: undefined,
    tableOutputIndices: [],
  };
}

export const TemporalJoinNode = {
  fromJSON(object: any): TemporalJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : JoinType.UNSPECIFIED,
      leftKey: Array.isArray(object?.leftKey) ? object.leftKey.map((e: any) => Number(e)) : [],
      rightKey: Array.isArray(object?.rightKey) ? object.rightKey.map((e: any) => Number(e)) : [],
      nullSafe: Array.isArray(object?.nullSafe) ? object.nullSafe.map((e: any) => Boolean(e)) : [],
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      tableDesc: isSet(object.tableDesc) ? StorageTableDesc.fromJSON(object.tableDesc) : undefined,
      tableOutputIndices: Array.isArray(object?.tableOutputIndices)
        ? object.tableOutputIndices.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: TemporalJoinNode): unknown {
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
    if (message.nullSafe) {
      obj.nullSafe = message.nullSafe.map((e) => e);
    } else {
      obj.nullSafe = [];
    }
    message.condition !== undefined &&
      (obj.condition = message.condition ? ExprNode.toJSON(message.condition) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    message.tableDesc !== undefined &&
      (obj.tableDesc = message.tableDesc ? StorageTableDesc.toJSON(message.tableDesc) : undefined);
    if (message.tableOutputIndices) {
      obj.tableOutputIndices = message.tableOutputIndices.map((e) => Math.round(e));
    } else {
      obj.tableOutputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TemporalJoinNode>, I>>(object: I): TemporalJoinNode {
    const message = createBaseTemporalJoinNode();
    message.joinType = object.joinType ?? JoinType.UNSPECIFIED;
    message.leftKey = object.leftKey?.map((e) => e) || [];
    message.rightKey = object.rightKey?.map((e) => e) || [];
    message.nullSafe = object.nullSafe?.map((e) => e) || [];
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.tableDesc = (object.tableDesc !== undefined && object.tableDesc !== null)
      ? StorageTableDesc.fromPartial(object.tableDesc)
      : undefined;
    message.tableOutputIndices = object.tableOutputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseDynamicFilterNode(): DynamicFilterNode {
  return { leftKey: 0, condition: undefined, leftTable: undefined, rightTable: undefined };
}

export const DynamicFilterNode = {
  fromJSON(object: any): DynamicFilterNode {
    return {
      leftKey: isSet(object.leftKey) ? Number(object.leftKey) : 0,
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      leftTable: isSet(object.leftTable) ? Table.fromJSON(object.leftTable) : undefined,
      rightTable: isSet(object.rightTable) ? Table.fromJSON(object.rightTable) : undefined,
    };
  },

  toJSON(message: DynamicFilterNode): unknown {
    const obj: any = {};
    message.leftKey !== undefined && (obj.leftKey = Math.round(message.leftKey));
    message.condition !== undefined &&
      (obj.condition = message.condition ? ExprNode.toJSON(message.condition) : undefined);
    message.leftTable !== undefined &&
      (obj.leftTable = message.leftTable ? Table.toJSON(message.leftTable) : undefined);
    message.rightTable !== undefined &&
      (obj.rightTable = message.rightTable ? Table.toJSON(message.rightTable) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DynamicFilterNode>, I>>(object: I): DynamicFilterNode {
    const message = createBaseDynamicFilterNode();
    message.leftKey = object.leftKey ?? 0;
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.leftTable = (object.leftTable !== undefined && object.leftTable !== null)
      ? Table.fromPartial(object.leftTable)
      : undefined;
    message.rightTable = (object.rightTable !== undefined && object.rightTable !== null)
      ? Table.fromPartial(object.rightTable)
      : undefined;
    return message;
  },
};

function createBaseDeltaIndexJoinNode(): DeltaIndexJoinNode {
  return {
    joinType: JoinType.UNSPECIFIED,
    leftKey: [],
    rightKey: [],
    condition: undefined,
    leftTableId: 0,
    rightTableId: 0,
    leftInfo: undefined,
    rightInfo: undefined,
    outputIndices: [],
  };
}

export const DeltaIndexJoinNode = {
  fromJSON(object: any): DeltaIndexJoinNode {
    return {
      joinType: isSet(object.joinType) ? joinTypeFromJSON(object.joinType) : JoinType.UNSPECIFIED,
      leftKey: Array.isArray(object?.leftKey) ? object.leftKey.map((e: any) => Number(e)) : [],
      rightKey: Array.isArray(object?.rightKey) ? object.rightKey.map((e: any) => Number(e)) : [],
      condition: isSet(object.condition) ? ExprNode.fromJSON(object.condition) : undefined,
      leftTableId: isSet(object.leftTableId) ? Number(object.leftTableId) : 0,
      rightTableId: isSet(object.rightTableId) ? Number(object.rightTableId) : 0,
      leftInfo: isSet(object.leftInfo) ? ArrangementInfo.fromJSON(object.leftInfo) : undefined,
      rightInfo: isSet(object.rightInfo) ? ArrangementInfo.fromJSON(object.rightInfo) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: DeltaIndexJoinNode): unknown {
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
    message.leftTableId !== undefined && (obj.leftTableId = Math.round(message.leftTableId));
    message.rightTableId !== undefined && (obj.rightTableId = Math.round(message.rightTableId));
    message.leftInfo !== undefined &&
      (obj.leftInfo = message.leftInfo ? ArrangementInfo.toJSON(message.leftInfo) : undefined);
    message.rightInfo !== undefined &&
      (obj.rightInfo = message.rightInfo ? ArrangementInfo.toJSON(message.rightInfo) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeltaIndexJoinNode>, I>>(object: I): DeltaIndexJoinNode {
    const message = createBaseDeltaIndexJoinNode();
    message.joinType = object.joinType ?? JoinType.UNSPECIFIED;
    message.leftKey = object.leftKey?.map((e) => e) || [];
    message.rightKey = object.rightKey?.map((e) => e) || [];
    message.condition = (object.condition !== undefined && object.condition !== null)
      ? ExprNode.fromPartial(object.condition)
      : undefined;
    message.leftTableId = object.leftTableId ?? 0;
    message.rightTableId = object.rightTableId ?? 0;
    message.leftInfo = (object.leftInfo !== undefined && object.leftInfo !== null)
      ? ArrangementInfo.fromPartial(object.leftInfo)
      : undefined;
    message.rightInfo = (object.rightInfo !== undefined && object.rightInfo !== null)
      ? ArrangementInfo.fromPartial(object.rightInfo)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseHopWindowNode(): HopWindowNode {
  return {
    timeCol: 0,
    windowSlide: undefined,
    windowSize: undefined,
    outputIndices: [],
    windowStartExprs: [],
    windowEndExprs: [],
  };
}

export const HopWindowNode = {
  fromJSON(object: any): HopWindowNode {
    return {
      timeCol: isSet(object.timeCol) ? Number(object.timeCol) : 0,
      windowSlide: isSet(object.windowSlide) ? IntervalUnit.fromJSON(object.windowSlide) : undefined,
      windowSize: isSet(object.windowSize) ? IntervalUnit.fromJSON(object.windowSize) : undefined,
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      windowStartExprs: Array.isArray(object?.windowStartExprs)
        ? object.windowStartExprs.map((e: any) => ExprNode.fromJSON(e))
        : [],
      windowEndExprs: Array.isArray(object?.windowEndExprs)
        ? object.windowEndExprs.map((e: any) => ExprNode.fromJSON(e))
        : [],
    };
  },

  toJSON(message: HopWindowNode): unknown {
    const obj: any = {};
    message.timeCol !== undefined && (obj.timeCol = Math.round(message.timeCol));
    message.windowSlide !== undefined &&
      (obj.windowSlide = message.windowSlide ? IntervalUnit.toJSON(message.windowSlide) : undefined);
    message.windowSize !== undefined &&
      (obj.windowSize = message.windowSize ? IntervalUnit.toJSON(message.windowSize) : undefined);
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    if (message.windowStartExprs) {
      obj.windowStartExprs = message.windowStartExprs.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.windowStartExprs = [];
    }
    if (message.windowEndExprs) {
      obj.windowEndExprs = message.windowEndExprs.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.windowEndExprs = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HopWindowNode>, I>>(object: I): HopWindowNode {
    const message = createBaseHopWindowNode();
    message.timeCol = object.timeCol ?? 0;
    message.windowSlide = (object.windowSlide !== undefined && object.windowSlide !== null)
      ? IntervalUnit.fromPartial(object.windowSlide)
      : undefined;
    message.windowSize = (object.windowSize !== undefined && object.windowSize !== null)
      ? IntervalUnit.fromPartial(object.windowSize)
      : undefined;
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.windowStartExprs = object.windowStartExprs?.map((e) => ExprNode.fromPartial(e)) || [];
    message.windowEndExprs = object.windowEndExprs?.map((e) => ExprNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseMergeNode(): MergeNode {
  return { upstreamActorId: [], upstreamFragmentId: 0, upstreamDispatcherType: DispatcherType.UNSPECIFIED, fields: [] };
}

export const MergeNode = {
  fromJSON(object: any): MergeNode {
    return {
      upstreamActorId: Array.isArray(object?.upstreamActorId) ? object.upstreamActorId.map((e: any) => Number(e)) : [],
      upstreamFragmentId: isSet(object.upstreamFragmentId) ? Number(object.upstreamFragmentId) : 0,
      upstreamDispatcherType: isSet(object.upstreamDispatcherType)
        ? dispatcherTypeFromJSON(object.upstreamDispatcherType)
        : DispatcherType.UNSPECIFIED,
      fields: Array.isArray(object?.fields) ? object.fields.map((e: any) => Field.fromJSON(e)) : [],
    };
  },

  toJSON(message: MergeNode): unknown {
    const obj: any = {};
    if (message.upstreamActorId) {
      obj.upstreamActorId = message.upstreamActorId.map((e) => Math.round(e));
    } else {
      obj.upstreamActorId = [];
    }
    message.upstreamFragmentId !== undefined && (obj.upstreamFragmentId = Math.round(message.upstreamFragmentId));
    message.upstreamDispatcherType !== undefined &&
      (obj.upstreamDispatcherType = dispatcherTypeToJSON(message.upstreamDispatcherType));
    if (message.fields) {
      obj.fields = message.fields.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.fields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MergeNode>, I>>(object: I): MergeNode {
    const message = createBaseMergeNode();
    message.upstreamActorId = object.upstreamActorId?.map((e) => e) || [];
    message.upstreamFragmentId = object.upstreamFragmentId ?? 0;
    message.upstreamDispatcherType = object.upstreamDispatcherType ?? DispatcherType.UNSPECIFIED;
    message.fields = object.fields?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseExchangeNode(): ExchangeNode {
  return { strategy: undefined };
}

export const ExchangeNode = {
  fromJSON(object: any): ExchangeNode {
    return { strategy: isSet(object.strategy) ? DispatchStrategy.fromJSON(object.strategy) : undefined };
  },

  toJSON(message: ExchangeNode): unknown {
    const obj: any = {};
    message.strategy !== undefined &&
      (obj.strategy = message.strategy ? DispatchStrategy.toJSON(message.strategy) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExchangeNode>, I>>(object: I): ExchangeNode {
    const message = createBaseExchangeNode();
    message.strategy = (object.strategy !== undefined && object.strategy !== null)
      ? DispatchStrategy.fromPartial(object.strategy)
      : undefined;
    return message;
  },
};

function createBaseChainNode(): ChainNode {
  return {
    tableId: 0,
    upstreamFields: [],
    upstreamColumnIndices: [],
    upstreamColumnIds: [],
    chainType: ChainType.CHAIN_UNSPECIFIED,
    isSingleton: false,
    tableDesc: undefined,
  };
}

export const ChainNode = {
  fromJSON(object: any): ChainNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      upstreamFields: Array.isArray(object?.upstreamFields)
        ? object.upstreamFields.map((e: any) => Field.fromJSON(e))
        : [],
      upstreamColumnIndices: Array.isArray(object?.upstreamColumnIndices)
        ? object.upstreamColumnIndices.map((e: any) => Number(e))
        : [],
      upstreamColumnIds: Array.isArray(object?.upstreamColumnIds)
        ? object.upstreamColumnIds.map((e: any) => Number(e))
        : [],
      chainType: isSet(object.chainType) ? chainTypeFromJSON(object.chainType) : ChainType.CHAIN_UNSPECIFIED,
      isSingleton: isSet(object.isSingleton) ? Boolean(object.isSingleton) : false,
      tableDesc: isSet(object.tableDesc) ? StorageTableDesc.fromJSON(object.tableDesc) : undefined,
    };
  },

  toJSON(message: ChainNode): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    if (message.upstreamFields) {
      obj.upstreamFields = message.upstreamFields.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.upstreamFields = [];
    }
    if (message.upstreamColumnIndices) {
      obj.upstreamColumnIndices = message.upstreamColumnIndices.map((e) => Math.round(e));
    } else {
      obj.upstreamColumnIndices = [];
    }
    if (message.upstreamColumnIds) {
      obj.upstreamColumnIds = message.upstreamColumnIds.map((e) => Math.round(e));
    } else {
      obj.upstreamColumnIds = [];
    }
    message.chainType !== undefined && (obj.chainType = chainTypeToJSON(message.chainType));
    message.isSingleton !== undefined && (obj.isSingleton = message.isSingleton);
    message.tableDesc !== undefined &&
      (obj.tableDesc = message.tableDesc ? StorageTableDesc.toJSON(message.tableDesc) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ChainNode>, I>>(object: I): ChainNode {
    const message = createBaseChainNode();
    message.tableId = object.tableId ?? 0;
    message.upstreamFields = object.upstreamFields?.map((e) => Field.fromPartial(e)) || [];
    message.upstreamColumnIndices = object.upstreamColumnIndices?.map((e) => e) || [];
    message.upstreamColumnIds = object.upstreamColumnIds?.map((e) => e) || [];
    message.chainType = object.chainType ?? ChainType.CHAIN_UNSPECIFIED;
    message.isSingleton = object.isSingleton ?? false;
    message.tableDesc = (object.tableDesc !== undefined && object.tableDesc !== null)
      ? StorageTableDesc.fromPartial(object.tableDesc)
      : undefined;
    return message;
  },
};

function createBaseBatchPlanNode(): BatchPlanNode {
  return { tableDesc: undefined, columnIds: [] };
}

export const BatchPlanNode = {
  fromJSON(object: any): BatchPlanNode {
    return {
      tableDesc: isSet(object.tableDesc) ? StorageTableDesc.fromJSON(object.tableDesc) : undefined,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: BatchPlanNode): unknown {
    const obj: any = {};
    message.tableDesc !== undefined &&
      (obj.tableDesc = message.tableDesc ? StorageTableDesc.toJSON(message.tableDesc) : undefined);
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BatchPlanNode>, I>>(object: I): BatchPlanNode {
    const message = createBaseBatchPlanNode();
    message.tableDesc = (object.tableDesc !== undefined && object.tableDesc !== null)
      ? StorageTableDesc.fromPartial(object.tableDesc)
      : undefined;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseArrangementInfo(): ArrangementInfo {
  return { arrangeKeyOrders: [], columnDescs: [], tableDesc: undefined };
}

export const ArrangementInfo = {
  fromJSON(object: any): ArrangementInfo {
    return {
      arrangeKeyOrders: Array.isArray(object?.arrangeKeyOrders)
        ? object.arrangeKeyOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
      columnDescs: Array.isArray(object?.columnDescs)
        ? object.columnDescs.map((e: any) => ColumnDesc.fromJSON(e))
        : [],
      tableDesc: isSet(object.tableDesc) ? StorageTableDesc.fromJSON(object.tableDesc) : undefined,
    };
  },

  toJSON(message: ArrangementInfo): unknown {
    const obj: any = {};
    if (message.arrangeKeyOrders) {
      obj.arrangeKeyOrders = message.arrangeKeyOrders.map((e) => e ? ColumnOrder.toJSON(e) : undefined);
    } else {
      obj.arrangeKeyOrders = [];
    }
    if (message.columnDescs) {
      obj.columnDescs = message.columnDescs.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.columnDescs = [];
    }
    message.tableDesc !== undefined &&
      (obj.tableDesc = message.tableDesc ? StorageTableDesc.toJSON(message.tableDesc) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ArrangementInfo>, I>>(object: I): ArrangementInfo {
    const message = createBaseArrangementInfo();
    message.arrangeKeyOrders = object.arrangeKeyOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.columnDescs = object.columnDescs?.map((e) => ColumnDesc.fromPartial(e)) || [];
    message.tableDesc = (object.tableDesc !== undefined && object.tableDesc !== null)
      ? StorageTableDesc.fromPartial(object.tableDesc)
      : undefined;
    return message;
  },
};

function createBaseArrangeNode(): ArrangeNode {
  return {
    tableInfo: undefined,
    distributionKey: [],
    table: undefined,
    handlePkConflictBehavior: HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
  };
}

export const ArrangeNode = {
  fromJSON(object: any): ArrangeNode {
    return {
      tableInfo: isSet(object.tableInfo) ? ArrangementInfo.fromJSON(object.tableInfo) : undefined,
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
      handlePkConflictBehavior: isSet(object.handlePkConflictBehavior)
        ? handleConflictBehaviorFromJSON(object.handlePkConflictBehavior)
        : HandleConflictBehavior.NO_CHECK_UNSPECIFIED,
    };
  },

  toJSON(message: ArrangeNode): unknown {
    const obj: any = {};
    message.tableInfo !== undefined &&
      (obj.tableInfo = message.tableInfo ? ArrangementInfo.toJSON(message.tableInfo) : undefined);
    if (message.distributionKey) {
      obj.distributionKey = message.distributionKey.map((e) => Math.round(e));
    } else {
      obj.distributionKey = [];
    }
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    message.handlePkConflictBehavior !== undefined &&
      (obj.handlePkConflictBehavior = handleConflictBehaviorToJSON(message.handlePkConflictBehavior));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ArrangeNode>, I>>(object: I): ArrangeNode {
    const message = createBaseArrangeNode();
    message.tableInfo = (object.tableInfo !== undefined && object.tableInfo !== null)
      ? ArrangementInfo.fromPartial(object.tableInfo)
      : undefined;
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    message.handlePkConflictBehavior = object.handlePkConflictBehavior ?? HandleConflictBehavior.NO_CHECK_UNSPECIFIED;
    return message;
  },
};

function createBaseLookupNode(): LookupNode {
  return {
    arrangeKey: [],
    streamKey: [],
    useCurrentEpoch: false,
    columnMapping: [],
    arrangementTableId: undefined,
    arrangementTableInfo: undefined,
  };
}

export const LookupNode = {
  fromJSON(object: any): LookupNode {
    return {
      arrangeKey: Array.isArray(object?.arrangeKey) ? object.arrangeKey.map((e: any) => Number(e)) : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      useCurrentEpoch: isSet(object.useCurrentEpoch) ? Boolean(object.useCurrentEpoch) : false,
      columnMapping: Array.isArray(object?.columnMapping) ? object.columnMapping.map((e: any) => Number(e)) : [],
      arrangementTableId: isSet(object.tableId)
        ? { $case: "tableId", tableId: Number(object.tableId) }
        : isSet(object.indexId)
        ? { $case: "indexId", indexId: Number(object.indexId) }
        : undefined,
      arrangementTableInfo: isSet(object.arrangementTableInfo)
        ? ArrangementInfo.fromJSON(object.arrangementTableInfo)
        : undefined,
    };
  },

  toJSON(message: LookupNode): unknown {
    const obj: any = {};
    if (message.arrangeKey) {
      obj.arrangeKey = message.arrangeKey.map((e) => Math.round(e));
    } else {
      obj.arrangeKey = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
    } else {
      obj.streamKey = [];
    }
    message.useCurrentEpoch !== undefined && (obj.useCurrentEpoch = message.useCurrentEpoch);
    if (message.columnMapping) {
      obj.columnMapping = message.columnMapping.map((e) => Math.round(e));
    } else {
      obj.columnMapping = [];
    }
    message.arrangementTableId?.$case === "tableId" && (obj.tableId = Math.round(message.arrangementTableId?.tableId));
    message.arrangementTableId?.$case === "indexId" && (obj.indexId = Math.round(message.arrangementTableId?.indexId));
    message.arrangementTableInfo !== undefined && (obj.arrangementTableInfo = message.arrangementTableInfo
      ? ArrangementInfo.toJSON(message.arrangementTableInfo)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LookupNode>, I>>(object: I): LookupNode {
    const message = createBaseLookupNode();
    message.arrangeKey = object.arrangeKey?.map((e) => e) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.useCurrentEpoch = object.useCurrentEpoch ?? false;
    message.columnMapping = object.columnMapping?.map((e) => e) || [];
    if (
      object.arrangementTableId?.$case === "tableId" &&
      object.arrangementTableId?.tableId !== undefined &&
      object.arrangementTableId?.tableId !== null
    ) {
      message.arrangementTableId = { $case: "tableId", tableId: object.arrangementTableId.tableId };
    }
    if (
      object.arrangementTableId?.$case === "indexId" &&
      object.arrangementTableId?.indexId !== undefined &&
      object.arrangementTableId?.indexId !== null
    ) {
      message.arrangementTableId = { $case: "indexId", indexId: object.arrangementTableId.indexId };
    }
    message.arrangementTableInfo = (object.arrangementTableInfo !== undefined && object.arrangementTableInfo !== null)
      ? ArrangementInfo.fromPartial(object.arrangementTableInfo)
      : undefined;
    return message;
  },
};

function createBaseWatermarkFilterNode(): WatermarkFilterNode {
  return { watermarkDescs: [], tables: [] };
}

export const WatermarkFilterNode = {
  fromJSON(object: any): WatermarkFilterNode {
    return {
      watermarkDescs: Array.isArray(object?.watermarkDescs)
        ? object.watermarkDescs.map((e: any) => WatermarkDesc.fromJSON(e))
        : [],
      tables: Array.isArray(object?.tables)
        ? object.tables.map((e: any) => Table.fromJSON(e))
        : [],
    };
  },

  toJSON(message: WatermarkFilterNode): unknown {
    const obj: any = {};
    if (message.watermarkDescs) {
      obj.watermarkDescs = message.watermarkDescs.map((e) => e ? WatermarkDesc.toJSON(e) : undefined);
    } else {
      obj.watermarkDescs = [];
    }
    if (message.tables) {
      obj.tables = message.tables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.tables = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<WatermarkFilterNode>, I>>(object: I): WatermarkFilterNode {
    const message = createBaseWatermarkFilterNode();
    message.watermarkDescs = object.watermarkDescs?.map((e) => WatermarkDesc.fromPartial(e)) || [];
    message.tables = object.tables?.map((e) => Table.fromPartial(e)) || [];
    return message;
  },
};

function createBaseUnionNode(): UnionNode {
  return {};
}

export const UnionNode = {
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

function createBaseLookupUnionNode(): LookupUnionNode {
  return { order: [] };
}

export const LookupUnionNode = {
  fromJSON(object: any): LookupUnionNode {
    return { order: Array.isArray(object?.order) ? object.order.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: LookupUnionNode): unknown {
    const obj: any = {};
    if (message.order) {
      obj.order = message.order.map((e) => Math.round(e));
    } else {
      obj.order = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LookupUnionNode>, I>>(object: I): LookupUnionNode {
    const message = createBaseLookupUnionNode();
    message.order = object.order?.map((e) => e) || [];
    return message;
  },
};

function createBaseExpandNode(): ExpandNode {
  return { columnSubsets: [] };
}

export const ExpandNode = {
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

function createBaseSortNode(): SortNode {
  return { stateTable: undefined, sortColumnIndex: 0 };
}

export const SortNode = {
  fromJSON(object: any): SortNode {
    return {
      stateTable: isSet(object.stateTable) ? Table.fromJSON(object.stateTable) : undefined,
      sortColumnIndex: isSet(object.sortColumnIndex) ? Number(object.sortColumnIndex) : 0,
    };
  },

  toJSON(message: SortNode): unknown {
    const obj: any = {};
    message.stateTable !== undefined &&
      (obj.stateTable = message.stateTable ? Table.toJSON(message.stateTable) : undefined);
    message.sortColumnIndex !== undefined && (obj.sortColumnIndex = Math.round(message.sortColumnIndex));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SortNode>, I>>(object: I): SortNode {
    const message = createBaseSortNode();
    message.stateTable = (object.stateTable !== undefined && object.stateTable !== null)
      ? Table.fromPartial(object.stateTable)
      : undefined;
    message.sortColumnIndex = object.sortColumnIndex ?? 0;
    return message;
  },
};

function createBaseDmlNode(): DmlNode {
  return { tableId: 0, tableVersionId: 0, columnDescs: [] };
}

export const DmlNode = {
  fromJSON(object: any): DmlNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      tableVersionId: isSet(object.tableVersionId) ? Number(object.tableVersionId) : 0,
      columnDescs: Array.isArray(object?.columnDescs) ? object.columnDescs.map((e: any) => ColumnDesc.fromJSON(e)) : [],
    };
  },

  toJSON(message: DmlNode): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    message.tableVersionId !== undefined && (obj.tableVersionId = Math.round(message.tableVersionId));
    if (message.columnDescs) {
      obj.columnDescs = message.columnDescs.map((e) => e ? ColumnDesc.toJSON(e) : undefined);
    } else {
      obj.columnDescs = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DmlNode>, I>>(object: I): DmlNode {
    const message = createBaseDmlNode();
    message.tableId = object.tableId ?? 0;
    message.tableVersionId = object.tableVersionId ?? 0;
    message.columnDescs = object.columnDescs?.map((e) => ColumnDesc.fromPartial(e)) || [];
    return message;
  },
};

function createBaseRowIdGenNode(): RowIdGenNode {
  return { rowIdIndex: 0 };
}

export const RowIdGenNode = {
  fromJSON(object: any): RowIdGenNode {
    return { rowIdIndex: isSet(object.rowIdIndex) ? Number(object.rowIdIndex) : 0 };
  },

  toJSON(message: RowIdGenNode): unknown {
    const obj: any = {};
    message.rowIdIndex !== undefined && (obj.rowIdIndex = Math.round(message.rowIdIndex));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<RowIdGenNode>, I>>(object: I): RowIdGenNode {
    const message = createBaseRowIdGenNode();
    message.rowIdIndex = object.rowIdIndex ?? 0;
    return message;
  },
};

function createBaseNowNode(): NowNode {
  return { stateTable: undefined };
}

export const NowNode = {
  fromJSON(object: any): NowNode {
    return { stateTable: isSet(object.stateTable) ? Table.fromJSON(object.stateTable) : undefined };
  },

  toJSON(message: NowNode): unknown {
    const obj: any = {};
    message.stateTable !== undefined &&
      (obj.stateTable = message.stateTable ? Table.toJSON(message.stateTable) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<NowNode>, I>>(object: I): NowNode {
    const message = createBaseNowNode();
    message.stateTable = (object.stateTable !== undefined && object.stateTable !== null)
      ? Table.fromPartial(object.stateTable)
      : undefined;
    return message;
  },
};

function createBaseStreamNode(): StreamNode {
  return { nodeBody: undefined, operatorId: 0, input: [], streamKey: [], appendOnly: false, identity: "", fields: [] };
}

export const StreamNode = {
  fromJSON(object: any): StreamNode {
    return {
      nodeBody: isSet(object.source)
        ? { $case: "source", source: SourceNode.fromJSON(object.source) }
        : isSet(object.project)
        ? { $case: "project", project: ProjectNode.fromJSON(object.project) }
        : isSet(object.filter)
        ? { $case: "filter", filter: FilterNode.fromJSON(object.filter) }
        : isSet(object.materialize)
        ? { $case: "materialize", materialize: MaterializeNode.fromJSON(object.materialize) }
        : isSet(object.localSimpleAgg)
        ? { $case: "localSimpleAgg", localSimpleAgg: SimpleAggNode.fromJSON(object.localSimpleAgg) }
        : isSet(object.globalSimpleAgg)
        ? { $case: "globalSimpleAgg", globalSimpleAgg: SimpleAggNode.fromJSON(object.globalSimpleAgg) }
        : isSet(object.hashAgg)
        ? { $case: "hashAgg", hashAgg: HashAggNode.fromJSON(object.hashAgg) }
        : isSet(object.appendOnlyTopN)
        ? { $case: "appendOnlyTopN", appendOnlyTopN: TopNNode.fromJSON(object.appendOnlyTopN) }
        : isSet(object.hashJoin)
        ? { $case: "hashJoin", hashJoin: HashJoinNode.fromJSON(object.hashJoin) }
        : isSet(object.topN)
        ? { $case: "topN", topN: TopNNode.fromJSON(object.topN) }
        : isSet(object.hopWindow)
        ? { $case: "hopWindow", hopWindow: HopWindowNode.fromJSON(object.hopWindow) }
        : isSet(object.merge)
        ? { $case: "merge", merge: MergeNode.fromJSON(object.merge) }
        : isSet(object.exchange)
        ? { $case: "exchange", exchange: ExchangeNode.fromJSON(object.exchange) }
        : isSet(object.chain)
        ? { $case: "chain", chain: ChainNode.fromJSON(object.chain) }
        : isSet(object.batchPlan)
        ? { $case: "batchPlan", batchPlan: BatchPlanNode.fromJSON(object.batchPlan) }
        : isSet(object.lookup)
        ? { $case: "lookup", lookup: LookupNode.fromJSON(object.lookup) }
        : isSet(object.arrange)
        ? { $case: "arrange", arrange: ArrangeNode.fromJSON(object.arrange) }
        : isSet(object.lookupUnion)
        ? { $case: "lookupUnion", lookupUnion: LookupUnionNode.fromJSON(object.lookupUnion) }
        : isSet(object.union)
        ? { $case: "union", union: UnionNode.fromJSON(object.union) }
        : isSet(object.deltaIndexJoin)
        ? { $case: "deltaIndexJoin", deltaIndexJoin: DeltaIndexJoinNode.fromJSON(object.deltaIndexJoin) }
        : isSet(object.sink)
        ? { $case: "sink", sink: SinkNode.fromJSON(object.sink) }
        : isSet(object.expand)
        ? { $case: "expand", expand: ExpandNode.fromJSON(object.expand) }
        : isSet(object.dynamicFilter)
        ? { $case: "dynamicFilter", dynamicFilter: DynamicFilterNode.fromJSON(object.dynamicFilter) }
        : isSet(object.projectSet)
        ? { $case: "projectSet", projectSet: ProjectSetNode.fromJSON(object.projectSet) }
        : isSet(object.groupTopN)
        ? { $case: "groupTopN", groupTopN: GroupTopNNode.fromJSON(object.groupTopN) }
        : isSet(object.sort)
        ? { $case: "sort", sort: SortNode.fromJSON(object.sort) }
        : isSet(object.watermarkFilter)
        ? { $case: "watermarkFilter", watermarkFilter: WatermarkFilterNode.fromJSON(object.watermarkFilter) }
        : isSet(object.dml)
        ? { $case: "dml", dml: DmlNode.fromJSON(object.dml) }
        : isSet(object.rowIdGen)
        ? { $case: "rowIdGen", rowIdGen: RowIdGenNode.fromJSON(object.rowIdGen) }
        : isSet(object.now)
        ? { $case: "now", now: NowNode.fromJSON(object.now) }
        : isSet(object.appendOnlyGroupTopN)
        ? { $case: "appendOnlyGroupTopN", appendOnlyGroupTopN: GroupTopNNode.fromJSON(object.appendOnlyGroupTopN) }
        : isSet(object.temporalJoin)
        ? { $case: "temporalJoin", temporalJoin: TemporalJoinNode.fromJSON(object.temporalJoin) }
        : undefined,
      operatorId: isSet(object.operatorId) ? Number(object.operatorId) : 0,
      input: Array.isArray(object?.input)
        ? object.input.map((e: any) => StreamNode.fromJSON(e))
        : [],
      streamKey: Array.isArray(object?.streamKey) ? object.streamKey.map((e: any) => Number(e)) : [],
      appendOnly: isSet(object.appendOnly) ? Boolean(object.appendOnly) : false,
      identity: isSet(object.identity) ? String(object.identity) : "",
      fields: Array.isArray(object?.fields) ? object.fields.map((e: any) => Field.fromJSON(e)) : [],
    };
  },

  toJSON(message: StreamNode): unknown {
    const obj: any = {};
    message.nodeBody?.$case === "source" &&
      (obj.source = message.nodeBody?.source ? SourceNode.toJSON(message.nodeBody?.source) : undefined);
    message.nodeBody?.$case === "project" &&
      (obj.project = message.nodeBody?.project ? ProjectNode.toJSON(message.nodeBody?.project) : undefined);
    message.nodeBody?.$case === "filter" &&
      (obj.filter = message.nodeBody?.filter ? FilterNode.toJSON(message.nodeBody?.filter) : undefined);
    message.nodeBody?.$case === "materialize" && (obj.materialize = message.nodeBody?.materialize
      ? MaterializeNode.toJSON(message.nodeBody?.materialize)
      : undefined);
    message.nodeBody?.$case === "localSimpleAgg" && (obj.localSimpleAgg = message.nodeBody?.localSimpleAgg
      ? SimpleAggNode.toJSON(message.nodeBody?.localSimpleAgg)
      : undefined);
    message.nodeBody?.$case === "globalSimpleAgg" && (obj.globalSimpleAgg = message.nodeBody?.globalSimpleAgg
      ? SimpleAggNode.toJSON(message.nodeBody?.globalSimpleAgg)
      : undefined);
    message.nodeBody?.$case === "hashAgg" &&
      (obj.hashAgg = message.nodeBody?.hashAgg ? HashAggNode.toJSON(message.nodeBody?.hashAgg) : undefined);
    message.nodeBody?.$case === "appendOnlyTopN" && (obj.appendOnlyTopN = message.nodeBody?.appendOnlyTopN
      ? TopNNode.toJSON(message.nodeBody?.appendOnlyTopN)
      : undefined);
    message.nodeBody?.$case === "hashJoin" &&
      (obj.hashJoin = message.nodeBody?.hashJoin ? HashJoinNode.toJSON(message.nodeBody?.hashJoin) : undefined);
    message.nodeBody?.$case === "topN" &&
      (obj.topN = message.nodeBody?.topN ? TopNNode.toJSON(message.nodeBody?.topN) : undefined);
    message.nodeBody?.$case === "hopWindow" &&
      (obj.hopWindow = message.nodeBody?.hopWindow ? HopWindowNode.toJSON(message.nodeBody?.hopWindow) : undefined);
    message.nodeBody?.$case === "merge" &&
      (obj.merge = message.nodeBody?.merge ? MergeNode.toJSON(message.nodeBody?.merge) : undefined);
    message.nodeBody?.$case === "exchange" &&
      (obj.exchange = message.nodeBody?.exchange ? ExchangeNode.toJSON(message.nodeBody?.exchange) : undefined);
    message.nodeBody?.$case === "chain" &&
      (obj.chain = message.nodeBody?.chain ? ChainNode.toJSON(message.nodeBody?.chain) : undefined);
    message.nodeBody?.$case === "batchPlan" &&
      (obj.batchPlan = message.nodeBody?.batchPlan ? BatchPlanNode.toJSON(message.nodeBody?.batchPlan) : undefined);
    message.nodeBody?.$case === "lookup" &&
      (obj.lookup = message.nodeBody?.lookup ? LookupNode.toJSON(message.nodeBody?.lookup) : undefined);
    message.nodeBody?.$case === "arrange" &&
      (obj.arrange = message.nodeBody?.arrange ? ArrangeNode.toJSON(message.nodeBody?.arrange) : undefined);
    message.nodeBody?.$case === "lookupUnion" && (obj.lookupUnion = message.nodeBody?.lookupUnion
      ? LookupUnionNode.toJSON(message.nodeBody?.lookupUnion)
      : undefined);
    message.nodeBody?.$case === "union" &&
      (obj.union = message.nodeBody?.union ? UnionNode.toJSON(message.nodeBody?.union) : undefined);
    message.nodeBody?.$case === "deltaIndexJoin" && (obj.deltaIndexJoin = message.nodeBody?.deltaIndexJoin
      ? DeltaIndexJoinNode.toJSON(message.nodeBody?.deltaIndexJoin)
      : undefined);
    message.nodeBody?.$case === "sink" &&
      (obj.sink = message.nodeBody?.sink ? SinkNode.toJSON(message.nodeBody?.sink) : undefined);
    message.nodeBody?.$case === "expand" &&
      (obj.expand = message.nodeBody?.expand ? ExpandNode.toJSON(message.nodeBody?.expand) : undefined);
    message.nodeBody?.$case === "dynamicFilter" && (obj.dynamicFilter = message.nodeBody?.dynamicFilter
      ? DynamicFilterNode.toJSON(message.nodeBody?.dynamicFilter)
      : undefined);
    message.nodeBody?.$case === "projectSet" &&
      (obj.projectSet = message.nodeBody?.projectSet ? ProjectSetNode.toJSON(message.nodeBody?.projectSet) : undefined);
    message.nodeBody?.$case === "groupTopN" &&
      (obj.groupTopN = message.nodeBody?.groupTopN ? GroupTopNNode.toJSON(message.nodeBody?.groupTopN) : undefined);
    message.nodeBody?.$case === "sort" &&
      (obj.sort = message.nodeBody?.sort ? SortNode.toJSON(message.nodeBody?.sort) : undefined);
    message.nodeBody?.$case === "watermarkFilter" && (obj.watermarkFilter = message.nodeBody?.watermarkFilter
      ? WatermarkFilterNode.toJSON(message.nodeBody?.watermarkFilter)
      : undefined);
    message.nodeBody?.$case === "dml" &&
      (obj.dml = message.nodeBody?.dml ? DmlNode.toJSON(message.nodeBody?.dml) : undefined);
    message.nodeBody?.$case === "rowIdGen" &&
      (obj.rowIdGen = message.nodeBody?.rowIdGen ? RowIdGenNode.toJSON(message.nodeBody?.rowIdGen) : undefined);
    message.nodeBody?.$case === "now" &&
      (obj.now = message.nodeBody?.now ? NowNode.toJSON(message.nodeBody?.now) : undefined);
    message.nodeBody?.$case === "appendOnlyGroupTopN" &&
      (obj.appendOnlyGroupTopN = message.nodeBody?.appendOnlyGroupTopN
        ? GroupTopNNode.toJSON(message.nodeBody?.appendOnlyGroupTopN)
        : undefined);
    message.nodeBody?.$case === "temporalJoin" && (obj.temporalJoin = message.nodeBody?.temporalJoin
      ? TemporalJoinNode.toJSON(message.nodeBody?.temporalJoin)
      : undefined);
    message.operatorId !== undefined && (obj.operatorId = Math.round(message.operatorId));
    if (message.input) {
      obj.input = message.input.map((e) =>
        e ? StreamNode.toJSON(e) : undefined
      );
    } else {
      obj.input = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) =>
        Math.round(e)
      );
    } else {
      obj.streamKey = [];
    }
    message.appendOnly !== undefined && (obj.appendOnly = message.appendOnly);
    message.identity !== undefined && (obj.identity = message.identity);
    if (message.fields) {
      obj.fields = message.fields.map((e) => e ? Field.toJSON(e) : undefined);
    } else {
      obj.fields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamNode>, I>>(object: I): StreamNode {
    const message = createBaseStreamNode();
    if (
      object.nodeBody?.$case === "source" && object.nodeBody?.source !== undefined && object.nodeBody?.source !== null
    ) {
      message.nodeBody = { $case: "source", source: SourceNode.fromPartial(object.nodeBody.source) };
    }
    if (
      object.nodeBody?.$case === "project" &&
      object.nodeBody?.project !== undefined &&
      object.nodeBody?.project !== null
    ) {
      message.nodeBody = { $case: "project", project: ProjectNode.fromPartial(object.nodeBody.project) };
    }
    if (
      object.nodeBody?.$case === "filter" && object.nodeBody?.filter !== undefined && object.nodeBody?.filter !== null
    ) {
      message.nodeBody = { $case: "filter", filter: FilterNode.fromPartial(object.nodeBody.filter) };
    }
    if (
      object.nodeBody?.$case === "materialize" &&
      object.nodeBody?.materialize !== undefined &&
      object.nodeBody?.materialize !== null
    ) {
      message.nodeBody = {
        $case: "materialize",
        materialize: MaterializeNode.fromPartial(object.nodeBody.materialize),
      };
    }
    if (
      object.nodeBody?.$case === "localSimpleAgg" &&
      object.nodeBody?.localSimpleAgg !== undefined &&
      object.nodeBody?.localSimpleAgg !== null
    ) {
      message.nodeBody = {
        $case: "localSimpleAgg",
        localSimpleAgg: SimpleAggNode.fromPartial(object.nodeBody.localSimpleAgg),
      };
    }
    if (
      object.nodeBody?.$case === "globalSimpleAgg" &&
      object.nodeBody?.globalSimpleAgg !== undefined &&
      object.nodeBody?.globalSimpleAgg !== null
    ) {
      message.nodeBody = {
        $case: "globalSimpleAgg",
        globalSimpleAgg: SimpleAggNode.fromPartial(object.nodeBody.globalSimpleAgg),
      };
    }
    if (
      object.nodeBody?.$case === "hashAgg" &&
      object.nodeBody?.hashAgg !== undefined &&
      object.nodeBody?.hashAgg !== null
    ) {
      message.nodeBody = { $case: "hashAgg", hashAgg: HashAggNode.fromPartial(object.nodeBody.hashAgg) };
    }
    if (
      object.nodeBody?.$case === "appendOnlyTopN" &&
      object.nodeBody?.appendOnlyTopN !== undefined &&
      object.nodeBody?.appendOnlyTopN !== null
    ) {
      message.nodeBody = {
        $case: "appendOnlyTopN",
        appendOnlyTopN: TopNNode.fromPartial(object.nodeBody.appendOnlyTopN),
      };
    }
    if (
      object.nodeBody?.$case === "hashJoin" &&
      object.nodeBody?.hashJoin !== undefined &&
      object.nodeBody?.hashJoin !== null
    ) {
      message.nodeBody = { $case: "hashJoin", hashJoin: HashJoinNode.fromPartial(object.nodeBody.hashJoin) };
    }
    if (object.nodeBody?.$case === "topN" && object.nodeBody?.topN !== undefined && object.nodeBody?.topN !== null) {
      message.nodeBody = { $case: "topN", topN: TopNNode.fromPartial(object.nodeBody.topN) };
    }
    if (
      object.nodeBody?.$case === "hopWindow" &&
      object.nodeBody?.hopWindow !== undefined &&
      object.nodeBody?.hopWindow !== null
    ) {
      message.nodeBody = { $case: "hopWindow", hopWindow: HopWindowNode.fromPartial(object.nodeBody.hopWindow) };
    }
    if (object.nodeBody?.$case === "merge" && object.nodeBody?.merge !== undefined && object.nodeBody?.merge !== null) {
      message.nodeBody = { $case: "merge", merge: MergeNode.fromPartial(object.nodeBody.merge) };
    }
    if (
      object.nodeBody?.$case === "exchange" &&
      object.nodeBody?.exchange !== undefined &&
      object.nodeBody?.exchange !== null
    ) {
      message.nodeBody = { $case: "exchange", exchange: ExchangeNode.fromPartial(object.nodeBody.exchange) };
    }
    if (object.nodeBody?.$case === "chain" && object.nodeBody?.chain !== undefined && object.nodeBody?.chain !== null) {
      message.nodeBody = { $case: "chain", chain: ChainNode.fromPartial(object.nodeBody.chain) };
    }
    if (
      object.nodeBody?.$case === "batchPlan" &&
      object.nodeBody?.batchPlan !== undefined &&
      object.nodeBody?.batchPlan !== null
    ) {
      message.nodeBody = { $case: "batchPlan", batchPlan: BatchPlanNode.fromPartial(object.nodeBody.batchPlan) };
    }
    if (
      object.nodeBody?.$case === "lookup" && object.nodeBody?.lookup !== undefined && object.nodeBody?.lookup !== null
    ) {
      message.nodeBody = { $case: "lookup", lookup: LookupNode.fromPartial(object.nodeBody.lookup) };
    }
    if (
      object.nodeBody?.$case === "arrange" &&
      object.nodeBody?.arrange !== undefined &&
      object.nodeBody?.arrange !== null
    ) {
      message.nodeBody = { $case: "arrange", arrange: ArrangeNode.fromPartial(object.nodeBody.arrange) };
    }
    if (
      object.nodeBody?.$case === "lookupUnion" &&
      object.nodeBody?.lookupUnion !== undefined &&
      object.nodeBody?.lookupUnion !== null
    ) {
      message.nodeBody = {
        $case: "lookupUnion",
        lookupUnion: LookupUnionNode.fromPartial(object.nodeBody.lookupUnion),
      };
    }
    if (object.nodeBody?.$case === "union" && object.nodeBody?.union !== undefined && object.nodeBody?.union !== null) {
      message.nodeBody = { $case: "union", union: UnionNode.fromPartial(object.nodeBody.union) };
    }
    if (
      object.nodeBody?.$case === "deltaIndexJoin" &&
      object.nodeBody?.deltaIndexJoin !== undefined &&
      object.nodeBody?.deltaIndexJoin !== null
    ) {
      message.nodeBody = {
        $case: "deltaIndexJoin",
        deltaIndexJoin: DeltaIndexJoinNode.fromPartial(object.nodeBody.deltaIndexJoin),
      };
    }
    if (object.nodeBody?.$case === "sink" && object.nodeBody?.sink !== undefined && object.nodeBody?.sink !== null) {
      message.nodeBody = { $case: "sink", sink: SinkNode.fromPartial(object.nodeBody.sink) };
    }
    if (
      object.nodeBody?.$case === "expand" && object.nodeBody?.expand !== undefined && object.nodeBody?.expand !== null
    ) {
      message.nodeBody = { $case: "expand", expand: ExpandNode.fromPartial(object.nodeBody.expand) };
    }
    if (
      object.nodeBody?.$case === "dynamicFilter" &&
      object.nodeBody?.dynamicFilter !== undefined &&
      object.nodeBody?.dynamicFilter !== null
    ) {
      message.nodeBody = {
        $case: "dynamicFilter",
        dynamicFilter: DynamicFilterNode.fromPartial(object.nodeBody.dynamicFilter),
      };
    }
    if (
      object.nodeBody?.$case === "projectSet" &&
      object.nodeBody?.projectSet !== undefined &&
      object.nodeBody?.projectSet !== null
    ) {
      message.nodeBody = { $case: "projectSet", projectSet: ProjectSetNode.fromPartial(object.nodeBody.projectSet) };
    }
    if (
      object.nodeBody?.$case === "groupTopN" &&
      object.nodeBody?.groupTopN !== undefined &&
      object.nodeBody?.groupTopN !== null
    ) {
      message.nodeBody = { $case: "groupTopN", groupTopN: GroupTopNNode.fromPartial(object.nodeBody.groupTopN) };
    }
    if (object.nodeBody?.$case === "sort" && object.nodeBody?.sort !== undefined && object.nodeBody?.sort !== null) {
      message.nodeBody = { $case: "sort", sort: SortNode.fromPartial(object.nodeBody.sort) };
    }
    if (
      object.nodeBody?.$case === "watermarkFilter" &&
      object.nodeBody?.watermarkFilter !== undefined &&
      object.nodeBody?.watermarkFilter !== null
    ) {
      message.nodeBody = {
        $case: "watermarkFilter",
        watermarkFilter: WatermarkFilterNode.fromPartial(object.nodeBody.watermarkFilter),
      };
    }
    if (object.nodeBody?.$case === "dml" && object.nodeBody?.dml !== undefined && object.nodeBody?.dml !== null) {
      message.nodeBody = { $case: "dml", dml: DmlNode.fromPartial(object.nodeBody.dml) };
    }
    if (
      object.nodeBody?.$case === "rowIdGen" &&
      object.nodeBody?.rowIdGen !== undefined &&
      object.nodeBody?.rowIdGen !== null
    ) {
      message.nodeBody = { $case: "rowIdGen", rowIdGen: RowIdGenNode.fromPartial(object.nodeBody.rowIdGen) };
    }
    if (object.nodeBody?.$case === "now" && object.nodeBody?.now !== undefined && object.nodeBody?.now !== null) {
      message.nodeBody = { $case: "now", now: NowNode.fromPartial(object.nodeBody.now) };
    }
    if (
      object.nodeBody?.$case === "appendOnlyGroupTopN" &&
      object.nodeBody?.appendOnlyGroupTopN !== undefined &&
      object.nodeBody?.appendOnlyGroupTopN !== null
    ) {
      message.nodeBody = {
        $case: "appendOnlyGroupTopN",
        appendOnlyGroupTopN: GroupTopNNode.fromPartial(object.nodeBody.appendOnlyGroupTopN),
      };
    }
    if (
      object.nodeBody?.$case === "temporalJoin" &&
      object.nodeBody?.temporalJoin !== undefined &&
      object.nodeBody?.temporalJoin !== null
    ) {
      message.nodeBody = {
        $case: "temporalJoin",
        temporalJoin: TemporalJoinNode.fromPartial(object.nodeBody.temporalJoin),
      };
    }
    message.operatorId = object.operatorId ?? 0;
    message.input = object.input?.map((e) => StreamNode.fromPartial(e)) || [];
    message.streamKey = object.streamKey?.map((e) => e) || [];
    message.appendOnly = object.appendOnly ?? false;
    message.identity = object.identity ?? "";
    message.fields = object.fields?.map((e) => Field.fromPartial(e)) || [];
    return message;
  },
};

function createBaseDispatchStrategy(): DispatchStrategy {
  return { type: DispatcherType.UNSPECIFIED, distKeyIndices: [], outputIndices: [] };
}

export const DispatchStrategy = {
  fromJSON(object: any): DispatchStrategy {
    return {
      type: isSet(object.type) ? dispatcherTypeFromJSON(object.type) : DispatcherType.UNSPECIFIED,
      distKeyIndices: Array.isArray(object?.distKeyIndices) ? object.distKeyIndices.map((e: any) => Number(e)) : [],
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: DispatchStrategy): unknown {
    const obj: any = {};
    message.type !== undefined && (obj.type = dispatcherTypeToJSON(message.type));
    if (message.distKeyIndices) {
      obj.distKeyIndices = message.distKeyIndices.map((e) => Math.round(e));
    } else {
      obj.distKeyIndices = [];
    }
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DispatchStrategy>, I>>(object: I): DispatchStrategy {
    const message = createBaseDispatchStrategy();
    message.type = object.type ?? DispatcherType.UNSPECIFIED;
    message.distKeyIndices = object.distKeyIndices?.map((e) => e) || [];
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseDispatcher(): Dispatcher {
  return {
    type: DispatcherType.UNSPECIFIED,
    distKeyIndices: [],
    outputIndices: [],
    hashMapping: undefined,
    dispatcherId: 0,
    downstreamActorId: [],
  };
}

export const Dispatcher = {
  fromJSON(object: any): Dispatcher {
    return {
      type: isSet(object.type) ? dispatcherTypeFromJSON(object.type) : DispatcherType.UNSPECIFIED,
      distKeyIndices: Array.isArray(object?.distKeyIndices) ? object.distKeyIndices.map((e: any) => Number(e)) : [],
      outputIndices: Array.isArray(object?.outputIndices) ? object.outputIndices.map((e: any) => Number(e)) : [],
      hashMapping: isSet(object.hashMapping) ? ActorMapping.fromJSON(object.hashMapping) : undefined,
      dispatcherId: isSet(object.dispatcherId) ? Number(object.dispatcherId) : 0,
      downstreamActorId: Array.isArray(object?.downstreamActorId)
        ? object.downstreamActorId.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: Dispatcher): unknown {
    const obj: any = {};
    message.type !== undefined && (obj.type = dispatcherTypeToJSON(message.type));
    if (message.distKeyIndices) {
      obj.distKeyIndices = message.distKeyIndices.map((e) => Math.round(e));
    } else {
      obj.distKeyIndices = [];
    }
    if (message.outputIndices) {
      obj.outputIndices = message.outputIndices.map((e) => Math.round(e));
    } else {
      obj.outputIndices = [];
    }
    message.hashMapping !== undefined &&
      (obj.hashMapping = message.hashMapping ? ActorMapping.toJSON(message.hashMapping) : undefined);
    message.dispatcherId !== undefined && (obj.dispatcherId = Math.round(message.dispatcherId));
    if (message.downstreamActorId) {
      obj.downstreamActorId = message.downstreamActorId.map((e) => Math.round(e));
    } else {
      obj.downstreamActorId = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Dispatcher>, I>>(object: I): Dispatcher {
    const message = createBaseDispatcher();
    message.type = object.type ?? DispatcherType.UNSPECIFIED;
    message.distKeyIndices = object.distKeyIndices?.map((e) => e) || [];
    message.outputIndices = object.outputIndices?.map((e) => e) || [];
    message.hashMapping = (object.hashMapping !== undefined && object.hashMapping !== null)
      ? ActorMapping.fromPartial(object.hashMapping)
      : undefined;
    message.dispatcherId = object.dispatcherId ?? 0;
    message.downstreamActorId = object.downstreamActorId?.map((e) => e) || [];
    return message;
  },
};

function createBaseStreamActor(): StreamActor {
  return {
    actorId: 0,
    fragmentId: 0,
    nodes: undefined,
    dispatcher: [],
    upstreamActorId: [],
    vnodeBitmap: undefined,
    mviewDefinition: "",
  };
}

export const StreamActor = {
  fromJSON(object: any): StreamActor {
    return {
      actorId: isSet(object.actorId) ? Number(object.actorId) : 0,
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      nodes: isSet(object.nodes) ? StreamNode.fromJSON(object.nodes) : undefined,
      dispatcher: Array.isArray(object?.dispatcher) ? object.dispatcher.map((e: any) => Dispatcher.fromJSON(e)) : [],
      upstreamActorId: Array.isArray(object?.upstreamActorId) ? object.upstreamActorId.map((e: any) => Number(e)) : [],
      vnodeBitmap: isSet(object.vnodeBitmap) ? Buffer.fromJSON(object.vnodeBitmap) : undefined,
      mviewDefinition: isSet(object.mviewDefinition) ? String(object.mviewDefinition) : "",
    };
  },

  toJSON(message: StreamActor): unknown {
    const obj: any = {};
    message.actorId !== undefined && (obj.actorId = Math.round(message.actorId));
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.nodes !== undefined && (obj.nodes = message.nodes ? StreamNode.toJSON(message.nodes) : undefined);
    if (message.dispatcher) {
      obj.dispatcher = message.dispatcher.map((e) => e ? Dispatcher.toJSON(e) : undefined);
    } else {
      obj.dispatcher = [];
    }
    if (message.upstreamActorId) {
      obj.upstreamActorId = message.upstreamActorId.map((e) => Math.round(e));
    } else {
      obj.upstreamActorId = [];
    }
    message.vnodeBitmap !== undefined &&
      (obj.vnodeBitmap = message.vnodeBitmap ? Buffer.toJSON(message.vnodeBitmap) : undefined);
    message.mviewDefinition !== undefined && (obj.mviewDefinition = message.mviewDefinition);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamActor>, I>>(object: I): StreamActor {
    const message = createBaseStreamActor();
    message.actorId = object.actorId ?? 0;
    message.fragmentId = object.fragmentId ?? 0;
    message.nodes = (object.nodes !== undefined && object.nodes !== null)
      ? StreamNode.fromPartial(object.nodes)
      : undefined;
    message.dispatcher = object.dispatcher?.map((e) => Dispatcher.fromPartial(e)) || [];
    message.upstreamActorId = object.upstreamActorId?.map((e) => e) || [];
    message.vnodeBitmap = (object.vnodeBitmap !== undefined && object.vnodeBitmap !== null)
      ? Buffer.fromPartial(object.vnodeBitmap)
      : undefined;
    message.mviewDefinition = object.mviewDefinition ?? "";
    return message;
  },
};

function createBaseStreamEnvironment(): StreamEnvironment {
  return { timezone: "" };
}

export const StreamEnvironment = {
  fromJSON(object: any): StreamEnvironment {
    return { timezone: isSet(object.timezone) ? String(object.timezone) : "" };
  },

  toJSON(message: StreamEnvironment): unknown {
    const obj: any = {};
    message.timezone !== undefined && (obj.timezone = message.timezone);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamEnvironment>, I>>(object: I): StreamEnvironment {
    const message = createBaseStreamEnvironment();
    message.timezone = object.timezone ?? "";
    return message;
  },
};

function createBaseStreamFragmentGraph(): StreamFragmentGraph {
  return { fragments: {}, edges: [], dependentRelationIds: [], tableIdsCnt: 0, env: undefined, parallelism: undefined };
}

export const StreamFragmentGraph = {
  fromJSON(object: any): StreamFragmentGraph {
    return {
      fragments: isObject(object.fragments)
        ? Object.entries(object.fragments).reduce<{ [key: number]: StreamFragmentGraph_StreamFragment }>(
          (acc, [key, value]) => {
            acc[Number(key)] = StreamFragmentGraph_StreamFragment.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
      edges: Array.isArray(object?.edges)
        ? object.edges.map((e: any) => StreamFragmentGraph_StreamFragmentEdge.fromJSON(e))
        : [],
      dependentRelationIds: Array.isArray(object?.dependentRelationIds)
        ? object.dependentRelationIds.map((e: any) => Number(e))
        : [],
      tableIdsCnt: isSet(object.tableIdsCnt) ? Number(object.tableIdsCnt) : 0,
      env: isSet(object.env) ? StreamEnvironment.fromJSON(object.env) : undefined,
      parallelism: isSet(object.parallelism) ? StreamFragmentGraph_Parallelism.fromJSON(object.parallelism) : undefined,
    };
  },

  toJSON(message: StreamFragmentGraph): unknown {
    const obj: any = {};
    obj.fragments = {};
    if (message.fragments) {
      Object.entries(message.fragments).forEach(([k, v]) => {
        obj.fragments[k] = StreamFragmentGraph_StreamFragment.toJSON(v);
      });
    }
    if (message.edges) {
      obj.edges = message.edges.map((e) => e ? StreamFragmentGraph_StreamFragmentEdge.toJSON(e) : undefined);
    } else {
      obj.edges = [];
    }
    if (message.dependentRelationIds) {
      obj.dependentRelationIds = message.dependentRelationIds.map((e) => Math.round(e));
    } else {
      obj.dependentRelationIds = [];
    }
    message.tableIdsCnt !== undefined && (obj.tableIdsCnt = Math.round(message.tableIdsCnt));
    message.env !== undefined && (obj.env = message.env ? StreamEnvironment.toJSON(message.env) : undefined);
    message.parallelism !== undefined &&
      (obj.parallelism = message.parallelism ? StreamFragmentGraph_Parallelism.toJSON(message.parallelism) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamFragmentGraph>, I>>(object: I): StreamFragmentGraph {
    const message = createBaseStreamFragmentGraph();
    message.fragments = Object.entries(object.fragments ?? {}).reduce<
      { [key: number]: StreamFragmentGraph_StreamFragment }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = StreamFragmentGraph_StreamFragment.fromPartial(value);
      }
      return acc;
    }, {});
    message.edges = object.edges?.map((e) => StreamFragmentGraph_StreamFragmentEdge.fromPartial(e)) || [];
    message.dependentRelationIds = object.dependentRelationIds?.map((e) => e) || [];
    message.tableIdsCnt = object.tableIdsCnt ?? 0;
    message.env = (object.env !== undefined && object.env !== null)
      ? StreamEnvironment.fromPartial(object.env)
      : undefined;
    message.parallelism = (object.parallelism !== undefined && object.parallelism !== null)
      ? StreamFragmentGraph_Parallelism.fromPartial(object.parallelism)
      : undefined;
    return message;
  },
};

function createBaseStreamFragmentGraph_StreamFragment(): StreamFragmentGraph_StreamFragment {
  return {
    fragmentId: 0,
    node: undefined,
    fragmentTypeMask: 0,
    isSingleton: false,
    tableIdsCnt: 0,
    upstreamTableIds: [],
  };
}

export const StreamFragmentGraph_StreamFragment = {
  fromJSON(object: any): StreamFragmentGraph_StreamFragment {
    return {
      fragmentId: isSet(object.fragmentId) ? Number(object.fragmentId) : 0,
      node: isSet(object.node) ? StreamNode.fromJSON(object.node) : undefined,
      fragmentTypeMask: isSet(object.fragmentTypeMask) ? Number(object.fragmentTypeMask) : 0,
      isSingleton: isSet(object.isSingleton) ? Boolean(object.isSingleton) : false,
      tableIdsCnt: isSet(object.tableIdsCnt) ? Number(object.tableIdsCnt) : 0,
      upstreamTableIds: Array.isArray(object?.upstreamTableIds)
        ? object.upstreamTableIds.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: StreamFragmentGraph_StreamFragment): unknown {
    const obj: any = {};
    message.fragmentId !== undefined && (obj.fragmentId = Math.round(message.fragmentId));
    message.node !== undefined && (obj.node = message.node ? StreamNode.toJSON(message.node) : undefined);
    message.fragmentTypeMask !== undefined && (obj.fragmentTypeMask = Math.round(message.fragmentTypeMask));
    message.isSingleton !== undefined && (obj.isSingleton = message.isSingleton);
    message.tableIdsCnt !== undefined && (obj.tableIdsCnt = Math.round(message.tableIdsCnt));
    if (message.upstreamTableIds) {
      obj.upstreamTableIds = message.upstreamTableIds.map((e) => Math.round(e));
    } else {
      obj.upstreamTableIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamFragmentGraph_StreamFragment>, I>>(
    object: I,
  ): StreamFragmentGraph_StreamFragment {
    const message = createBaseStreamFragmentGraph_StreamFragment();
    message.fragmentId = object.fragmentId ?? 0;
    message.node = (object.node !== undefined && object.node !== null)
      ? StreamNode.fromPartial(object.node)
      : undefined;
    message.fragmentTypeMask = object.fragmentTypeMask ?? 0;
    message.isSingleton = object.isSingleton ?? false;
    message.tableIdsCnt = object.tableIdsCnt ?? 0;
    message.upstreamTableIds = object.upstreamTableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseStreamFragmentGraph_StreamFragmentEdge(): StreamFragmentGraph_StreamFragmentEdge {
  return { dispatchStrategy: undefined, linkId: 0, upstreamId: 0, downstreamId: 0 };
}

export const StreamFragmentGraph_StreamFragmentEdge = {
  fromJSON(object: any): StreamFragmentGraph_StreamFragmentEdge {
    return {
      dispatchStrategy: isSet(object.dispatchStrategy) ? DispatchStrategy.fromJSON(object.dispatchStrategy) : undefined,
      linkId: isSet(object.linkId) ? Number(object.linkId) : 0,
      upstreamId: isSet(object.upstreamId) ? Number(object.upstreamId) : 0,
      downstreamId: isSet(object.downstreamId) ? Number(object.downstreamId) : 0,
    };
  },

  toJSON(message: StreamFragmentGraph_StreamFragmentEdge): unknown {
    const obj: any = {};
    message.dispatchStrategy !== undefined &&
      (obj.dispatchStrategy = message.dispatchStrategy ? DispatchStrategy.toJSON(message.dispatchStrategy) : undefined);
    message.linkId !== undefined && (obj.linkId = Math.round(message.linkId));
    message.upstreamId !== undefined && (obj.upstreamId = Math.round(message.upstreamId));
    message.downstreamId !== undefined && (obj.downstreamId = Math.round(message.downstreamId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamFragmentGraph_StreamFragmentEdge>, I>>(
    object: I,
  ): StreamFragmentGraph_StreamFragmentEdge {
    const message = createBaseStreamFragmentGraph_StreamFragmentEdge();
    message.dispatchStrategy = (object.dispatchStrategy !== undefined && object.dispatchStrategy !== null)
      ? DispatchStrategy.fromPartial(object.dispatchStrategy)
      : undefined;
    message.linkId = object.linkId ?? 0;
    message.upstreamId = object.upstreamId ?? 0;
    message.downstreamId = object.downstreamId ?? 0;
    return message;
  },
};

function createBaseStreamFragmentGraph_Parallelism(): StreamFragmentGraph_Parallelism {
  return { parallelism: 0 };
}

export const StreamFragmentGraph_Parallelism = {
  fromJSON(object: any): StreamFragmentGraph_Parallelism {
    return { parallelism: isSet(object.parallelism) ? Number(object.parallelism) : 0 };
  },

  toJSON(message: StreamFragmentGraph_Parallelism): unknown {
    const obj: any = {};
    message.parallelism !== undefined && (obj.parallelism = Math.round(message.parallelism));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamFragmentGraph_Parallelism>, I>>(
    object: I,
  ): StreamFragmentGraph_Parallelism {
    const message = createBaseStreamFragmentGraph_Parallelism();
    message.parallelism = object.parallelism ?? 0;
    return message;
  },
};

function createBaseStreamFragmentGraph_FragmentsEntry(): StreamFragmentGraph_FragmentsEntry {
  return { key: 0, value: undefined };
}

export const StreamFragmentGraph_FragmentsEntry = {
  fromJSON(object: any): StreamFragmentGraph_FragmentsEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? StreamFragmentGraph_StreamFragment.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: StreamFragmentGraph_FragmentsEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? StreamFragmentGraph_StreamFragment.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StreamFragmentGraph_FragmentsEntry>, I>>(
    object: I,
  ): StreamFragmentGraph_FragmentsEntry {
    const message = createBaseStreamFragmentGraph_FragmentsEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? StreamFragmentGraph_StreamFragment.fromPartial(object.value)
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
