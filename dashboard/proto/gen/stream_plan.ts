/* eslint-disable */
import { Table } from "./catalog";
import { Buffer } from "./common";
import { Epoch, IntervalUnit, StreamChunk } from "./data";
import { AggCall, ExprNode, InputRefExpr, ProjectSetSelectItem } from "./expr";
import {
  ColumnDesc,
  ColumnOrder,
  Field,
  JoinType,
  joinTypeFromJSON,
  joinTypeToJSON,
  StorageTableDesc,
} from "./plan_common";
import { ConnectorSplits } from "./source";

export const protobufPackage = "stream_plan";

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

export const FragmentType = {
  FRAGMENT_UNSPECIFIED: "FRAGMENT_UNSPECIFIED",
  OTHERS: "OTHERS",
  SOURCE: "SOURCE",
  /** SINK - TODO: change it to MATERIALIZED_VIEW or other name, since we have sink type now. */
  SINK: "SINK",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type FragmentType = typeof FragmentType[keyof typeof FragmentType];

export function fragmentTypeFromJSON(object: any): FragmentType {
  switch (object) {
    case 0:
    case "FRAGMENT_UNSPECIFIED":
      return FragmentType.FRAGMENT_UNSPECIFIED;
    case 1:
    case "OTHERS":
      return FragmentType.OTHERS;
    case 2:
    case "SOURCE":
      return FragmentType.SOURCE;
    case 3:
    case "SINK":
      return FragmentType.SINK;
    case -1:
    case "UNRECOGNIZED":
    default:
      return FragmentType.UNRECOGNIZED;
  }
}

export function fragmentTypeToJSON(object: FragmentType): string {
  switch (object) {
    case FragmentType.FRAGMENT_UNSPECIFIED:
      return "FRAGMENT_UNSPECIFIED";
    case FragmentType.OTHERS:
      return "OTHERS";
    case FragmentType.SOURCE:
      return "SOURCE";
    case FragmentType.SINK:
      return "SINK";
    case FragmentType.UNRECOGNIZED:
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
  /** Dispatcher updates for each upstream actor. */
  actorDispatcherUpdate: { [key: number]: UpdateMutation_DispatcherUpdate };
  /** Merge updates for each downstream actor. */
  actorMergeUpdate: { [key: number]: UpdateMutation_MergeUpdate };
  /** Vnode bitmap updates for each actor. */
  actorVnodeBitmapUpdate: { [key: number]: Buffer };
  /** All actors to be dropped in this update. */
  droppedActors: number[];
}

export interface UpdateMutation_DispatcherUpdate {
  /** Dispatcher can be uniquely identified by a combination of actor id and dispatcher id. */
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

/**
 * TODO: These actor ids should be the same as those in `DispatcherUpdate`.
 * We may find a way to deduplicate this.
 */
export interface UpdateMutation_MergeUpdate {
  /** Added upstream actors. */
  addedUpstreamActorId: number[];
  /** Removed upstream actors. */
  removedUpstreamActorId: number[];
}

export interface UpdateMutation_ActorDispatcherUpdateEntry {
  key: number;
  value: UpdateMutation_DispatcherUpdate | undefined;
}

export interface UpdateMutation_ActorMergeUpdateEntry {
  key: number;
  value: UpdateMutation_MergeUpdate | undefined;
}

export interface UpdateMutation_ActorVnodeBitmapUpdateEntry {
  key: number;
  value: Buffer | undefined;
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

export interface StreamMessage {
  streamMessage?: { $case: "streamChunk"; streamChunk: StreamChunk } | { $case: "barrier"; barrier: Barrier };
}

/** Hash mapping for compute node. Stores mapping from virtual node to actor id. */
export interface ActorMapping {
  originalIndices: number[];
  data: number[];
}

/** todo: StreamSourceNode or TableSourceNode */
export interface SourceNode {
  /** use source_id to fetch SourceDesc from local source manager */
  sourceId: number;
  columnIds: number[];
  sourceType: SourceNode_SourceType;
  /** use state_table_id as state store prefix */
  stateTableId: number;
}

export const SourceNode_SourceType = {
  UNSPECIFIED: "UNSPECIFIED",
  TABLE: "TABLE",
  SOURCE: "SOURCE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type SourceNode_SourceType = typeof SourceNode_SourceType[keyof typeof SourceNode_SourceType];

export function sourceNode_SourceTypeFromJSON(object: any): SourceNode_SourceType {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return SourceNode_SourceType.UNSPECIFIED;
    case 1:
    case "TABLE":
      return SourceNode_SourceType.TABLE;
    case 2:
    case "SOURCE":
      return SourceNode_SourceType.SOURCE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return SourceNode_SourceType.UNRECOGNIZED;
  }
}

export function sourceNode_SourceTypeToJSON(object: SourceNode_SourceType): string {
  switch (object) {
    case SourceNode_SourceType.UNSPECIFIED:
      return "UNSPECIFIED";
    case SourceNode_SourceType.TABLE:
      return "TABLE";
    case SourceNode_SourceType.SOURCE:
      return "SOURCE";
    case SourceNode_SourceType.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface SinkNode {
  tableId: number;
  columnIds: number[];
  properties: { [key: string]: string };
}

export interface SinkNode_PropertiesEntry {
  key: string;
  value: string;
}

export interface ProjectNode {
  selectList: ExprNode[];
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
  /** Column indexes and orders of primary key */
  columnOrders: ColumnOrder[];
  /** Used for internal table states. */
  table: Table | undefined;
}

/**
 * Remark by Yanghao: for both local and global we use the same node in the protobuf.
 * Local and global aggregator distinguish with each other in PlanNode definition.
 */
export interface SimpleAggNode {
  aggCalls: AggCall[];
  /** Only used for local simple agg, not used for global simple agg. */
  distributionKey: number[];
  internalTables: Table[];
  columnMappings: ColumnMapping[];
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
}

export interface ColumnMapping {
  indices: number[];
}

export interface HashAggNode {
  groupKey: number[];
  aggCalls: AggCall[];
  internalTables: Table[];
  columnMappings: ColumnMapping[];
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
}

export interface TopNNode {
  /** 0 means no limit as limit of 0 means this node should be optimized away */
  limit: number;
  offset: number;
  table: Table | undefined;
}

export interface GroupTopNNode {
  /** 0 means no limit as limit of 0 means this node should be optimized away */
  limit: number;
  offset: number;
  groupKey: number[];
  table: Table | undefined;
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
  nullSafe: boolean[];
  /**
   * Whether to optimize for append only stream.
   * It is true when the input is append-only
   */
  isAppendOnly: boolean;
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
  timeCol: InputRefExpr | undefined;
  windowSlide: IntervalUnit | undefined;
  windowSize: IntervalUnit | undefined;
  outputIndices: number[];
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
  /** Which columns from upstream are used in this Chain node. */
  upstreamColumnIndices: number[];
  /**
   * Generally, the barrier needs to be rearranged during the MV creation process, so that data can
   * be flushed to shared buffer periodically, instead of making the first epoch from batch query extra
   * large. However, in some cases, e.g., shared state, the barrier cannot be rearranged in ChainNode.
   * This option is used to disable barrier rearrangement.
   */
  disableRearrange: boolean;
  /** Whether to place this chain on the same worker node as upstream actors. */
  sameWorkerNode: boolean;
  /**
   * Whether the upstream materialize is and this chain should be a singleton.
   * FIXME: This is a workaround for fragmenter since the distribution info will be lost if there's only one
   * fragment in the downstream mview. Remove this when we refactor the fragmenter.
   */
  isSingleton: boolean;
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
  table: Table | undefined;
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
  arrangementTableInfo:
    | ArrangementInfo
    | undefined;
  /** Internal table of arrangement. */
  arrangementTable: Table | undefined;
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
    | { $case: "groupTopN"; groupTopN: GroupTopNNode };
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

export interface DispatchStrategy {
  type: DispatcherType;
  columnIndices: number[];
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
  columnIndices: number[];
  /**
   * The hash mapping for consistent hash.
   * For dispatcher types other than HASH, this is ignored.
   */
  hashMapping:
    | ActorMapping
    | undefined;
  /**
   * Dispatcher can be uniquely identified by a combination of actor id and dispatcher id.
   * - For dispatchers within actors, the id is the same as operator_id of the exchange plan node.
   * - For MV on MV, the id is the same as the actor id of chain node in the downstream MV.
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
  /** Placement rule for actor, need to stay on the same node as upstream. */
  sameWorkerNodeAsUpstream: boolean;
  /**
   * Vnodes that the executors in this actor own. If this actor is the only actor in its fragment, `vnode_bitmap`
   * will be empty.
   */
  vnodeBitmap: Buffer | undefined;
}

export interface StreamFragmentGraph {
  /** all the fragments in the graph. */
  fragments: { [key: number]: StreamFragmentGraph_StreamFragment };
  /** edges between fragments. */
  edges: StreamFragmentGraph_StreamFragmentEdge[];
  dependentTableIds: number[];
  tableIdsCnt: number;
}

export interface StreamFragmentGraph_StreamFragment {
  /** 0-based on frontend, and will be rewritten to global id on meta. */
  fragmentId: number;
  /** root stream node in this fragment. */
  node: StreamNode | undefined;
  fragmentType: FragmentType;
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
  /** Whether the two linked nodes should be placed on the same worker node */
  sameWorkerNode: boolean;
  /**
   * A unique identifier of this edge. Generally it should be exchange node's operator id. When
   * rewriting fragments into delta joins or when inserting 1-to-1 exchange, there will be
   * virtual links generated.
   */
  linkId: number;
  upstreamId: number;
  downstreamId: number;
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
  return { actorDispatcherUpdate: {}, actorMergeUpdate: {}, actorVnodeBitmapUpdate: {}, droppedActors: [] };
}

export const UpdateMutation = {
  fromJSON(object: any): UpdateMutation {
    return {
      actorDispatcherUpdate: isObject(object.actorDispatcherUpdate)
        ? Object.entries(object.actorDispatcherUpdate).reduce<{ [key: number]: UpdateMutation_DispatcherUpdate }>(
          (acc, [key, value]) => {
            acc[Number(key)] = UpdateMutation_DispatcherUpdate.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
      actorMergeUpdate: isObject(object.actorMergeUpdate)
        ? Object.entries(object.actorMergeUpdate).reduce<{ [key: number]: UpdateMutation_MergeUpdate }>(
          (acc, [key, value]) => {
            acc[Number(key)] = UpdateMutation_MergeUpdate.fromJSON(value);
            return acc;
          },
          {},
        )
        : {},
      actorVnodeBitmapUpdate: isObject(object.actorVnodeBitmapUpdate)
        ? Object.entries(object.actorVnodeBitmapUpdate).reduce<{ [key: number]: Buffer }>((acc, [key, value]) => {
          acc[Number(key)] = Buffer.fromJSON(value);
          return acc;
        }, {})
        : {},
      droppedActors: Array.isArray(object?.droppedActors)
        ? object.droppedActors.map((e: any) => Number(e))
        : [],
    };
  },

  toJSON(message: UpdateMutation): unknown {
    const obj: any = {};
    obj.actorDispatcherUpdate = {};
    if (message.actorDispatcherUpdate) {
      Object.entries(message.actorDispatcherUpdate).forEach(([k, v]) => {
        obj.actorDispatcherUpdate[k] = UpdateMutation_DispatcherUpdate.toJSON(v);
      });
    }
    obj.actorMergeUpdate = {};
    if (message.actorMergeUpdate) {
      Object.entries(message.actorMergeUpdate).forEach(([k, v]) => {
        obj.actorMergeUpdate[k] = UpdateMutation_MergeUpdate.toJSON(v);
      });
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
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation>, I>>(object: I): UpdateMutation {
    const message = createBaseUpdateMutation();
    message.actorDispatcherUpdate = Object.entries(object.actorDispatcherUpdate ?? {}).reduce<
      { [key: number]: UpdateMutation_DispatcherUpdate }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = UpdateMutation_DispatcherUpdate.fromPartial(value);
      }
      return acc;
    }, {});
    message.actorMergeUpdate = Object.entries(object.actorMergeUpdate ?? {}).reduce<
      { [key: number]: UpdateMutation_MergeUpdate }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = UpdateMutation_MergeUpdate.fromPartial(value);
      }
      return acc;
    }, {});
    message.actorVnodeBitmapUpdate = Object.entries(object.actorVnodeBitmapUpdate ?? {}).reduce<
      { [key: number]: Buffer }
    >((acc, [key, value]) => {
      if (value !== undefined) {
        acc[Number(key)] = Buffer.fromPartial(value);
      }
      return acc;
    }, {});
    message.droppedActors = object.droppedActors?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateMutation_DispatcherUpdate(): UpdateMutation_DispatcherUpdate {
  return { dispatcherId: 0, hashMapping: undefined, addedDownstreamActorId: [], removedDownstreamActorId: [] };
}

export const UpdateMutation_DispatcherUpdate = {
  fromJSON(object: any): UpdateMutation_DispatcherUpdate {
    return {
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
  return { addedUpstreamActorId: [], removedUpstreamActorId: [] };
}

export const UpdateMutation_MergeUpdate = {
  fromJSON(object: any): UpdateMutation_MergeUpdate {
    return {
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
    message.addedUpstreamActorId = object.addedUpstreamActorId?.map((e) => e) || [];
    message.removedUpstreamActorId = object.removedUpstreamActorId?.map((e) => e) || [];
    return message;
  },
};

function createBaseUpdateMutation_ActorDispatcherUpdateEntry(): UpdateMutation_ActorDispatcherUpdateEntry {
  return { key: 0, value: undefined };
}

export const UpdateMutation_ActorDispatcherUpdateEntry = {
  fromJSON(object: any): UpdateMutation_ActorDispatcherUpdateEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? UpdateMutation_DispatcherUpdate.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: UpdateMutation_ActorDispatcherUpdateEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? UpdateMutation_DispatcherUpdate.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_ActorDispatcherUpdateEntry>, I>>(
    object: I,
  ): UpdateMutation_ActorDispatcherUpdateEntry {
    const message = createBaseUpdateMutation_ActorDispatcherUpdateEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? UpdateMutation_DispatcherUpdate.fromPartial(object.value)
      : undefined;
    return message;
  },
};

function createBaseUpdateMutation_ActorMergeUpdateEntry(): UpdateMutation_ActorMergeUpdateEntry {
  return { key: 0, value: undefined };
}

export const UpdateMutation_ActorMergeUpdateEntry = {
  fromJSON(object: any): UpdateMutation_ActorMergeUpdateEntry {
    return {
      key: isSet(object.key) ? Number(object.key) : 0,
      value: isSet(object.value) ? UpdateMutation_MergeUpdate.fromJSON(object.value) : undefined,
    };
  },

  toJSON(message: UpdateMutation_ActorMergeUpdateEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = Math.round(message.key));
    message.value !== undefined &&
      (obj.value = message.value ? UpdateMutation_MergeUpdate.toJSON(message.value) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UpdateMutation_ActorMergeUpdateEntry>, I>>(
    object: I,
  ): UpdateMutation_ActorMergeUpdateEntry {
    const message = createBaseUpdateMutation_ActorMergeUpdateEntry();
    message.key = object.key ?? 0;
    message.value = (object.value !== undefined && object.value !== null)
      ? UpdateMutation_MergeUpdate.fromPartial(object.value)
      : undefined;
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

function createBaseSourceNode(): SourceNode {
  return { sourceId: 0, columnIds: [], sourceType: SourceNode_SourceType.UNSPECIFIED, stateTableId: 0 };
}

export const SourceNode = {
  fromJSON(object: any): SourceNode {
    return {
      sourceId: isSet(object.sourceId) ? Number(object.sourceId) : 0,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
      sourceType: isSet(object.sourceType)
        ? sourceNode_SourceTypeFromJSON(object.sourceType)
        : SourceNode_SourceType.UNSPECIFIED,
      stateTableId: isSet(object.stateTableId) ? Number(object.stateTableId) : 0,
    };
  },

  toJSON(message: SourceNode): unknown {
    const obj: any = {};
    message.sourceId !== undefined && (obj.sourceId = Math.round(message.sourceId));
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    message.sourceType !== undefined && (obj.sourceType = sourceNode_SourceTypeToJSON(message.sourceType));
    message.stateTableId !== undefined && (obj.stateTableId = Math.round(message.stateTableId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SourceNode>, I>>(object: I): SourceNode {
    const message = createBaseSourceNode();
    message.sourceId = object.sourceId ?? 0;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    message.sourceType = object.sourceType ?? SourceNode_SourceType.UNSPECIFIED;
    message.stateTableId = object.stateTableId ?? 0;
    return message;
  },
};

function createBaseSinkNode(): SinkNode {
  return { tableId: 0, columnIds: [], properties: {} };
}

export const SinkNode = {
  fromJSON(object: any): SinkNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      columnIds: Array.isArray(object?.columnIds) ? object.columnIds.map((e: any) => Number(e)) : [],
      properties: isObject(object.properties)
        ? Object.entries(object.properties).reduce<{ [key: string]: string }>((acc, [key, value]) => {
          acc[key] = String(value);
          return acc;
        }, {})
        : {},
    };
  },

  toJSON(message: SinkNode): unknown {
    const obj: any = {};
    message.tableId !== undefined && (obj.tableId = Math.round(message.tableId));
    if (message.columnIds) {
      obj.columnIds = message.columnIds.map((e) => Math.round(e));
    } else {
      obj.columnIds = [];
    }
    obj.properties = {};
    if (message.properties) {
      Object.entries(message.properties).forEach(([k, v]) => {
        obj.properties[k] = v;
      });
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkNode>, I>>(object: I): SinkNode {
    const message = createBaseSinkNode();
    message.tableId = object.tableId ?? 0;
    message.columnIds = object.columnIds?.map((e) => e) || [];
    message.properties = Object.entries(object.properties ?? {}).reduce<{ [key: string]: string }>(
      (acc, [key, value]) => {
        if (value !== undefined) {
          acc[key] = String(value);
        }
        return acc;
      },
      {},
    );
    return message;
  },
};

function createBaseSinkNode_PropertiesEntry(): SinkNode_PropertiesEntry {
  return { key: "", value: "" };
}

export const SinkNode_PropertiesEntry = {
  fromJSON(object: any): SinkNode_PropertiesEntry {
    return { key: isSet(object.key) ? String(object.key) : "", value: isSet(object.value) ? String(object.value) : "" };
  },

  toJSON(message: SinkNode_PropertiesEntry): unknown {
    const obj: any = {};
    message.key !== undefined && (obj.key = message.key);
    message.value !== undefined && (obj.value = message.value);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SinkNode_PropertiesEntry>, I>>(object: I): SinkNode_PropertiesEntry {
    const message = createBaseSinkNode_PropertiesEntry();
    message.key = object.key ?? "";
    message.value = object.value ?? "";
    return message;
  },
};

function createBaseProjectNode(): ProjectNode {
  return { selectList: [] };
}

export const ProjectNode = {
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
  return { tableId: 0, columnOrders: [], table: undefined };
}

export const MaterializeNode = {
  fromJSON(object: any): MaterializeNode {
    return {
      tableId: isSet(object.tableId) ? Number(object.tableId) : 0,
      columnOrders: Array.isArray(object?.columnOrders)
        ? object.columnOrders.map((e: any) => ColumnOrder.fromJSON(e))
        : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
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
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MaterializeNode>, I>>(object: I): MaterializeNode {
    const message = createBaseMaterializeNode();
    message.tableId = object.tableId ?? 0;
    message.columnOrders = object.columnOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    return message;
  },
};

function createBaseSimpleAggNode(): SimpleAggNode {
  return { aggCalls: [], distributionKey: [], internalTables: [], columnMappings: [], isAppendOnly: false };
}

export const SimpleAggNode = {
  fromJSON(object: any): SimpleAggNode {
    return {
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      internalTables: Array.isArray(object?.internalTables)
        ? object.internalTables.map((e: any) => Table.fromJSON(e))
        : [],
      columnMappings: Array.isArray(object?.columnMappings)
        ? object.columnMappings.map((e: any) => ColumnMapping.fromJSON(e))
        : [],
      isAppendOnly: isSet(object.isAppendOnly) ? Boolean(object.isAppendOnly) : false,
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
    if (message.internalTables) {
      obj.internalTables = message.internalTables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.internalTables = [];
    }
    if (message.columnMappings) {
      obj.columnMappings = message.columnMappings.map((e) => e ? ColumnMapping.toJSON(e) : undefined);
    } else {
      obj.columnMappings = [];
    }
    message.isAppendOnly !== undefined && (obj.isAppendOnly = message.isAppendOnly);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<SimpleAggNode>, I>>(object: I): SimpleAggNode {
    const message = createBaseSimpleAggNode();
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.internalTables = object.internalTables?.map((e) => Table.fromPartial(e)) || [];
    message.columnMappings = object.columnMappings?.map((e) => ColumnMapping.fromPartial(e)) || [];
    message.isAppendOnly = object.isAppendOnly ?? false;
    return message;
  },
};

function createBaseColumnMapping(): ColumnMapping {
  return { indices: [] };
}

export const ColumnMapping = {
  fromJSON(object: any): ColumnMapping {
    return { indices: Array.isArray(object?.indices) ? object.indices.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: ColumnMapping): unknown {
    const obj: any = {};
    if (message.indices) {
      obj.indices = message.indices.map((e) => Math.round(e));
    } else {
      obj.indices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ColumnMapping>, I>>(object: I): ColumnMapping {
    const message = createBaseColumnMapping();
    message.indices = object.indices?.map((e) => e) || [];
    return message;
  },
};

function createBaseHashAggNode(): HashAggNode {
  return { groupKey: [], aggCalls: [], internalTables: [], columnMappings: [], isAppendOnly: false };
}

export const HashAggNode = {
  fromJSON(object: any): HashAggNode {
    return {
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => Number(e)) : [],
      aggCalls: Array.isArray(object?.aggCalls) ? object.aggCalls.map((e: any) => AggCall.fromJSON(e)) : [],
      internalTables: Array.isArray(object?.internalTables)
        ? object.internalTables.map((e: any) => Table.fromJSON(e))
        : [],
      columnMappings: Array.isArray(object?.columnMappings)
        ? object.columnMappings.map((e: any) => ColumnMapping.fromJSON(e))
        : [],
      isAppendOnly: isSet(object.isAppendOnly) ? Boolean(object.isAppendOnly) : false,
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
    if (message.internalTables) {
      obj.internalTables = message.internalTables.map((e) => e ? Table.toJSON(e) : undefined);
    } else {
      obj.internalTables = [];
    }
    if (message.columnMappings) {
      obj.columnMappings = message.columnMappings.map((e) => e ? ColumnMapping.toJSON(e) : undefined);
    } else {
      obj.columnMappings = [];
    }
    message.isAppendOnly !== undefined && (obj.isAppendOnly = message.isAppendOnly);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<HashAggNode>, I>>(object: I): HashAggNode {
    const message = createBaseHashAggNode();
    message.groupKey = object.groupKey?.map((e) => e) || [];
    message.aggCalls = object.aggCalls?.map((e) => AggCall.fromPartial(e)) || [];
    message.internalTables = object.internalTables?.map((e) => Table.fromPartial(e)) || [];
    message.columnMappings = object.columnMappings?.map((e) => ColumnMapping.fromPartial(e)) || [];
    message.isAppendOnly = object.isAppendOnly ?? false;
    return message;
  },
};

function createBaseTopNNode(): TopNNode {
  return { limit: 0, offset: 0, table: undefined };
}

export const TopNNode = {
  fromJSON(object: any): TopNNode {
    return {
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
    };
  },

  toJSON(message: TopNNode): unknown {
    const obj: any = {};
    message.limit !== undefined && (obj.limit = Math.round(message.limit));
    message.offset !== undefined && (obj.offset = Math.round(message.offset));
    message.table !== undefined && (obj.table = message.table ? Table.toJSON(message.table) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TopNNode>, I>>(object: I): TopNNode {
    const message = createBaseTopNNode();
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
    return message;
  },
};

function createBaseGroupTopNNode(): GroupTopNNode {
  return { limit: 0, offset: 0, groupKey: [], table: undefined };
}

export const GroupTopNNode = {
  fromJSON(object: any): GroupTopNNode {
    return {
      limit: isSet(object.limit) ? Number(object.limit) : 0,
      offset: isSet(object.offset) ? Number(object.offset) : 0,
      groupKey: Array.isArray(object?.groupKey) ? object.groupKey.map((e: any) => Number(e)) : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
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
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GroupTopNNode>, I>>(object: I): GroupTopNNode {
    const message = createBaseGroupTopNNode();
    message.limit = object.limit ?? 0;
    message.offset = object.offset ?? 0;
    message.groupKey = object.groupKey?.map((e) => e) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
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
    message.nullSafe = object.nullSafe?.map((e) => e) || [];
    message.isAppendOnly = object.isAppendOnly ?? false;
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
  return { timeCol: undefined, windowSlide: undefined, windowSize: undefined, outputIndices: [] };
}

export const HopWindowNode = {
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
    disableRearrange: false,
    sameWorkerNode: false,
    isSingleton: false,
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
      disableRearrange: isSet(object.disableRearrange) ? Boolean(object.disableRearrange) : false,
      sameWorkerNode: isSet(object.sameWorkerNode) ? Boolean(object.sameWorkerNode) : false,
      isSingleton: isSet(object.isSingleton) ? Boolean(object.isSingleton) : false,
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
    message.disableRearrange !== undefined && (obj.disableRearrange = message.disableRearrange);
    message.sameWorkerNode !== undefined && (obj.sameWorkerNode = message.sameWorkerNode);
    message.isSingleton !== undefined && (obj.isSingleton = message.isSingleton);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ChainNode>, I>>(object: I): ChainNode {
    const message = createBaseChainNode();
    message.tableId = object.tableId ?? 0;
    message.upstreamFields = object.upstreamFields?.map((e) => Field.fromPartial(e)) || [];
    message.upstreamColumnIndices = object.upstreamColumnIndices?.map((e) => e) || [];
    message.disableRearrange = object.disableRearrange ?? false;
    message.sameWorkerNode = object.sameWorkerNode ?? false;
    message.isSingleton = object.isSingleton ?? false;
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
  return { arrangeKeyOrders: [], columnDescs: [] };
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
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ArrangementInfo>, I>>(object: I): ArrangementInfo {
    const message = createBaseArrangementInfo();
    message.arrangeKeyOrders = object.arrangeKeyOrders?.map((e) => ColumnOrder.fromPartial(e)) || [];
    message.columnDescs = object.columnDescs?.map((e) => ColumnDesc.fromPartial(e)) || [];
    return message;
  },
};

function createBaseArrangeNode(): ArrangeNode {
  return { tableInfo: undefined, distributionKey: [], table: undefined };
}

export const ArrangeNode = {
  fromJSON(object: any): ArrangeNode {
    return {
      tableInfo: isSet(object.tableInfo) ? ArrangementInfo.fromJSON(object.tableInfo) : undefined,
      distributionKey: Array.isArray(object?.distributionKey) ? object.distributionKey.map((e: any) => Number(e)) : [],
      table: isSet(object.table) ? Table.fromJSON(object.table) : undefined,
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
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ArrangeNode>, I>>(object: I): ArrangeNode {
    const message = createBaseArrangeNode();
    message.tableInfo = (object.tableInfo !== undefined && object.tableInfo !== null)
      ? ArrangementInfo.fromPartial(object.tableInfo)
      : undefined;
    message.distributionKey = object.distributionKey?.map((e) => e) || [];
    message.table = (object.table !== undefined && object.table !== null) ? Table.fromPartial(object.table) : undefined;
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
    arrangementTable: undefined,
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
      arrangementTable: isSet(object.arrangementTable) ? Table.fromJSON(object.arrangementTable) : undefined,
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
    message.arrangementTable !== undefined &&
      (obj.arrangementTable = message.arrangementTable ? Table.toJSON(message.arrangementTable) : undefined);
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
    message.arrangementTable = (object.arrangementTable !== undefined && object.arrangementTable !== null)
      ? Table.fromPartial(object.arrangementTable)
      : undefined;
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
    message.operatorId !== undefined && (obj.operatorId = Math.round(message.operatorId));
    if (message.input) {
      obj.input = message.input.map((e) =>
        e ? StreamNode.toJSON(e) : undefined
      );
    } else {
      obj.input = [];
    }
    if (message.streamKey) {
      obj.streamKey = message.streamKey.map((e) => Math.round(e));
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
  return { type: DispatcherType.UNSPECIFIED, columnIndices: [] };
}

export const DispatchStrategy = {
  fromJSON(object: any): DispatchStrategy {
    return {
      type: isSet(object.type) ? dispatcherTypeFromJSON(object.type) : DispatcherType.UNSPECIFIED,
      columnIndices: Array.isArray(object?.columnIndices) ? object.columnIndices.map((e: any) => Number(e)) : [],
    };
  },

  toJSON(message: DispatchStrategy): unknown {
    const obj: any = {};
    message.type !== undefined && (obj.type = dispatcherTypeToJSON(message.type));
    if (message.columnIndices) {
      obj.columnIndices = message.columnIndices.map((e) => Math.round(e));
    } else {
      obj.columnIndices = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DispatchStrategy>, I>>(object: I): DispatchStrategy {
    const message = createBaseDispatchStrategy();
    message.type = object.type ?? DispatcherType.UNSPECIFIED;
    message.columnIndices = object.columnIndices?.map((e) => e) || [];
    return message;
  },
};

function createBaseDispatcher(): Dispatcher {
  return {
    type: DispatcherType.UNSPECIFIED,
    columnIndices: [],
    hashMapping: undefined,
    dispatcherId: 0,
    downstreamActorId: [],
  };
}

export const Dispatcher = {
  fromJSON(object: any): Dispatcher {
    return {
      type: isSet(object.type) ? dispatcherTypeFromJSON(object.type) : DispatcherType.UNSPECIFIED,
      columnIndices: Array.isArray(object?.columnIndices) ? object.columnIndices.map((e: any) => Number(e)) : [],
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
    if (message.columnIndices) {
      obj.columnIndices = message.columnIndices.map((e) => Math.round(e));
    } else {
      obj.columnIndices = [];
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
    message.columnIndices = object.columnIndices?.map((e) => e) || [];
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
    sameWorkerNodeAsUpstream: false,
    vnodeBitmap: undefined,
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
      sameWorkerNodeAsUpstream: isSet(object.sameWorkerNodeAsUpstream)
        ? Boolean(object.sameWorkerNodeAsUpstream)
        : false,
      vnodeBitmap: isSet(object.vnodeBitmap) ? Buffer.fromJSON(object.vnodeBitmap) : undefined,
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
    message.sameWorkerNodeAsUpstream !== undefined && (obj.sameWorkerNodeAsUpstream = message.sameWorkerNodeAsUpstream);
    message.vnodeBitmap !== undefined &&
      (obj.vnodeBitmap = message.vnodeBitmap ? Buffer.toJSON(message.vnodeBitmap) : undefined);
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
    message.sameWorkerNodeAsUpstream = object.sameWorkerNodeAsUpstream ?? false;
    message.vnodeBitmap = (object.vnodeBitmap !== undefined && object.vnodeBitmap !== null)
      ? Buffer.fromPartial(object.vnodeBitmap)
      : undefined;
    return message;
  },
};

function createBaseStreamFragmentGraph(): StreamFragmentGraph {
  return { fragments: {}, edges: [], dependentTableIds: [], tableIdsCnt: 0 };
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
      dependentTableIds: Array.isArray(object?.dependentTableIds)
        ? object.dependentTableIds.map((e: any) => Number(e))
        : [],
      tableIdsCnt: isSet(object.tableIdsCnt) ? Number(object.tableIdsCnt) : 0,
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
    if (message.dependentTableIds) {
      obj.dependentTableIds = message.dependentTableIds.map((e) => Math.round(e));
    } else {
      obj.dependentTableIds = [];
    }
    message.tableIdsCnt !== undefined && (obj.tableIdsCnt = Math.round(message.tableIdsCnt));
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
    message.dependentTableIds = object.dependentTableIds?.map((e) => e) || [];
    message.tableIdsCnt = object.tableIdsCnt ?? 0;
    return message;
  },
};

function createBaseStreamFragmentGraph_StreamFragment(): StreamFragmentGraph_StreamFragment {
  return {
    fragmentId: 0,
    node: undefined,
    fragmentType: FragmentType.FRAGMENT_UNSPECIFIED,
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
      fragmentType: isSet(object.fragmentType)
        ? fragmentTypeFromJSON(object.fragmentType)
        : FragmentType.FRAGMENT_UNSPECIFIED,
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
    message.fragmentType !== undefined && (obj.fragmentType = fragmentTypeToJSON(message.fragmentType));
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
    message.fragmentType = object.fragmentType ?? FragmentType.FRAGMENT_UNSPECIFIED;
    message.isSingleton = object.isSingleton ?? false;
    message.tableIdsCnt = object.tableIdsCnt ?? 0;
    message.upstreamTableIds = object.upstreamTableIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseStreamFragmentGraph_StreamFragmentEdge(): StreamFragmentGraph_StreamFragmentEdge {
  return { dispatchStrategy: undefined, sameWorkerNode: false, linkId: 0, upstreamId: 0, downstreamId: 0 };
}

export const StreamFragmentGraph_StreamFragmentEdge = {
  fromJSON(object: any): StreamFragmentGraph_StreamFragmentEdge {
    return {
      dispatchStrategy: isSet(object.dispatchStrategy) ? DispatchStrategy.fromJSON(object.dispatchStrategy) : undefined,
      sameWorkerNode: isSet(object.sameWorkerNode) ? Boolean(object.sameWorkerNode) : false,
      linkId: isSet(object.linkId) ? Number(object.linkId) : 0,
      upstreamId: isSet(object.upstreamId) ? Number(object.upstreamId) : 0,
      downstreamId: isSet(object.downstreamId) ? Number(object.downstreamId) : 0,
    };
  },

  toJSON(message: StreamFragmentGraph_StreamFragmentEdge): unknown {
    const obj: any = {};
    message.dispatchStrategy !== undefined &&
      (obj.dispatchStrategy = message.dispatchStrategy ? DispatchStrategy.toJSON(message.dispatchStrategy) : undefined);
    message.sameWorkerNode !== undefined && (obj.sameWorkerNode = message.sameWorkerNode);
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
    message.sameWorkerNode = object.sameWorkerNode ?? false;
    message.linkId = object.linkId ?? 0;
    message.upstreamId = object.upstreamId ?? 0;
    message.downstreamId = object.downstreamId ?? 0;
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
