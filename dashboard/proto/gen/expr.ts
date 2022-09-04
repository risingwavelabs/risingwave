/* eslint-disable */
import * as _m0 from "protobufjs/minimal";
import { DataType } from "./data";
import { OrderType, orderTypeFromJSON, orderTypeToJSON } from "./plan_common";

export const protobufPackage = "expr";

export interface ExprNode {
  exprType: ExprNode_Type;
  returnType: DataType | undefined;
  rexNode?: { $case: "inputRef"; inputRef: InputRefExpr } | { $case: "constant"; constant: ConstantValue } | {
    $case: "funcCall";
    funcCall: FunctionCall;
  };
}

export enum ExprNode_Type {
  UNSPECIFIED = 0,
  INPUT_REF = 1,
  CONSTANT_VALUE = 2,
  /** ADD - arithmetics operators */
  ADD = 3,
  SUBTRACT = 4,
  MULTIPLY = 5,
  DIVIDE = 6,
  MODULUS = 7,
  /** EQUAL - comparison operators */
  EQUAL = 8,
  NOT_EQUAL = 9,
  LESS_THAN = 10,
  LESS_THAN_OR_EQUAL = 11,
  GREATER_THAN = 12,
  GREATER_THAN_OR_EQUAL = 13,
  /** AND - logical operators */
  AND = 21,
  OR = 22,
  NOT = 23,
  IN = 24,
  /** BITWISE_AND - bitwise operators */
  BITWISE_AND = 31,
  BITWISE_OR = 32,
  BITWISE_XOR = 33,
  BITWISE_NOT = 34,
  BITWISE_SHIFT_LEFT = 35,
  BITWISE_SHIFT_RIGHT = 36,
  /** EXTRACT - date functions */
  EXTRACT = 101,
  TUMBLE_START = 103,
  /** CAST - other functions */
  CAST = 201,
  SUBSTR = 202,
  LENGTH = 203,
  LIKE = 204,
  UPPER = 205,
  LOWER = 206,
  TRIM = 207,
  REPLACE = 208,
  POSITION = 209,
  LTRIM = 210,
  RTRIM = 211,
  CASE = 212,
  /** ROUND_DIGIT - ROUND(numeric, integer) -> numeric */
  ROUND_DIGIT = 213,
  /**
   * ROUND - ROUND(numeric) -> numeric
   * ROUND(double precision) -> double precision
   */
  ROUND = 214,
  ASCII = 215,
  TRANSLATE = 216,
  COALESCE = 217,
  CONCAT_WS = 218,
  ABS = 219,
  SPLIT_PART = 220,
  CEIL = 221,
  FLOOR = 222,
  TO_CHAR = 223,
  MD5 = 224,
  CHAR_LENGTH = 225,
  REPEAT = 226,
  CONCAT_OP = 227,
  /** BOOL_OUT - BOOL_OUT is different from CAST-bool-to-varchar in PostgreSQL. */
  BOOL_OUT = 228,
  OCTET_LENGTH = 229,
  BIT_LENGTH = 230,
  OVERLAY = 231,
  REGEXP_MATCH = 232,
  /** IS_TRUE - Boolean comparison */
  IS_TRUE = 301,
  IS_NOT_TRUE = 302,
  IS_FALSE = 303,
  IS_NOT_FALSE = 304,
  IS_NULL = 305,
  IS_NOT_NULL = 306,
  IS_DISTINCT_FROM = 307,
  IS_NOT_DISTINCT_FROM = 308,
  /** NEG - Unary operators */
  NEG = 401,
  /** FIELD - Nested selection operators */
  FIELD = 501,
  /** ARRAY - Array expression. */
  ARRAY = 521,
  ARRAY_ACCESS = 522,
  ROW = 523,
  /** SEARCH - Search operator and Search ARGument */
  SEARCH = 998,
  SARG = 999,
  /** VNODE - Internal functions */
  VNODE = 1101,
  UNRECOGNIZED = -1,
}

export function exprNode_TypeFromJSON(object: any): ExprNode_Type {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return ExprNode_Type.UNSPECIFIED;
    case 1:
    case "INPUT_REF":
      return ExprNode_Type.INPUT_REF;
    case 2:
    case "CONSTANT_VALUE":
      return ExprNode_Type.CONSTANT_VALUE;
    case 3:
    case "ADD":
      return ExprNode_Type.ADD;
    case 4:
    case "SUBTRACT":
      return ExprNode_Type.SUBTRACT;
    case 5:
    case "MULTIPLY":
      return ExprNode_Type.MULTIPLY;
    case 6:
    case "DIVIDE":
      return ExprNode_Type.DIVIDE;
    case 7:
    case "MODULUS":
      return ExprNode_Type.MODULUS;
    case 8:
    case "EQUAL":
      return ExprNode_Type.EQUAL;
    case 9:
    case "NOT_EQUAL":
      return ExprNode_Type.NOT_EQUAL;
    case 10:
    case "LESS_THAN":
      return ExprNode_Type.LESS_THAN;
    case 11:
    case "LESS_THAN_OR_EQUAL":
      return ExprNode_Type.LESS_THAN_OR_EQUAL;
    case 12:
    case "GREATER_THAN":
      return ExprNode_Type.GREATER_THAN;
    case 13:
    case "GREATER_THAN_OR_EQUAL":
      return ExprNode_Type.GREATER_THAN_OR_EQUAL;
    case 21:
    case "AND":
      return ExprNode_Type.AND;
    case 22:
    case "OR":
      return ExprNode_Type.OR;
    case 23:
    case "NOT":
      return ExprNode_Type.NOT;
    case 24:
    case "IN":
      return ExprNode_Type.IN;
    case 31:
    case "BITWISE_AND":
      return ExprNode_Type.BITWISE_AND;
    case 32:
    case "BITWISE_OR":
      return ExprNode_Type.BITWISE_OR;
    case 33:
    case "BITWISE_XOR":
      return ExprNode_Type.BITWISE_XOR;
    case 34:
    case "BITWISE_NOT":
      return ExprNode_Type.BITWISE_NOT;
    case 35:
    case "BITWISE_SHIFT_LEFT":
      return ExprNode_Type.BITWISE_SHIFT_LEFT;
    case 36:
    case "BITWISE_SHIFT_RIGHT":
      return ExprNode_Type.BITWISE_SHIFT_RIGHT;
    case 101:
    case "EXTRACT":
      return ExprNode_Type.EXTRACT;
    case 103:
    case "TUMBLE_START":
      return ExprNode_Type.TUMBLE_START;
    case 201:
    case "CAST":
      return ExprNode_Type.CAST;
    case 202:
    case "SUBSTR":
      return ExprNode_Type.SUBSTR;
    case 203:
    case "LENGTH":
      return ExprNode_Type.LENGTH;
    case 204:
    case "LIKE":
      return ExprNode_Type.LIKE;
    case 205:
    case "UPPER":
      return ExprNode_Type.UPPER;
    case 206:
    case "LOWER":
      return ExprNode_Type.LOWER;
    case 207:
    case "TRIM":
      return ExprNode_Type.TRIM;
    case 208:
    case "REPLACE":
      return ExprNode_Type.REPLACE;
    case 209:
    case "POSITION":
      return ExprNode_Type.POSITION;
    case 210:
    case "LTRIM":
      return ExprNode_Type.LTRIM;
    case 211:
    case "RTRIM":
      return ExprNode_Type.RTRIM;
    case 212:
    case "CASE":
      return ExprNode_Type.CASE;
    case 213:
    case "ROUND_DIGIT":
      return ExprNode_Type.ROUND_DIGIT;
    case 214:
    case "ROUND":
      return ExprNode_Type.ROUND;
    case 215:
    case "ASCII":
      return ExprNode_Type.ASCII;
    case 216:
    case "TRANSLATE":
      return ExprNode_Type.TRANSLATE;
    case 217:
    case "COALESCE":
      return ExprNode_Type.COALESCE;
    case 218:
    case "CONCAT_WS":
      return ExprNode_Type.CONCAT_WS;
    case 219:
    case "ABS":
      return ExprNode_Type.ABS;
    case 220:
    case "SPLIT_PART":
      return ExprNode_Type.SPLIT_PART;
    case 221:
    case "CEIL":
      return ExprNode_Type.CEIL;
    case 222:
    case "FLOOR":
      return ExprNode_Type.FLOOR;
    case 223:
    case "TO_CHAR":
      return ExprNode_Type.TO_CHAR;
    case 224:
    case "MD5":
      return ExprNode_Type.MD5;
    case 225:
    case "CHAR_LENGTH":
      return ExprNode_Type.CHAR_LENGTH;
    case 226:
    case "REPEAT":
      return ExprNode_Type.REPEAT;
    case 227:
    case "CONCAT_OP":
      return ExprNode_Type.CONCAT_OP;
    case 228:
    case "BOOL_OUT":
      return ExprNode_Type.BOOL_OUT;
    case 229:
    case "OCTET_LENGTH":
      return ExprNode_Type.OCTET_LENGTH;
    case 230:
    case "BIT_LENGTH":
      return ExprNode_Type.BIT_LENGTH;
    case 231:
    case "OVERLAY":
      return ExprNode_Type.OVERLAY;
    case 232:
    case "REGEXP_MATCH":
      return ExprNode_Type.REGEXP_MATCH;
    case 301:
    case "IS_TRUE":
      return ExprNode_Type.IS_TRUE;
    case 302:
    case "IS_NOT_TRUE":
      return ExprNode_Type.IS_NOT_TRUE;
    case 303:
    case "IS_FALSE":
      return ExprNode_Type.IS_FALSE;
    case 304:
    case "IS_NOT_FALSE":
      return ExprNode_Type.IS_NOT_FALSE;
    case 305:
    case "IS_NULL":
      return ExprNode_Type.IS_NULL;
    case 306:
    case "IS_NOT_NULL":
      return ExprNode_Type.IS_NOT_NULL;
    case 307:
    case "IS_DISTINCT_FROM":
      return ExprNode_Type.IS_DISTINCT_FROM;
    case 308:
    case "IS_NOT_DISTINCT_FROM":
      return ExprNode_Type.IS_NOT_DISTINCT_FROM;
    case 401:
    case "NEG":
      return ExprNode_Type.NEG;
    case 501:
    case "FIELD":
      return ExprNode_Type.FIELD;
    case 521:
    case "ARRAY":
      return ExprNode_Type.ARRAY;
    case 522:
    case "ARRAY_ACCESS":
      return ExprNode_Type.ARRAY_ACCESS;
    case 523:
    case "ROW":
      return ExprNode_Type.ROW;
    case 998:
    case "SEARCH":
      return ExprNode_Type.SEARCH;
    case 999:
    case "SARG":
      return ExprNode_Type.SARG;
    case 1101:
    case "VNODE":
      return ExprNode_Type.VNODE;
    case -1:
    case "UNRECOGNIZED":
    default:
      return ExprNode_Type.UNRECOGNIZED;
  }
}

export function exprNode_TypeToJSON(object: ExprNode_Type): string {
  switch (object) {
    case ExprNode_Type.UNSPECIFIED:
      return "UNSPECIFIED";
    case ExprNode_Type.INPUT_REF:
      return "INPUT_REF";
    case ExprNode_Type.CONSTANT_VALUE:
      return "CONSTANT_VALUE";
    case ExprNode_Type.ADD:
      return "ADD";
    case ExprNode_Type.SUBTRACT:
      return "SUBTRACT";
    case ExprNode_Type.MULTIPLY:
      return "MULTIPLY";
    case ExprNode_Type.DIVIDE:
      return "DIVIDE";
    case ExprNode_Type.MODULUS:
      return "MODULUS";
    case ExprNode_Type.EQUAL:
      return "EQUAL";
    case ExprNode_Type.NOT_EQUAL:
      return "NOT_EQUAL";
    case ExprNode_Type.LESS_THAN:
      return "LESS_THAN";
    case ExprNode_Type.LESS_THAN_OR_EQUAL:
      return "LESS_THAN_OR_EQUAL";
    case ExprNode_Type.GREATER_THAN:
      return "GREATER_THAN";
    case ExprNode_Type.GREATER_THAN_OR_EQUAL:
      return "GREATER_THAN_OR_EQUAL";
    case ExprNode_Type.AND:
      return "AND";
    case ExprNode_Type.OR:
      return "OR";
    case ExprNode_Type.NOT:
      return "NOT";
    case ExprNode_Type.IN:
      return "IN";
    case ExprNode_Type.BITWISE_AND:
      return "BITWISE_AND";
    case ExprNode_Type.BITWISE_OR:
      return "BITWISE_OR";
    case ExprNode_Type.BITWISE_XOR:
      return "BITWISE_XOR";
    case ExprNode_Type.BITWISE_NOT:
      return "BITWISE_NOT";
    case ExprNode_Type.BITWISE_SHIFT_LEFT:
      return "BITWISE_SHIFT_LEFT";
    case ExprNode_Type.BITWISE_SHIFT_RIGHT:
      return "BITWISE_SHIFT_RIGHT";
    case ExprNode_Type.EXTRACT:
      return "EXTRACT";
    case ExprNode_Type.TUMBLE_START:
      return "TUMBLE_START";
    case ExprNode_Type.CAST:
      return "CAST";
    case ExprNode_Type.SUBSTR:
      return "SUBSTR";
    case ExprNode_Type.LENGTH:
      return "LENGTH";
    case ExprNode_Type.LIKE:
      return "LIKE";
    case ExprNode_Type.UPPER:
      return "UPPER";
    case ExprNode_Type.LOWER:
      return "LOWER";
    case ExprNode_Type.TRIM:
      return "TRIM";
    case ExprNode_Type.REPLACE:
      return "REPLACE";
    case ExprNode_Type.POSITION:
      return "POSITION";
    case ExprNode_Type.LTRIM:
      return "LTRIM";
    case ExprNode_Type.RTRIM:
      return "RTRIM";
    case ExprNode_Type.CASE:
      return "CASE";
    case ExprNode_Type.ROUND_DIGIT:
      return "ROUND_DIGIT";
    case ExprNode_Type.ROUND:
      return "ROUND";
    case ExprNode_Type.ASCII:
      return "ASCII";
    case ExprNode_Type.TRANSLATE:
      return "TRANSLATE";
    case ExprNode_Type.COALESCE:
      return "COALESCE";
    case ExprNode_Type.CONCAT_WS:
      return "CONCAT_WS";
    case ExprNode_Type.ABS:
      return "ABS";
    case ExprNode_Type.SPLIT_PART:
      return "SPLIT_PART";
    case ExprNode_Type.CEIL:
      return "CEIL";
    case ExprNode_Type.FLOOR:
      return "FLOOR";
    case ExprNode_Type.TO_CHAR:
      return "TO_CHAR";
    case ExprNode_Type.MD5:
      return "MD5";
    case ExprNode_Type.CHAR_LENGTH:
      return "CHAR_LENGTH";
    case ExprNode_Type.REPEAT:
      return "REPEAT";
    case ExprNode_Type.CONCAT_OP:
      return "CONCAT_OP";
    case ExprNode_Type.BOOL_OUT:
      return "BOOL_OUT";
    case ExprNode_Type.OCTET_LENGTH:
      return "OCTET_LENGTH";
    case ExprNode_Type.BIT_LENGTH:
      return "BIT_LENGTH";
    case ExprNode_Type.OVERLAY:
      return "OVERLAY";
    case ExprNode_Type.REGEXP_MATCH:
      return "REGEXP_MATCH";
    case ExprNode_Type.IS_TRUE:
      return "IS_TRUE";
    case ExprNode_Type.IS_NOT_TRUE:
      return "IS_NOT_TRUE";
    case ExprNode_Type.IS_FALSE:
      return "IS_FALSE";
    case ExprNode_Type.IS_NOT_FALSE:
      return "IS_NOT_FALSE";
    case ExprNode_Type.IS_NULL:
      return "IS_NULL";
    case ExprNode_Type.IS_NOT_NULL:
      return "IS_NOT_NULL";
    case ExprNode_Type.IS_DISTINCT_FROM:
      return "IS_DISTINCT_FROM";
    case ExprNode_Type.IS_NOT_DISTINCT_FROM:
      return "IS_NOT_DISTINCT_FROM";
    case ExprNode_Type.NEG:
      return "NEG";
    case ExprNode_Type.FIELD:
      return "FIELD";
    case ExprNode_Type.ARRAY:
      return "ARRAY";
    case ExprNode_Type.ARRAY_ACCESS:
      return "ARRAY_ACCESS";
    case ExprNode_Type.ROW:
      return "ROW";
    case ExprNode_Type.SEARCH:
      return "SEARCH";
    case ExprNode_Type.SARG:
      return "SARG";
    case ExprNode_Type.VNODE:
      return "VNODE";
    case ExprNode_Type.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface TableFunction {
  functionType: TableFunction_Type;
  args: ExprNode[];
  returnType: DataType | undefined;
}

export enum TableFunction_Type {
  UNSPECIFIED = 0,
  GENERATE = 1,
  UNNEST = 2,
  REGEXP_MATCHES = 3,
  UNRECOGNIZED = -1,
}

export function tableFunction_TypeFromJSON(object: any): TableFunction_Type {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return TableFunction_Type.UNSPECIFIED;
    case 1:
    case "GENERATE":
      return TableFunction_Type.GENERATE;
    case 2:
    case "UNNEST":
      return TableFunction_Type.UNNEST;
    case 3:
    case "REGEXP_MATCHES":
      return TableFunction_Type.REGEXP_MATCHES;
    case -1:
    case "UNRECOGNIZED":
    default:
      return TableFunction_Type.UNRECOGNIZED;
  }
}

export function tableFunction_TypeToJSON(object: TableFunction_Type): string {
  switch (object) {
    case TableFunction_Type.UNSPECIFIED:
      return "UNSPECIFIED";
    case TableFunction_Type.GENERATE:
      return "GENERATE";
    case TableFunction_Type.UNNEST:
      return "UNNEST";
    case TableFunction_Type.REGEXP_MATCHES:
      return "REGEXP_MATCHES";
    case TableFunction_Type.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface InputRefExpr {
  columnIdx: number;
}

/**
 * The items which can occur in the select list of `ProjectSet` operator.
 *
 * When there are table functions in the SQL query `SELECT ...`, it will be planned as `ProjectSet`.
 * Otherwise it will be planned as `Project`.
 *
 * # Examples
 *
 * ```sql
 * # Project
 * select 1;
 *
 * # ProjectSet
 * select unnest(array[1,2,3]);
 *
 * # ProjectSet (table function & usual expression)
 * select unnest(array[1,2,3]), 1;
 *
 * # ProjectSet (multiple table functions)
 * select unnest(array[1,2,3]), unnest(array[4,5]);
 *
 * # ProjectSet over ProjectSet (table function as parameters of table function)
 * select unnest(regexp_matches(v1, 'a(\d)c(\d)', 'g')) from t;
 *
 * # Project over ProjectSet (table function as parameters of usual function)
 * select unnest(regexp_matches(v1, 'a(\d)c(\d)', 'g')) from t;
 * ```
 */
export interface ProjectSetSelectItem {
  selectItem?: { $case: "expr"; expr: ExprNode } | { $case: "tableFunction"; tableFunction: TableFunction };
}

export interface ConstantValue {
  /**
   * bool array/bitmap: one byte, 0 for false (null), non-zero for true (non-null)
   * integer, float,  double: big-endianness
   * interval: encoded to (months, days, milliseconds), big-endianness
   * varchar: encoded accorded to encoding, currently only utf8 is supported.
   */
  body: Uint8Array;
}

/** For serializing struct value to vec<bytes>. */
export interface StructValue {
  fields: Uint8Array[];
}

/** For serializing list value to vec<bytes>. */
export interface ListValue {
  fields: Uint8Array[];
}

export interface FunctionCall {
  children: ExprNode[];
}

/** Aggregate Function Calls for Aggregation */
export interface AggCall {
  type: AggCall_Type;
  args: AggCall_Arg[];
  returnType: DataType | undefined;
  distinct: boolean;
  orderByFields: AggCall_OrderByField[];
  filter: ExprNode | undefined;
}

export enum AggCall_Type {
  UNSPECIFIED = 0,
  SUM = 1,
  MIN = 2,
  MAX = 3,
  COUNT = 4,
  AVG = 5,
  STRING_AGG = 6,
  SINGLE_VALUE = 7,
  APPROX_COUNT_DISTINCT = 8,
  ARRAY_AGG = 9,
  UNRECOGNIZED = -1,
}

export function aggCall_TypeFromJSON(object: any): AggCall_Type {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return AggCall_Type.UNSPECIFIED;
    case 1:
    case "SUM":
      return AggCall_Type.SUM;
    case 2:
    case "MIN":
      return AggCall_Type.MIN;
    case 3:
    case "MAX":
      return AggCall_Type.MAX;
    case 4:
    case "COUNT":
      return AggCall_Type.COUNT;
    case 5:
    case "AVG":
      return AggCall_Type.AVG;
    case 6:
    case "STRING_AGG":
      return AggCall_Type.STRING_AGG;
    case 7:
    case "SINGLE_VALUE":
      return AggCall_Type.SINGLE_VALUE;
    case 8:
    case "APPROX_COUNT_DISTINCT":
      return AggCall_Type.APPROX_COUNT_DISTINCT;
    case 9:
    case "ARRAY_AGG":
      return AggCall_Type.ARRAY_AGG;
    case -1:
    case "UNRECOGNIZED":
    default:
      return AggCall_Type.UNRECOGNIZED;
  }
}

export function aggCall_TypeToJSON(object: AggCall_Type): string {
  switch (object) {
    case AggCall_Type.UNSPECIFIED:
      return "UNSPECIFIED";
    case AggCall_Type.SUM:
      return "SUM";
    case AggCall_Type.MIN:
      return "MIN";
    case AggCall_Type.MAX:
      return "MAX";
    case AggCall_Type.COUNT:
      return "COUNT";
    case AggCall_Type.AVG:
      return "AVG";
    case AggCall_Type.STRING_AGG:
      return "STRING_AGG";
    case AggCall_Type.SINGLE_VALUE:
      return "SINGLE_VALUE";
    case AggCall_Type.APPROX_COUNT_DISTINCT:
      return "APPROX_COUNT_DISTINCT";
    case AggCall_Type.ARRAY_AGG:
      return "ARRAY_AGG";
    case AggCall_Type.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface AggCall_Arg {
  input: InputRefExpr | undefined;
  type: DataType | undefined;
}

export interface AggCall_OrderByField {
  input: InputRefExpr | undefined;
  type: DataType | undefined;
  direction: OrderType;
  nullsFirst: boolean;
}

function createBaseExprNode(): ExprNode {
  return { exprType: 0, returnType: undefined, rexNode: undefined };
}

export const ExprNode = {
  encode(message: ExprNode, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.exprType !== 0) {
      writer.uint32(8).int32(message.exprType);
    }
    if (message.returnType !== undefined) {
      DataType.encode(message.returnType, writer.uint32(26).fork()).ldelim();
    }
    if (message.rexNode?.$case === "inputRef") {
      InputRefExpr.encode(message.rexNode.inputRef, writer.uint32(34).fork()).ldelim();
    }
    if (message.rexNode?.$case === "constant") {
      ConstantValue.encode(message.rexNode.constant, writer.uint32(42).fork()).ldelim();
    }
    if (message.rexNode?.$case === "funcCall") {
      FunctionCall.encode(message.rexNode.funcCall, writer.uint32(50).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ExprNode {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseExprNode();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.exprType = reader.int32() as any;
          break;
        case 3:
          message.returnType = DataType.decode(reader, reader.uint32());
          break;
        case 4:
          message.rexNode = { $case: "inputRef", inputRef: InputRefExpr.decode(reader, reader.uint32()) };
          break;
        case 5:
          message.rexNode = { $case: "constant", constant: ConstantValue.decode(reader, reader.uint32()) };
          break;
        case 6:
          message.rexNode = { $case: "funcCall", funcCall: FunctionCall.decode(reader, reader.uint32()) };
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ExprNode {
    return {
      exprType: isSet(object.exprType) ? exprNode_TypeFromJSON(object.exprType) : 0,
      returnType: isSet(object.returnType) ? DataType.fromJSON(object.returnType) : undefined,
      rexNode: isSet(object.inputRef)
        ? { $case: "inputRef", inputRef: InputRefExpr.fromJSON(object.inputRef) }
        : isSet(object.constant)
        ? { $case: "constant", constant: ConstantValue.fromJSON(object.constant) }
        : isSet(object.funcCall)
        ? { $case: "funcCall", funcCall: FunctionCall.fromJSON(object.funcCall) }
        : undefined,
    };
  },

  toJSON(message: ExprNode): unknown {
    const obj: any = {};
    message.exprType !== undefined && (obj.exprType = exprNode_TypeToJSON(message.exprType));
    message.returnType !== undefined &&
      (obj.returnType = message.returnType ? DataType.toJSON(message.returnType) : undefined);
    message.rexNode?.$case === "inputRef" &&
      (obj.inputRef = message.rexNode?.inputRef ? InputRefExpr.toJSON(message.rexNode?.inputRef) : undefined);
    message.rexNode?.$case === "constant" &&
      (obj.constant = message.rexNode?.constant ? ConstantValue.toJSON(message.rexNode?.constant) : undefined);
    message.rexNode?.$case === "funcCall" &&
      (obj.funcCall = message.rexNode?.funcCall ? FunctionCall.toJSON(message.rexNode?.funcCall) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExprNode>, I>>(object: I): ExprNode {
    const message = createBaseExprNode();
    message.exprType = object.exprType ?? 0;
    message.returnType = (object.returnType !== undefined && object.returnType !== null)
      ? DataType.fromPartial(object.returnType)
      : undefined;
    if (
      object.rexNode?.$case === "inputRef" &&
      object.rexNode?.inputRef !== undefined &&
      object.rexNode?.inputRef !== null
    ) {
      message.rexNode = { $case: "inputRef", inputRef: InputRefExpr.fromPartial(object.rexNode.inputRef) };
    }
    if (
      object.rexNode?.$case === "constant" &&
      object.rexNode?.constant !== undefined &&
      object.rexNode?.constant !== null
    ) {
      message.rexNode = { $case: "constant", constant: ConstantValue.fromPartial(object.rexNode.constant) };
    }
    if (
      object.rexNode?.$case === "funcCall" &&
      object.rexNode?.funcCall !== undefined &&
      object.rexNode?.funcCall !== null
    ) {
      message.rexNode = { $case: "funcCall", funcCall: FunctionCall.fromPartial(object.rexNode.funcCall) };
    }
    return message;
  },
};

function createBaseTableFunction(): TableFunction {
  return { functionType: 0, args: [], returnType: undefined };
}

export const TableFunction = {
  encode(message: TableFunction, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.functionType !== 0) {
      writer.uint32(8).int32(message.functionType);
    }
    for (const v of message.args) {
      ExprNode.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.returnType !== undefined) {
      DataType.encode(message.returnType, writer.uint32(26).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): TableFunction {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseTableFunction();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.functionType = reader.int32() as any;
          break;
        case 2:
          message.args.push(ExprNode.decode(reader, reader.uint32()));
          break;
        case 3:
          message.returnType = DataType.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): TableFunction {
    return {
      functionType: isSet(object.functionType) ? tableFunction_TypeFromJSON(object.functionType) : 0,
      args: Array.isArray(object?.args) ? object.args.map((e: any) => ExprNode.fromJSON(e)) : [],
      returnType: isSet(object.returnType) ? DataType.fromJSON(object.returnType) : undefined,
    };
  },

  toJSON(message: TableFunction): unknown {
    const obj: any = {};
    message.functionType !== undefined && (obj.functionType = tableFunction_TypeToJSON(message.functionType));
    if (message.args) {
      obj.args = message.args.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.args = [];
    }
    message.returnType !== undefined &&
      (obj.returnType = message.returnType ? DataType.toJSON(message.returnType) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<TableFunction>, I>>(object: I): TableFunction {
    const message = createBaseTableFunction();
    message.functionType = object.functionType ?? 0;
    message.args = object.args?.map((e) => ExprNode.fromPartial(e)) || [];
    message.returnType = (object.returnType !== undefined && object.returnType !== null)
      ? DataType.fromPartial(object.returnType)
      : undefined;
    return message;
  },
};

function createBaseInputRefExpr(): InputRefExpr {
  return { columnIdx: 0 };
}

export const InputRefExpr = {
  encode(message: InputRefExpr, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.columnIdx !== 0) {
      writer.uint32(8).int32(message.columnIdx);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): InputRefExpr {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseInputRefExpr();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.columnIdx = reader.int32();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): InputRefExpr {
    return { columnIdx: isSet(object.columnIdx) ? Number(object.columnIdx) : 0 };
  },

  toJSON(message: InputRefExpr): unknown {
    const obj: any = {};
    message.columnIdx !== undefined && (obj.columnIdx = Math.round(message.columnIdx));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<InputRefExpr>, I>>(object: I): InputRefExpr {
    const message = createBaseInputRefExpr();
    message.columnIdx = object.columnIdx ?? 0;
    return message;
  },
};

function createBaseProjectSetSelectItem(): ProjectSetSelectItem {
  return { selectItem: undefined };
}

export const ProjectSetSelectItem = {
  encode(message: ProjectSetSelectItem, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.selectItem?.$case === "expr") {
      ExprNode.encode(message.selectItem.expr, writer.uint32(10).fork()).ldelim();
    }
    if (message.selectItem?.$case === "tableFunction") {
      TableFunction.encode(message.selectItem.tableFunction, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ProjectSetSelectItem {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseProjectSetSelectItem();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.selectItem = { $case: "expr", expr: ExprNode.decode(reader, reader.uint32()) };
          break;
        case 2:
          message.selectItem = { $case: "tableFunction", tableFunction: TableFunction.decode(reader, reader.uint32()) };
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ProjectSetSelectItem {
    return {
      selectItem: isSet(object.expr)
        ? { $case: "expr", expr: ExprNode.fromJSON(object.expr) }
        : isSet(object.tableFunction)
        ? { $case: "tableFunction", tableFunction: TableFunction.fromJSON(object.tableFunction) }
        : undefined,
    };
  },

  toJSON(message: ProjectSetSelectItem): unknown {
    const obj: any = {};
    message.selectItem?.$case === "expr" &&
      (obj.expr = message.selectItem?.expr ? ExprNode.toJSON(message.selectItem?.expr) : undefined);
    message.selectItem?.$case === "tableFunction" && (obj.tableFunction = message.selectItem?.tableFunction
      ? TableFunction.toJSON(message.selectItem?.tableFunction)
      : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ProjectSetSelectItem>, I>>(object: I): ProjectSetSelectItem {
    const message = createBaseProjectSetSelectItem();
    if (
      object.selectItem?.$case === "expr" && object.selectItem?.expr !== undefined && object.selectItem?.expr !== null
    ) {
      message.selectItem = { $case: "expr", expr: ExprNode.fromPartial(object.selectItem.expr) };
    }
    if (
      object.selectItem?.$case === "tableFunction" &&
      object.selectItem?.tableFunction !== undefined &&
      object.selectItem?.tableFunction !== null
    ) {
      message.selectItem = {
        $case: "tableFunction",
        tableFunction: TableFunction.fromPartial(object.selectItem.tableFunction),
      };
    }
    return message;
  },
};

function createBaseConstantValue(): ConstantValue {
  return { body: new Uint8Array() };
}

export const ConstantValue = {
  encode(message: ConstantValue, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.body.length !== 0) {
      writer.uint32(10).bytes(message.body);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ConstantValue {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseConstantValue();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.body = reader.bytes();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ConstantValue {
    return { body: isSet(object.body) ? bytesFromBase64(object.body) : new Uint8Array() };
  },

  toJSON(message: ConstantValue): unknown {
    const obj: any = {};
    message.body !== undefined &&
      (obj.body = base64FromBytes(message.body !== undefined ? message.body : new Uint8Array()));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ConstantValue>, I>>(object: I): ConstantValue {
    const message = createBaseConstantValue();
    message.body = object.body ?? new Uint8Array();
    return message;
  },
};

function createBaseStructValue(): StructValue {
  return { fields: [] };
}

export const StructValue = {
  encode(message: StructValue, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.fields) {
      writer.uint32(10).bytes(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): StructValue {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseStructValue();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.fields.push(reader.bytes());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): StructValue {
    return { fields: Array.isArray(object?.fields) ? object.fields.map((e: any) => bytesFromBase64(e)) : [] };
  },

  toJSON(message: StructValue): unknown {
    const obj: any = {};
    if (message.fields) {
      obj.fields = message.fields.map((e) => base64FromBytes(e !== undefined ? e : new Uint8Array()));
    } else {
      obj.fields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<StructValue>, I>>(object: I): StructValue {
    const message = createBaseStructValue();
    message.fields = object.fields?.map((e) => e) || [];
    return message;
  },
};

function createBaseListValue(): ListValue {
  return { fields: [] };
}

export const ListValue = {
  encode(message: ListValue, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.fields) {
      writer.uint32(10).bytes(v!);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): ListValue {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseListValue();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.fields.push(reader.bytes());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): ListValue {
    return { fields: Array.isArray(object?.fields) ? object.fields.map((e: any) => bytesFromBase64(e)) : [] };
  },

  toJSON(message: ListValue): unknown {
    const obj: any = {};
    if (message.fields) {
      obj.fields = message.fields.map((e) => base64FromBytes(e !== undefined ? e : new Uint8Array()));
    } else {
      obj.fields = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ListValue>, I>>(object: I): ListValue {
    const message = createBaseListValue();
    message.fields = object.fields?.map((e) => e) || [];
    return message;
  },
};

function createBaseFunctionCall(): FunctionCall {
  return { children: [] };
}

export const FunctionCall = {
  encode(message: FunctionCall, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    for (const v of message.children) {
      ExprNode.encode(v!, writer.uint32(10).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): FunctionCall {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseFunctionCall();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.children.push(ExprNode.decode(reader, reader.uint32()));
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): FunctionCall {
    return { children: Array.isArray(object?.children) ? object.children.map((e: any) => ExprNode.fromJSON(e)) : [] };
  },

  toJSON(message: FunctionCall): unknown {
    const obj: any = {};
    if (message.children) {
      obj.children = message.children.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.children = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<FunctionCall>, I>>(object: I): FunctionCall {
    const message = createBaseFunctionCall();
    message.children = object.children?.map((e) => ExprNode.fromPartial(e)) || [];
    return message;
  },
};

function createBaseAggCall(): AggCall {
  return { type: 0, args: [], returnType: undefined, distinct: false, orderByFields: [], filter: undefined };
}

export const AggCall = {
  encode(message: AggCall, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.type !== 0) {
      writer.uint32(8).int32(message.type);
    }
    for (const v of message.args) {
      AggCall_Arg.encode(v!, writer.uint32(18).fork()).ldelim();
    }
    if (message.returnType !== undefined) {
      DataType.encode(message.returnType, writer.uint32(26).fork()).ldelim();
    }
    if (message.distinct === true) {
      writer.uint32(32).bool(message.distinct);
    }
    for (const v of message.orderByFields) {
      AggCall_OrderByField.encode(v!, writer.uint32(42).fork()).ldelim();
    }
    if (message.filter !== undefined) {
      ExprNode.encode(message.filter, writer.uint32(50).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AggCall {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAggCall();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.type = reader.int32() as any;
          break;
        case 2:
          message.args.push(AggCall_Arg.decode(reader, reader.uint32()));
          break;
        case 3:
          message.returnType = DataType.decode(reader, reader.uint32());
          break;
        case 4:
          message.distinct = reader.bool();
          break;
        case 5:
          message.orderByFields.push(AggCall_OrderByField.decode(reader, reader.uint32()));
          break;
        case 6:
          message.filter = ExprNode.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AggCall {
    return {
      type: isSet(object.type) ? aggCall_TypeFromJSON(object.type) : 0,
      args: Array.isArray(object?.args) ? object.args.map((e: any) => AggCall_Arg.fromJSON(e)) : [],
      returnType: isSet(object.returnType) ? DataType.fromJSON(object.returnType) : undefined,
      distinct: isSet(object.distinct) ? Boolean(object.distinct) : false,
      orderByFields: Array.isArray(object?.orderByFields)
        ? object.orderByFields.map((e: any) => AggCall_OrderByField.fromJSON(e))
        : [],
      filter: isSet(object.filter) ? ExprNode.fromJSON(object.filter) : undefined,
    };
  },

  toJSON(message: AggCall): unknown {
    const obj: any = {};
    message.type !== undefined && (obj.type = aggCall_TypeToJSON(message.type));
    if (message.args) {
      obj.args = message.args.map((e) => e ? AggCall_Arg.toJSON(e) : undefined);
    } else {
      obj.args = [];
    }
    message.returnType !== undefined &&
      (obj.returnType = message.returnType ? DataType.toJSON(message.returnType) : undefined);
    message.distinct !== undefined && (obj.distinct = message.distinct);
    if (message.orderByFields) {
      obj.orderByFields = message.orderByFields.map((e) => e ? AggCall_OrderByField.toJSON(e) : undefined);
    } else {
      obj.orderByFields = [];
    }
    message.filter !== undefined && (obj.filter = message.filter ? ExprNode.toJSON(message.filter) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCall>, I>>(object: I): AggCall {
    const message = createBaseAggCall();
    message.type = object.type ?? 0;
    message.args = object.args?.map((e) => AggCall_Arg.fromPartial(e)) || [];
    message.returnType = (object.returnType !== undefined && object.returnType !== null)
      ? DataType.fromPartial(object.returnType)
      : undefined;
    message.distinct = object.distinct ?? false;
    message.orderByFields = object.orderByFields?.map((e) => AggCall_OrderByField.fromPartial(e)) || [];
    message.filter = (object.filter !== undefined && object.filter !== null)
      ? ExprNode.fromPartial(object.filter)
      : undefined;
    return message;
  },
};

function createBaseAggCall_Arg(): AggCall_Arg {
  return { input: undefined, type: undefined };
}

export const AggCall_Arg = {
  encode(message: AggCall_Arg, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.input !== undefined) {
      InputRefExpr.encode(message.input, writer.uint32(10).fork()).ldelim();
    }
    if (message.type !== undefined) {
      DataType.encode(message.type, writer.uint32(18).fork()).ldelim();
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AggCall_Arg {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAggCall_Arg();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.input = InputRefExpr.decode(reader, reader.uint32());
          break;
        case 2:
          message.type = DataType.decode(reader, reader.uint32());
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AggCall_Arg {
    return {
      input: isSet(object.input) ? InputRefExpr.fromJSON(object.input) : undefined,
      type: isSet(object.type) ? DataType.fromJSON(object.type) : undefined,
    };
  },

  toJSON(message: AggCall_Arg): unknown {
    const obj: any = {};
    message.input !== undefined && (obj.input = message.input ? InputRefExpr.toJSON(message.input) : undefined);
    message.type !== undefined && (obj.type = message.type ? DataType.toJSON(message.type) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCall_Arg>, I>>(object: I): AggCall_Arg {
    const message = createBaseAggCall_Arg();
    message.input = (object.input !== undefined && object.input !== null)
      ? InputRefExpr.fromPartial(object.input)
      : undefined;
    message.type = (object.type !== undefined && object.type !== null) ? DataType.fromPartial(object.type) : undefined;
    return message;
  },
};

function createBaseAggCall_OrderByField(): AggCall_OrderByField {
  return { input: undefined, type: undefined, direction: 0, nullsFirst: false };
}

export const AggCall_OrderByField = {
  encode(message: AggCall_OrderByField, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.input !== undefined) {
      InputRefExpr.encode(message.input, writer.uint32(10).fork()).ldelim();
    }
    if (message.type !== undefined) {
      DataType.encode(message.type, writer.uint32(18).fork()).ldelim();
    }
    if (message.direction !== 0) {
      writer.uint32(24).int32(message.direction);
    }
    if (message.nullsFirst === true) {
      writer.uint32(32).bool(message.nullsFirst);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): AggCall_OrderByField {
    const reader = input instanceof _m0.Reader ? input : new _m0.Reader(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAggCall_OrderByField();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          message.input = InputRefExpr.decode(reader, reader.uint32());
          break;
        case 2:
          message.type = DataType.decode(reader, reader.uint32());
          break;
        case 3:
          message.direction = reader.int32() as any;
          break;
        case 4:
          message.nullsFirst = reader.bool();
          break;
        default:
          reader.skipType(tag & 7);
          break;
      }
    }
    return message;
  },

  fromJSON(object: any): AggCall_OrderByField {
    return {
      input: isSet(object.input) ? InputRefExpr.fromJSON(object.input) : undefined,
      type: isSet(object.type) ? DataType.fromJSON(object.type) : undefined,
      direction: isSet(object.direction) ? orderTypeFromJSON(object.direction) : 0,
      nullsFirst: isSet(object.nullsFirst) ? Boolean(object.nullsFirst) : false,
    };
  },

  toJSON(message: AggCall_OrderByField): unknown {
    const obj: any = {};
    message.input !== undefined && (obj.input = message.input ? InputRefExpr.toJSON(message.input) : undefined);
    message.type !== undefined && (obj.type = message.type ? DataType.toJSON(message.type) : undefined);
    message.direction !== undefined && (obj.direction = orderTypeToJSON(message.direction));
    message.nullsFirst !== undefined && (obj.nullsFirst = message.nullsFirst);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<AggCall_OrderByField>, I>>(object: I): AggCall_OrderByField {
    const message = createBaseAggCall_OrderByField();
    message.input = (object.input !== undefined && object.input !== null)
      ? InputRefExpr.fromPartial(object.input)
      : undefined;
    message.type = (object.type !== undefined && object.type !== null) ? DataType.fromPartial(object.type) : undefined;
    message.direction = object.direction ?? 0;
    message.nullsFirst = object.nullsFirst ?? false;
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
