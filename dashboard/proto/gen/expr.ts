/* eslint-disable */
import { DataType, Datum } from "./data";
import { OrderType, orderTypeFromJSON, orderTypeToJSON } from "./plan_common";

export const protobufPackage = "expr";

export interface ExprNode {
  exprType: ExprNode_Type;
  returnType: DataType | undefined;
  rexNode?: { $case: "inputRef"; inputRef: InputRefExpr } | { $case: "constant"; constant: Datum } | {
    $case: "funcCall";
    funcCall: FunctionCall;
  } | { $case: "udf"; udf: UserDefinedFunction };
}

/**
 * a "pure function" will be defined as having `1 < expr_node as i32 <= 600`.
 * Please modify this definition if adding a pure function that does not belong
 * to this range.
 */
export const ExprNode_Type = {
  UNSPECIFIED: "UNSPECIFIED",
  INPUT_REF: "INPUT_REF",
  CONSTANT_VALUE: "CONSTANT_VALUE",
  /** ADD - arithmetics operators */
  ADD: "ADD",
  SUBTRACT: "SUBTRACT",
  MULTIPLY: "MULTIPLY",
  DIVIDE: "DIVIDE",
  MODULUS: "MODULUS",
  /** EQUAL - comparison operators */
  EQUAL: "EQUAL",
  NOT_EQUAL: "NOT_EQUAL",
  LESS_THAN: "LESS_THAN",
  LESS_THAN_OR_EQUAL: "LESS_THAN_OR_EQUAL",
  GREATER_THAN: "GREATER_THAN",
  GREATER_THAN_OR_EQUAL: "GREATER_THAN_OR_EQUAL",
  /** AND - logical operators */
  AND: "AND",
  OR: "OR",
  NOT: "NOT",
  IN: "IN",
  SOME: "SOME",
  ALL: "ALL",
  /** BITWISE_AND - bitwise operators */
  BITWISE_AND: "BITWISE_AND",
  BITWISE_OR: "BITWISE_OR",
  BITWISE_XOR: "BITWISE_XOR",
  BITWISE_NOT: "BITWISE_NOT",
  BITWISE_SHIFT_LEFT: "BITWISE_SHIFT_LEFT",
  BITWISE_SHIFT_RIGHT: "BITWISE_SHIFT_RIGHT",
  /** EXTRACT - date functions */
  EXTRACT: "EXTRACT",
  TUMBLE_START: "TUMBLE_START",
  /**
   * TO_TIMESTAMP - From f64 to timestamp.
   * e.g. `select to_timestamp(1672044740.0)`
   */
  TO_TIMESTAMP: "TO_TIMESTAMP",
  AT_TIME_ZONE: "AT_TIME_ZONE",
  DATE_TRUNC: "DATE_TRUNC",
  /**
   * TO_TIMESTAMP1 - Parse text to timestamp by format string.
   * e.g. `select to_timestamp('2022 08 21', 'YYYY MM DD')`
   */
  TO_TIMESTAMP1: "TO_TIMESTAMP1",
  /** CAST_WITH_TIME_ZONE - Performs a cast with additional timezone information. */
  CAST_WITH_TIME_ZONE: "CAST_WITH_TIME_ZONE",
  /** CAST - other functions */
  CAST: "CAST",
  SUBSTR: "SUBSTR",
  LENGTH: "LENGTH",
  LIKE: "LIKE",
  UPPER: "UPPER",
  LOWER: "LOWER",
  TRIM: "TRIM",
  REPLACE: "REPLACE",
  POSITION: "POSITION",
  LTRIM: "LTRIM",
  RTRIM: "RTRIM",
  CASE: "CASE",
  /** ROUND_DIGIT - ROUND(numeric, integer) -> numeric */
  ROUND_DIGIT: "ROUND_DIGIT",
  /**
   * ROUND - ROUND(numeric) -> numeric
   * ROUND(double precision) -> double precision
   */
  ROUND: "ROUND",
  ASCII: "ASCII",
  TRANSLATE: "TRANSLATE",
  COALESCE: "COALESCE",
  CONCAT_WS: "CONCAT_WS",
  ABS: "ABS",
  SPLIT_PART: "SPLIT_PART",
  CEIL: "CEIL",
  FLOOR: "FLOOR",
  TO_CHAR: "TO_CHAR",
  MD5: "MD5",
  CHAR_LENGTH: "CHAR_LENGTH",
  REPEAT: "REPEAT",
  CONCAT_OP: "CONCAT_OP",
  /** BOOL_OUT - BOOL_OUT is different from CAST-bool-to-varchar in PostgreSQL. */
  BOOL_OUT: "BOOL_OUT",
  OCTET_LENGTH: "OCTET_LENGTH",
  BIT_LENGTH: "BIT_LENGTH",
  OVERLAY: "OVERLAY",
  REGEXP_MATCH: "REGEXP_MATCH",
  POW: "POW",
  EXP: "EXP",
  /** IS_TRUE - Boolean comparison */
  IS_TRUE: "IS_TRUE",
  IS_NOT_TRUE: "IS_NOT_TRUE",
  IS_FALSE: "IS_FALSE",
  IS_NOT_FALSE: "IS_NOT_FALSE",
  IS_NULL: "IS_NULL",
  IS_NOT_NULL: "IS_NOT_NULL",
  IS_DISTINCT_FROM: "IS_DISTINCT_FROM",
  IS_NOT_DISTINCT_FROM: "IS_NOT_DISTINCT_FROM",
  /** NEG - Unary operators */
  NEG: "NEG",
  /** FIELD - Nested selection operators */
  FIELD: "FIELD",
  /** ARRAY - Array expression. */
  ARRAY: "ARRAY",
  ARRAY_ACCESS: "ARRAY_ACCESS",
  ROW: "ROW",
  ARRAY_TO_STRING: "ARRAY_TO_STRING",
  /** ARRAY_CAT - Array functions */
  ARRAY_CAT: "ARRAY_CAT",
  ARRAY_APPEND: "ARRAY_APPEND",
  ARRAY_PREPEND: "ARRAY_PREPEND",
  FORMAT_TYPE: "FORMAT_TYPE",
  ARRAY_DISTINCT: "ARRAY_DISTINCT",
  /** JSONB_ACCESS_INNER - jsonb -> int, jsonb -> text, jsonb #> text[] that returns jsonb */
  JSONB_ACCESS_INNER: "JSONB_ACCESS_INNER",
  /** JSONB_ACCESS_STR - jsonb ->> int, jsonb ->> text, jsonb #>> text[] that returns text */
  JSONB_ACCESS_STR: "JSONB_ACCESS_STR",
  JSONB_TYPEOF: "JSONB_TYPEOF",
  JSONB_ARRAY_LENGTH: "JSONB_ARRAY_LENGTH",
  /**
   * VNODE - Non-pure functions below (> 1000)
   * ------------------------
   * Internal functions
   */
  VNODE: "VNODE",
  /** NOW - Non-deterministic functions */
  NOW: "NOW",
  /** UDF - User defined functions */
  UDF: "UDF",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type ExprNode_Type = typeof ExprNode_Type[keyof typeof ExprNode_Type];

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
    case 25:
    case "SOME":
      return ExprNode_Type.SOME;
    case 26:
    case "ALL":
      return ExprNode_Type.ALL;
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
    case 104:
    case "TO_TIMESTAMP":
      return ExprNode_Type.TO_TIMESTAMP;
    case 105:
    case "AT_TIME_ZONE":
      return ExprNode_Type.AT_TIME_ZONE;
    case 106:
    case "DATE_TRUNC":
      return ExprNode_Type.DATE_TRUNC;
    case 107:
    case "TO_TIMESTAMP1":
      return ExprNode_Type.TO_TIMESTAMP1;
    case 108:
    case "CAST_WITH_TIME_ZONE":
      return ExprNode_Type.CAST_WITH_TIME_ZONE;
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
    case 233:
    case "POW":
      return ExprNode_Type.POW;
    case 234:
    case "EXP":
      return ExprNode_Type.EXP;
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
    case 524:
    case "ARRAY_TO_STRING":
      return ExprNode_Type.ARRAY_TO_STRING;
    case 531:
    case "ARRAY_CAT":
      return ExprNode_Type.ARRAY_CAT;
    case 532:
    case "ARRAY_APPEND":
      return ExprNode_Type.ARRAY_APPEND;
    case 533:
    case "ARRAY_PREPEND":
      return ExprNode_Type.ARRAY_PREPEND;
    case 534:
    case "FORMAT_TYPE":
      return ExprNode_Type.FORMAT_TYPE;
    case 535:
    case "ARRAY_DISTINCT":
      return ExprNode_Type.ARRAY_DISTINCT;
    case 600:
    case "JSONB_ACCESS_INNER":
      return ExprNode_Type.JSONB_ACCESS_INNER;
    case 601:
    case "JSONB_ACCESS_STR":
      return ExprNode_Type.JSONB_ACCESS_STR;
    case 602:
    case "JSONB_TYPEOF":
      return ExprNode_Type.JSONB_TYPEOF;
    case 603:
    case "JSONB_ARRAY_LENGTH":
      return ExprNode_Type.JSONB_ARRAY_LENGTH;
    case 1101:
    case "VNODE":
      return ExprNode_Type.VNODE;
    case 2022:
    case "NOW":
      return ExprNode_Type.NOW;
    case 3000:
    case "UDF":
      return ExprNode_Type.UDF;
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
    case ExprNode_Type.SOME:
      return "SOME";
    case ExprNode_Type.ALL:
      return "ALL";
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
    case ExprNode_Type.TO_TIMESTAMP:
      return "TO_TIMESTAMP";
    case ExprNode_Type.AT_TIME_ZONE:
      return "AT_TIME_ZONE";
    case ExprNode_Type.DATE_TRUNC:
      return "DATE_TRUNC";
    case ExprNode_Type.TO_TIMESTAMP1:
      return "TO_TIMESTAMP1";
    case ExprNode_Type.CAST_WITH_TIME_ZONE:
      return "CAST_WITH_TIME_ZONE";
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
    case ExprNode_Type.POW:
      return "POW";
    case ExprNode_Type.EXP:
      return "EXP";
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
    case ExprNode_Type.ARRAY_TO_STRING:
      return "ARRAY_TO_STRING";
    case ExprNode_Type.ARRAY_CAT:
      return "ARRAY_CAT";
    case ExprNode_Type.ARRAY_APPEND:
      return "ARRAY_APPEND";
    case ExprNode_Type.ARRAY_PREPEND:
      return "ARRAY_PREPEND";
    case ExprNode_Type.FORMAT_TYPE:
      return "FORMAT_TYPE";
    case ExprNode_Type.ARRAY_DISTINCT:
      return "ARRAY_DISTINCT";
    case ExprNode_Type.JSONB_ACCESS_INNER:
      return "JSONB_ACCESS_INNER";
    case ExprNode_Type.JSONB_ACCESS_STR:
      return "JSONB_ACCESS_STR";
    case ExprNode_Type.JSONB_TYPEOF:
      return "JSONB_TYPEOF";
    case ExprNode_Type.JSONB_ARRAY_LENGTH:
      return "JSONB_ARRAY_LENGTH";
    case ExprNode_Type.VNODE:
      return "VNODE";
    case ExprNode_Type.NOW:
      return "NOW";
    case ExprNode_Type.UDF:
      return "UDF";
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

export const TableFunction_Type = {
  UNSPECIFIED: "UNSPECIFIED",
  GENERATE: "GENERATE",
  UNNEST: "UNNEST",
  REGEXP_MATCHES: "REGEXP_MATCHES",
  RANGE: "RANGE",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type TableFunction_Type = typeof TableFunction_Type[keyof typeof TableFunction_Type];

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
    case 4:
    case "RANGE":
      return TableFunction_Type.RANGE;
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
    case TableFunction_Type.RANGE:
      return "RANGE";
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

export const AggCall_Type = {
  UNSPECIFIED: "UNSPECIFIED",
  SUM: "SUM",
  MIN: "MIN",
  MAX: "MAX",
  COUNT: "COUNT",
  AVG: "AVG",
  STRING_AGG: "STRING_AGG",
  APPROX_COUNT_DISTINCT: "APPROX_COUNT_DISTINCT",
  ARRAY_AGG: "ARRAY_AGG",
  FIRST_VALUE: "FIRST_VALUE",
  SUM0: "SUM0",
  VAR_POP: "VAR_POP",
  VAR_SAMP: "VAR_SAMP",
  STDDEV_POP: "STDDEV_POP",
  STDDEV_SAMP: "STDDEV_SAMP",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type AggCall_Type = typeof AggCall_Type[keyof typeof AggCall_Type];

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
    case "APPROX_COUNT_DISTINCT":
      return AggCall_Type.APPROX_COUNT_DISTINCT;
    case 8:
    case "ARRAY_AGG":
      return AggCall_Type.ARRAY_AGG;
    case 9:
    case "FIRST_VALUE":
      return AggCall_Type.FIRST_VALUE;
    case 10:
    case "SUM0":
      return AggCall_Type.SUM0;
    case 11:
    case "VAR_POP":
      return AggCall_Type.VAR_POP;
    case 12:
    case "VAR_SAMP":
      return AggCall_Type.VAR_SAMP;
    case 13:
    case "STDDEV_POP":
      return AggCall_Type.STDDEV_POP;
    case 14:
    case "STDDEV_SAMP":
      return AggCall_Type.STDDEV_SAMP;
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
    case AggCall_Type.APPROX_COUNT_DISTINCT:
      return "APPROX_COUNT_DISTINCT";
    case AggCall_Type.ARRAY_AGG:
      return "ARRAY_AGG";
    case AggCall_Type.FIRST_VALUE:
      return "FIRST_VALUE";
    case AggCall_Type.SUM0:
      return "SUM0";
    case AggCall_Type.VAR_POP:
      return "VAR_POP";
    case AggCall_Type.VAR_SAMP:
      return "VAR_SAMP";
    case AggCall_Type.STDDEV_POP:
      return "STDDEV_POP";
    case AggCall_Type.STDDEV_SAMP:
      return "STDDEV_SAMP";
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

export interface UserDefinedFunction {
  children: ExprNode[];
  name: string;
  argTypes: DataType[];
  language: string;
  link: string;
  identifier: string;
}

function createBaseExprNode(): ExprNode {
  return { exprType: ExprNode_Type.UNSPECIFIED, returnType: undefined, rexNode: undefined };
}

export const ExprNode = {
  fromJSON(object: any): ExprNode {
    return {
      exprType: isSet(object.exprType) ? exprNode_TypeFromJSON(object.exprType) : ExprNode_Type.UNSPECIFIED,
      returnType: isSet(object.returnType) ? DataType.fromJSON(object.returnType) : undefined,
      rexNode: isSet(object.inputRef)
        ? { $case: "inputRef", inputRef: InputRefExpr.fromJSON(object.inputRef) }
        : isSet(object.constant)
        ? { $case: "constant", constant: Datum.fromJSON(object.constant) }
        : isSet(object.funcCall)
        ? { $case: "funcCall", funcCall: FunctionCall.fromJSON(object.funcCall) }
        : isSet(object.udf)
        ? { $case: "udf", udf: UserDefinedFunction.fromJSON(object.udf) }
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
      (obj.constant = message.rexNode?.constant ? Datum.toJSON(message.rexNode?.constant) : undefined);
    message.rexNode?.$case === "funcCall" &&
      (obj.funcCall = message.rexNode?.funcCall ? FunctionCall.toJSON(message.rexNode?.funcCall) : undefined);
    message.rexNode?.$case === "udf" &&
      (obj.udf = message.rexNode?.udf ? UserDefinedFunction.toJSON(message.rexNode?.udf) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<ExprNode>, I>>(object: I): ExprNode {
    const message = createBaseExprNode();
    message.exprType = object.exprType ?? ExprNode_Type.UNSPECIFIED;
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
      message.rexNode = { $case: "constant", constant: Datum.fromPartial(object.rexNode.constant) };
    }
    if (
      object.rexNode?.$case === "funcCall" &&
      object.rexNode?.funcCall !== undefined &&
      object.rexNode?.funcCall !== null
    ) {
      message.rexNode = { $case: "funcCall", funcCall: FunctionCall.fromPartial(object.rexNode.funcCall) };
    }
    if (object.rexNode?.$case === "udf" && object.rexNode?.udf !== undefined && object.rexNode?.udf !== null) {
      message.rexNode = { $case: "udf", udf: UserDefinedFunction.fromPartial(object.rexNode.udf) };
    }
    return message;
  },
};

function createBaseTableFunction(): TableFunction {
  return { functionType: TableFunction_Type.UNSPECIFIED, args: [], returnType: undefined };
}

export const TableFunction = {
  fromJSON(object: any): TableFunction {
    return {
      functionType: isSet(object.functionType)
        ? tableFunction_TypeFromJSON(object.functionType)
        : TableFunction_Type.UNSPECIFIED,
      args: Array.isArray(object?.args)
        ? object.args.map((e: any) => ExprNode.fromJSON(e))
        : [],
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
    message.functionType = object.functionType ?? TableFunction_Type.UNSPECIFIED;
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

function createBaseFunctionCall(): FunctionCall {
  return { children: [] };
}

export const FunctionCall = {
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
  return {
    type: AggCall_Type.UNSPECIFIED,
    args: [],
    returnType: undefined,
    distinct: false,
    orderByFields: [],
    filter: undefined,
  };
}

export const AggCall = {
  fromJSON(object: any): AggCall {
    return {
      type: isSet(object.type) ? aggCall_TypeFromJSON(object.type) : AggCall_Type.UNSPECIFIED,
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
    message.type = object.type ?? AggCall_Type.UNSPECIFIED;
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
  return { input: undefined, type: undefined, direction: OrderType.ORDER_UNSPECIFIED, nullsFirst: false };
}

export const AggCall_OrderByField = {
  fromJSON(object: any): AggCall_OrderByField {
    return {
      input: isSet(object.input) ? InputRefExpr.fromJSON(object.input) : undefined,
      type: isSet(object.type) ? DataType.fromJSON(object.type) : undefined,
      direction: isSet(object.direction) ? orderTypeFromJSON(object.direction) : OrderType.ORDER_UNSPECIFIED,
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
    message.direction = object.direction ?? OrderType.ORDER_UNSPECIFIED;
    message.nullsFirst = object.nullsFirst ?? false;
    return message;
  },
};

function createBaseUserDefinedFunction(): UserDefinedFunction {
  return { children: [], name: "", argTypes: [], language: "", link: "", identifier: "" };
}

export const UserDefinedFunction = {
  fromJSON(object: any): UserDefinedFunction {
    return {
      children: Array.isArray(object?.children) ? object.children.map((e: any) => ExprNode.fromJSON(e)) : [],
      name: isSet(object.name) ? String(object.name) : "",
      argTypes: Array.isArray(object?.argTypes) ? object.argTypes.map((e: any) => DataType.fromJSON(e)) : [],
      language: isSet(object.language) ? String(object.language) : "",
      link: isSet(object.link) ? String(object.link) : "",
      identifier: isSet(object.identifier) ? String(object.identifier) : "",
    };
  },

  toJSON(message: UserDefinedFunction): unknown {
    const obj: any = {};
    if (message.children) {
      obj.children = message.children.map((e) => e ? ExprNode.toJSON(e) : undefined);
    } else {
      obj.children = [];
    }
    message.name !== undefined && (obj.name = message.name);
    if (message.argTypes) {
      obj.argTypes = message.argTypes.map((e) => e ? DataType.toJSON(e) : undefined);
    } else {
      obj.argTypes = [];
    }
    message.language !== undefined && (obj.language = message.language);
    message.link !== undefined && (obj.link = message.link);
    message.identifier !== undefined && (obj.identifier = message.identifier);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<UserDefinedFunction>, I>>(object: I): UserDefinedFunction {
    const message = createBaseUserDefinedFunction();
    message.children = object.children?.map((e) => ExprNode.fromPartial(e)) || [];
    message.name = object.name ?? "";
    message.argTypes = object.argTypes?.map((e) => DataType.fromPartial(e)) || [];
    message.language = object.language ?? "";
    message.link = object.link ?? "";
    message.identifier = object.identifier ?? "";
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
