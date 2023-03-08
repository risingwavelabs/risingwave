/* eslint-disable */

export const protobufPackage = "order";

export const PbDirection = {
  PbDirection_UNSPECIFIED: "PbDirection_UNSPECIFIED",
  PbDirection_ASCENDING: "PbDirection_ASCENDING",
  PbDirection_DESCENDING: "PbDirection_DESCENDING",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type PbDirection = typeof PbDirection[keyof typeof PbDirection];

export function pbDirectionFromJSON(object: any): PbDirection {
  switch (object) {
    case 0:
    case "PbDirection_UNSPECIFIED":
      return PbDirection.PbDirection_UNSPECIFIED;
    case 1:
    case "PbDirection_ASCENDING":
      return PbDirection.PbDirection_ASCENDING;
    case 2:
    case "PbDirection_DESCENDING":
      return PbDirection.PbDirection_DESCENDING;
    case -1:
    case "UNRECOGNIZED":
    default:
      return PbDirection.UNRECOGNIZED;
  }
}

export function pbDirectionToJSON(object: PbDirection): string {
  switch (object) {
    case PbDirection.PbDirection_UNSPECIFIED:
      return "PbDirection_UNSPECIFIED";
    case PbDirection.PbDirection_ASCENDING:
      return "PbDirection_ASCENDING";
    case PbDirection.PbDirection_DESCENDING:
      return "PbDirection_DESCENDING";
    case PbDirection.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface PbOrderType {
  /**
   * TODO(rc): enable `NULLS FIRST | LAST`
   * PbNullsAre nulls_are = 2;
   */
  direction: PbDirection;
}

/** Column index with an order type (ASC or DESC). Used to represent a sort key (`repeated PbColumnOrder`). */
export interface PbColumnOrder {
  columnIndex: number;
  orderType: PbOrderType | undefined;
}

function createBasePbOrderType(): PbOrderType {
  return { direction: PbDirection.PbDirection_UNSPECIFIED };
}

export const PbOrderType = {
  fromJSON(object: any): PbOrderType {
    return {
      direction: isSet(object.direction) ? pbDirectionFromJSON(object.direction) : PbDirection.PbDirection_UNSPECIFIED,
    };
  },

  toJSON(message: PbOrderType): unknown {
    const obj: any = {};
    message.direction !== undefined && (obj.direction = pbDirectionToJSON(message.direction));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PbOrderType>, I>>(object: I): PbOrderType {
    const message = createBasePbOrderType();
    message.direction = object.direction ?? PbDirection.PbDirection_UNSPECIFIED;
    return message;
  },
};

function createBasePbColumnOrder(): PbColumnOrder {
  return { columnIndex: 0, orderType: undefined };
}

export const PbColumnOrder = {
  fromJSON(object: any): PbColumnOrder {
    return {
      columnIndex: isSet(object.columnIndex) ? Number(object.columnIndex) : 0,
      orderType: isSet(object.orderType) ? PbOrderType.fromJSON(object.orderType) : undefined,
    };
  },

  toJSON(message: PbColumnOrder): unknown {
    const obj: any = {};
    message.columnIndex !== undefined && (obj.columnIndex = Math.round(message.columnIndex));
    message.orderType !== undefined &&
      (obj.orderType = message.orderType ? PbOrderType.toJSON(message.orderType) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<PbColumnOrder>, I>>(object: I): PbColumnOrder {
    const message = createBasePbColumnOrder();
    message.columnIndex = object.columnIndex ?? 0;
    message.orderType = (object.orderType !== undefined && object.orderType !== null)
      ? PbOrderType.fromPartial(object.orderType)
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

function isSet(value: any): boolean {
  return value !== null && value !== undefined;
}
