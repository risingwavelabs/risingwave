/* eslint-disable */
import { HostAddress } from "./common";

export const protobufPackage = "leader";

export interface LeaderRequest {
}

export interface LeaderResponse {
  leaderAddr: HostAddress | undefined;
}

function createBaseLeaderRequest(): LeaderRequest {
  return {};
}

export const LeaderRequest = {
  fromJSON(_: any): LeaderRequest {
    return {};
  },

  toJSON(_: LeaderRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LeaderRequest>, I>>(_: I): LeaderRequest {
    const message = createBaseLeaderRequest();
    return message;
  },
};

function createBaseLeaderResponse(): LeaderResponse {
  return { leaderAddr: undefined };
}

export const LeaderResponse = {
  fromJSON(object: any): LeaderResponse {
    return { leaderAddr: isSet(object.leaderAddr) ? HostAddress.fromJSON(object.leaderAddr) : undefined };
  },

  toJSON(message: LeaderResponse): unknown {
    const obj: any = {};
    message.leaderAddr !== undefined &&
      (obj.leaderAddr = message.leaderAddr ? HostAddress.toJSON(message.leaderAddr) : undefined);
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<LeaderResponse>, I>>(object: I): LeaderResponse {
    const message = createBaseLeaderResponse();
    message.leaderAddr = (object.leaderAddr !== undefined && object.leaderAddr !== null)
      ? HostAddress.fromPartial(object.leaderAddr)
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
