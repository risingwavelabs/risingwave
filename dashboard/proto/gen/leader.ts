/* eslint-disable */
import { HostAddress } from "./common";

export const protobufPackage = "leader";

export interface LeaderRequest {
}

export interface LeaderResponse {
  leaderAddr: HostAddress | undefined;
}

export interface MembersRequest {
}

export interface Member {
  memberAddr: HostAddress | undefined;
  leaseId: number;
}

export interface MembersResponse {
  members: Member[];
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

function createBaseMembersRequest(): MembersRequest {
  return {};
}

export const MembersRequest = {
  fromJSON(_: any): MembersRequest {
    return {};
  },

  toJSON(_: MembersRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MembersRequest>, I>>(_: I): MembersRequest {
    const message = createBaseMembersRequest();
    return message;
  },
};

function createBaseMember(): Member {
  return { memberAddr: undefined, leaseId: 0 };
}

export const Member = {
  fromJSON(object: any): Member {
    return {
      memberAddr: isSet(object.memberAddr) ? HostAddress.fromJSON(object.memberAddr) : undefined,
      leaseId: isSet(object.leaseId) ? Number(object.leaseId) : 0,
    };
  },

  toJSON(message: Member): unknown {
    const obj: any = {};
    message.memberAddr !== undefined &&
      (obj.memberAddr = message.memberAddr ? HostAddress.toJSON(message.memberAddr) : undefined);
    message.leaseId !== undefined && (obj.leaseId = Math.round(message.leaseId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<Member>, I>>(object: I): Member {
    const message = createBaseMember();
    message.memberAddr = (object.memberAddr !== undefined && object.memberAddr !== null)
      ? HostAddress.fromPartial(object.memberAddr)
      : undefined;
    message.leaseId = object.leaseId ?? 0;
    return message;
  },
};

function createBaseMembersResponse(): MembersResponse {
  return { members: [] };
}

export const MembersResponse = {
  fromJSON(object: any): MembersResponse {
    return { members: Array.isArray(object?.members) ? object.members.map((e: any) => Member.fromJSON(e)) : [] };
  },

  toJSON(message: MembersResponse): unknown {
    const obj: any = {};
    if (message.members) {
      obj.members = message.members.map((e) => e ? Member.toJSON(e) : undefined);
    } else {
      obj.members = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<MembersResponse>, I>>(object: I): MembersResponse {
    const message = createBaseMembersResponse();
    message.members = object.members?.map((e) => Member.fromPartial(e)) || [];
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
