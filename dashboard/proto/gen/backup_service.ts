/* eslint-disable */

export const protobufPackage = "backup_service";

export const BackupJobStatus = {
  UNSPECIFIED: "UNSPECIFIED",
  RUNNING: "RUNNING",
  SUCCEEDED: "SUCCEEDED",
  /**
   * NOT_FOUND - NOT_FOUND indicates one of these cases:
   * - Invalid job id.
   * - Job has failed.
   * - Job has succeeded, but its resulted backup has been deleted later.
   */
  NOT_FOUND: "NOT_FOUND",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type BackupJobStatus = typeof BackupJobStatus[keyof typeof BackupJobStatus];

export function backupJobStatusFromJSON(object: any): BackupJobStatus {
  switch (object) {
    case 0:
    case "UNSPECIFIED":
      return BackupJobStatus.UNSPECIFIED;
    case 1:
    case "RUNNING":
      return BackupJobStatus.RUNNING;
    case 2:
    case "SUCCEEDED":
      return BackupJobStatus.SUCCEEDED;
    case 3:
    case "NOT_FOUND":
      return BackupJobStatus.NOT_FOUND;
    case -1:
    case "UNRECOGNIZED":
    default:
      return BackupJobStatus.UNRECOGNIZED;
  }
}

export function backupJobStatusToJSON(object: BackupJobStatus): string {
  switch (object) {
    case BackupJobStatus.UNSPECIFIED:
      return "UNSPECIFIED";
    case BackupJobStatus.RUNNING:
      return "RUNNING";
    case BackupJobStatus.SUCCEEDED:
      return "SUCCEEDED";
    case BackupJobStatus.NOT_FOUND:
      return "NOT_FOUND";
    case BackupJobStatus.UNRECOGNIZED:
    default:
      return "UNRECOGNIZED";
  }
}

export interface BackupMetaRequest {
}

export interface BackupMetaResponse {
  jobId: number;
}

export interface GetBackupJobStatusRequest {
  jobId: number;
}

export interface GetBackupJobStatusResponse {
  jobId: number;
  jobStatus: BackupJobStatus;
}

export interface DeleteMetaSnapshotRequest {
  snapshotIds: number[];
}

export interface DeleteMetaSnapshotResponse {
}

function createBaseBackupMetaRequest(): BackupMetaRequest {
  return {};
}

export const BackupMetaRequest = {
  fromJSON(_: any): BackupMetaRequest {
    return {};
  },

  toJSON(_: BackupMetaRequest): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BackupMetaRequest>, I>>(_: I): BackupMetaRequest {
    const message = createBaseBackupMetaRequest();
    return message;
  },
};

function createBaseBackupMetaResponse(): BackupMetaResponse {
  return { jobId: 0 };
}

export const BackupMetaResponse = {
  fromJSON(object: any): BackupMetaResponse {
    return { jobId: isSet(object.jobId) ? Number(object.jobId) : 0 };
  },

  toJSON(message: BackupMetaResponse): unknown {
    const obj: any = {};
    message.jobId !== undefined && (obj.jobId = Math.round(message.jobId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<BackupMetaResponse>, I>>(object: I): BackupMetaResponse {
    const message = createBaseBackupMetaResponse();
    message.jobId = object.jobId ?? 0;
    return message;
  },
};

function createBaseGetBackupJobStatusRequest(): GetBackupJobStatusRequest {
  return { jobId: 0 };
}

export const GetBackupJobStatusRequest = {
  fromJSON(object: any): GetBackupJobStatusRequest {
    return { jobId: isSet(object.jobId) ? Number(object.jobId) : 0 };
  },

  toJSON(message: GetBackupJobStatusRequest): unknown {
    const obj: any = {};
    message.jobId !== undefined && (obj.jobId = Math.round(message.jobId));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetBackupJobStatusRequest>, I>>(object: I): GetBackupJobStatusRequest {
    const message = createBaseGetBackupJobStatusRequest();
    message.jobId = object.jobId ?? 0;
    return message;
  },
};

function createBaseGetBackupJobStatusResponse(): GetBackupJobStatusResponse {
  return { jobId: 0, jobStatus: BackupJobStatus.UNSPECIFIED };
}

export const GetBackupJobStatusResponse = {
  fromJSON(object: any): GetBackupJobStatusResponse {
    return {
      jobId: isSet(object.jobId) ? Number(object.jobId) : 0,
      jobStatus: isSet(object.jobStatus) ? backupJobStatusFromJSON(object.jobStatus) : BackupJobStatus.UNSPECIFIED,
    };
  },

  toJSON(message: GetBackupJobStatusResponse): unknown {
    const obj: any = {};
    message.jobId !== undefined && (obj.jobId = Math.round(message.jobId));
    message.jobStatus !== undefined && (obj.jobStatus = backupJobStatusToJSON(message.jobStatus));
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<GetBackupJobStatusResponse>, I>>(object: I): GetBackupJobStatusResponse {
    const message = createBaseGetBackupJobStatusResponse();
    message.jobId = object.jobId ?? 0;
    message.jobStatus = object.jobStatus ?? BackupJobStatus.UNSPECIFIED;
    return message;
  },
};

function createBaseDeleteMetaSnapshotRequest(): DeleteMetaSnapshotRequest {
  return { snapshotIds: [] };
}

export const DeleteMetaSnapshotRequest = {
  fromJSON(object: any): DeleteMetaSnapshotRequest {
    return { snapshotIds: Array.isArray(object?.snapshotIds) ? object.snapshotIds.map((e: any) => Number(e)) : [] };
  },

  toJSON(message: DeleteMetaSnapshotRequest): unknown {
    const obj: any = {};
    if (message.snapshotIds) {
      obj.snapshotIds = message.snapshotIds.map((e) => Math.round(e));
    } else {
      obj.snapshotIds = [];
    }
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeleteMetaSnapshotRequest>, I>>(object: I): DeleteMetaSnapshotRequest {
    const message = createBaseDeleteMetaSnapshotRequest();
    message.snapshotIds = object.snapshotIds?.map((e) => e) || [];
    return message;
  },
};

function createBaseDeleteMetaSnapshotResponse(): DeleteMetaSnapshotResponse {
  return {};
}

export const DeleteMetaSnapshotResponse = {
  fromJSON(_: any): DeleteMetaSnapshotResponse {
    return {};
  },

  toJSON(_: DeleteMetaSnapshotResponse): unknown {
    const obj: any = {};
    return obj;
  },

  fromPartial<I extends Exact<DeepPartial<DeleteMetaSnapshotResponse>, I>>(_: I): DeleteMetaSnapshotResponse {
    const message = createBaseDeleteMetaSnapshotResponse();
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
