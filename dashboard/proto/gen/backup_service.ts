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

export interface MetaBackupManifestId {
  id: number;
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

export interface GetMetaSnapshotManifestRequest {
}

export interface GetMetaSnapshotManifestResponse {
  manifest: MetaSnapshotManifest | undefined;
}

export interface MetaSnapshotManifest {
  manifestId: number;
  snapshotMetadata: MetaSnapshotMetadata[];
}

export interface MetaSnapshotMetadata {
  id: number;
  hummockVersionId: number;
  maxCommittedEpoch: number;
  safeEpoch: number;
}

function createBaseMetaBackupManifestId(): MetaBackupManifestId {
  return { id: 0 };
}

export const MetaBackupManifestId = {
  fromJSON(object: any): MetaBackupManifestId {
    return { id: isSet(object.id) ? Number(object.id) : 0 };
  },

  toJSON(message: MetaBackupManifestId): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    return obj;
  },

  create<I extends Exact<DeepPartial<MetaBackupManifestId>, I>>(base?: I): MetaBackupManifestId {
    return MetaBackupManifestId.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<MetaBackupManifestId>, I>>(object: I): MetaBackupManifestId {
    const message = createBaseMetaBackupManifestId();
    message.id = object.id ?? 0;
    return message;
  },
};

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

  create<I extends Exact<DeepPartial<BackupMetaRequest>, I>>(base?: I): BackupMetaRequest {
    return BackupMetaRequest.fromPartial(base ?? {});
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

  create<I extends Exact<DeepPartial<BackupMetaResponse>, I>>(base?: I): BackupMetaResponse {
    return BackupMetaResponse.fromPartial(base ?? {});
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

  create<I extends Exact<DeepPartial<GetBackupJobStatusRequest>, I>>(base?: I): GetBackupJobStatusRequest {
    return GetBackupJobStatusRequest.fromPartial(base ?? {});
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

  create<I extends Exact<DeepPartial<GetBackupJobStatusResponse>, I>>(base?: I): GetBackupJobStatusResponse {
    return GetBackupJobStatusResponse.fromPartial(base ?? {});
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

  create<I extends Exact<DeepPartial<DeleteMetaSnapshotRequest>, I>>(base?: I): DeleteMetaSnapshotRequest {
    return DeleteMetaSnapshotRequest.fromPartial(base ?? {});
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

  create<I extends Exact<DeepPartial<DeleteMetaSnapshotResponse>, I>>(base?: I): DeleteMetaSnapshotResponse {
    return DeleteMetaSnapshotResponse.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<DeleteMetaSnapshotResponse>, I>>(_: I): DeleteMetaSnapshotResponse {
    const message = createBaseDeleteMetaSnapshotResponse();
    return message;
  },
};

function createBaseGetMetaSnapshotManifestRequest(): GetMetaSnapshotManifestRequest {
  return {};
}

export const GetMetaSnapshotManifestRequest = {
  fromJSON(_: any): GetMetaSnapshotManifestRequest {
    return {};
  },

  toJSON(_: GetMetaSnapshotManifestRequest): unknown {
    const obj: any = {};
    return obj;
  },

  create<I extends Exact<DeepPartial<GetMetaSnapshotManifestRequest>, I>>(base?: I): GetMetaSnapshotManifestRequest {
    return GetMetaSnapshotManifestRequest.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<GetMetaSnapshotManifestRequest>, I>>(_: I): GetMetaSnapshotManifestRequest {
    const message = createBaseGetMetaSnapshotManifestRequest();
    return message;
  },
};

function createBaseGetMetaSnapshotManifestResponse(): GetMetaSnapshotManifestResponse {
  return { manifest: undefined };
}

export const GetMetaSnapshotManifestResponse = {
  fromJSON(object: any): GetMetaSnapshotManifestResponse {
    return { manifest: isSet(object.manifest) ? MetaSnapshotManifest.fromJSON(object.manifest) : undefined };
  },

  toJSON(message: GetMetaSnapshotManifestResponse): unknown {
    const obj: any = {};
    message.manifest !== undefined &&
      (obj.manifest = message.manifest ? MetaSnapshotManifest.toJSON(message.manifest) : undefined);
    return obj;
  },

  create<I extends Exact<DeepPartial<GetMetaSnapshotManifestResponse>, I>>(base?: I): GetMetaSnapshotManifestResponse {
    return GetMetaSnapshotManifestResponse.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<GetMetaSnapshotManifestResponse>, I>>(
    object: I,
  ): GetMetaSnapshotManifestResponse {
    const message = createBaseGetMetaSnapshotManifestResponse();
    message.manifest = (object.manifest !== undefined && object.manifest !== null)
      ? MetaSnapshotManifest.fromPartial(object.manifest)
      : undefined;
    return message;
  },
};

function createBaseMetaSnapshotManifest(): MetaSnapshotManifest {
  return { manifestId: 0, snapshotMetadata: [] };
}

export const MetaSnapshotManifest = {
  fromJSON(object: any): MetaSnapshotManifest {
    return {
      manifestId: isSet(object.manifestId) ? Number(object.manifestId) : 0,
      snapshotMetadata: Array.isArray(object?.snapshotMetadata)
        ? object.snapshotMetadata.map((e: any) => MetaSnapshotMetadata.fromJSON(e))
        : [],
    };
  },

  toJSON(message: MetaSnapshotManifest): unknown {
    const obj: any = {};
    message.manifestId !== undefined && (obj.manifestId = Math.round(message.manifestId));
    if (message.snapshotMetadata) {
      obj.snapshotMetadata = message.snapshotMetadata.map((e) => e ? MetaSnapshotMetadata.toJSON(e) : undefined);
    } else {
      obj.snapshotMetadata = [];
    }
    return obj;
  },

  create<I extends Exact<DeepPartial<MetaSnapshotManifest>, I>>(base?: I): MetaSnapshotManifest {
    return MetaSnapshotManifest.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<MetaSnapshotManifest>, I>>(object: I): MetaSnapshotManifest {
    const message = createBaseMetaSnapshotManifest();
    message.manifestId = object.manifestId ?? 0;
    message.snapshotMetadata = object.snapshotMetadata?.map((e) => MetaSnapshotMetadata.fromPartial(e)) || [];
    return message;
  },
};

function createBaseMetaSnapshotMetadata(): MetaSnapshotMetadata {
  return { id: 0, hummockVersionId: 0, maxCommittedEpoch: 0, safeEpoch: 0 };
}

export const MetaSnapshotMetadata = {
  fromJSON(object: any): MetaSnapshotMetadata {
    return {
      id: isSet(object.id) ? Number(object.id) : 0,
      hummockVersionId: isSet(object.hummockVersionId) ? Number(object.hummockVersionId) : 0,
      maxCommittedEpoch: isSet(object.maxCommittedEpoch) ? Number(object.maxCommittedEpoch) : 0,
      safeEpoch: isSet(object.safeEpoch) ? Number(object.safeEpoch) : 0,
    };
  },

  toJSON(message: MetaSnapshotMetadata): unknown {
    const obj: any = {};
    message.id !== undefined && (obj.id = Math.round(message.id));
    message.hummockVersionId !== undefined && (obj.hummockVersionId = Math.round(message.hummockVersionId));
    message.maxCommittedEpoch !== undefined && (obj.maxCommittedEpoch = Math.round(message.maxCommittedEpoch));
    message.safeEpoch !== undefined && (obj.safeEpoch = Math.round(message.safeEpoch));
    return obj;
  },

  create<I extends Exact<DeepPartial<MetaSnapshotMetadata>, I>>(base?: I): MetaSnapshotMetadata {
    return MetaSnapshotMetadata.fromPartial(base ?? {});
  },

  fromPartial<I extends Exact<DeepPartial<MetaSnapshotMetadata>, I>>(object: I): MetaSnapshotMetadata {
    const message = createBaseMetaSnapshotMetadata();
    message.id = object.id ?? 0;
    message.hummockVersionId = object.hummockVersionId ?? 0;
    message.maxCommittedEpoch = object.maxCommittedEpoch ?? 0;
    message.safeEpoch = object.safeEpoch ?? 0;
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
