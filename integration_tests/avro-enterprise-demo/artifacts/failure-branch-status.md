# Failure Branch Status

## STEP_IDS
- F1-source-recovery
- F2-downstream-delivery-retry

- [x] Source restart recovery resumes ingestion and preserves continuity
- [x] Downstream delivery failure is surfaced and the same key is later delivered successfully after retry

## Scope boundaries

- Scope boundary: remove optional field is not part of this demo scope.
- Scope boundary: AD group-to-role mapping is not part of this demo scope.
- Scope boundary: real higher-environment promotion is not part of this demo scope.
- Scope boundary: CSV/XML/fixed-width outputs are not part of this demo scope; fixed-width remains deferred with a remark only.
