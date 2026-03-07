# AGO Kickoff: Failure Matrix (Appendix)

## Input Path
- duplicate events
- reordered events
- late events
- burst traffic

## RW Path
- watermark stall
- recovery during active intent generation
- projection lag

## Control Plane Path
- crash after claim
- timeout during execution
- side-effect committed but result writeback failed

## Contract Evolution
- payload/schema drift
- policy version mismatch
- DLQ accumulation

## Required Outcome
For each scenario: expected behavior, detection signal, owner, and rollback action must be documented.
