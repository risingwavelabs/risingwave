# CI Workflow

## Sqlsmith

Sqlsmith has two `cron` workflows.
1. Frontend tests
2. E2e tests

It is separate from PR workflow because:
1. Fuzzing tests might fail as new features and generators are added.
2. Avoid slowing down PR workflow, fuzzing tests take a while to run.
   We can include failing tests in e2e / unit tests when encountered.
