# User Interface Test for Error Reporting

The test cases in this directory act as snapshot tests for the error reporting, to ensure that the UI does not change unexpectedly.

When you find the tests in this directory failing...

- First, ensure that the changes to the error messages are expected and make them look better.
- Then, update the snapshots by running:
  ```bash
  sqllogictest -p 4566 -d dev './e2e_test/error_ui/**/*.slt' --override
  ```
