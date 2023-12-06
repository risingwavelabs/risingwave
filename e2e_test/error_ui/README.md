# User Interface Test for Error Reporting

The test cases in this directory act as snapshot tests for the error reporting, to ensure that the UI does not change unexpectedly.

When you find the tests in this directory failing...

- First, ensure that the changes to the error messages are expected and make them look better.
- Then, update the test cases by running:
  ```bash
  ./risedev slt './e2e_test/error_ui/simple/**/*.slt' --override
  ./risedev slt './e2e_test/error_ui/extended/**/*.slt' --engine postgres-extended --override
  ```
  Please note that the minimum required version of `sqllogictest` is 0.18 or higher.
