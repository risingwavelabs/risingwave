# Risingwave SQL parser

This parser is a fork of <https://github.com/andygrove/sqlparser-rs>.


## Add a new test case
1. Copy an item in the yaml file and edit the `input` to the sql you want to test.
2. Run `./risedev update-parser-test` to regenerate the `formatted_sql` which is the expected output.