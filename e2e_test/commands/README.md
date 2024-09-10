# "built-in" Sqllogictest system commands

Scripts (executables) in this directory are expected to be used as `system` commands in sqllogictests. You can use any language like bash, python, zx.

They will be loaded in `PATH` by `risedev slt`, and thus function as kind of "built-in" commands.

Only general commands should be put here.
If the script is ad-hoc (only used for one test), it's better to put next to the test file.

See the [connector development guide](http://risingwavelabs.github.io/risingwave/connector/intro.html#end-to-end-tests) for more information about how to test.
