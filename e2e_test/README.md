# e2e test

This folder contains sqllogictest source files for e2e. It is running on CI for integration.

Scripts in folder `distributed` is able to pass both 3 compute nodes + 1 leader nodes test.

Scripts in folder `streaming` is only for streaming related tests.

e2e test should drop table if create one to avoid table exist conflict (Currently all tests are running in a single database).
## How to write e2e tests
Refer to Sqllogictest [Doc](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)
